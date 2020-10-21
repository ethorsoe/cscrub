#define _XOPEN_SOURCE 9000
#define _GNU_SOURCE
#include <stdint.h>
#include <unistd.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <btrfs/ioctl.h>
#include <btrfs/rbtree.h>
#include <btrfs/btrfs-list.h>
#include <btrfs/ctree.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <threads.h>
#include <sys/mman.h>
#include <libaio.h>
#include <omp.h>
#include <malloc.h>
#include <stdarg.h>
#include "libbtrfs.h"

#include "crc32c.h"
static inline u32 cscrub_crc(u8 *data, int len) {
	return ~multitable_crc32c(~0U, data, len);
}

#define WORK_ITEM_COUNT 2
#define MAX_INDEPENDENT_PARITY_STRIPES 2
#define CSCRUB_AIO_INTERLEAVE_STAGES 2
#define MAX_DEVID 1024
#define CHECKSUMSIZE 4
#define MAX_CHECKSUMS_PER_STRIPE (1ULL << 18)
#define PARALLEL_BLOCK_SIZE (4 * 1024 * 1024)
#define CHECKSUM_SPACE_IN_META_NODE 0x20

struct scrub_io_ctx {
	io_context_t ctxp;
	u8 **disk_buffers;
};

struct shared_data;
struct work_item_data {
	struct scrub_io_ctx *io_contexts;
	u8 *parity;
	unsigned char *bitmap;
	u32 *checksums;
	u64 *disk_offsets;
	int *diskfds;
	struct shared_data *parent;
	u64 logical_offset;
	u64 length;
	u64 stripe_len;
	u32 num_stripes;
	u32 type;
};

struct shared_data {
	struct btrfs_ioctl_fs_info_args fsinfo;
	mtx_t mutex;
	cnd_t condition;
	int *diskfds;
	unsigned consumer_offset;
	unsigned producer_offset;
	int mountfd;
	struct work_item_data work[WORK_ITEM_COUNT];
};


static inline int get_bit(const u8 *data, u32 index) {
	return (data[index >> 3U] >> (index & 7U)) & 1U;
}
static inline void set_bit(u8 *data, u32 index) {
	data[index >> 3U] |= 1U << (index & 7U);
}
static inline void set_bits(u8 *data, u32 index, u32 len) {
	while(len && (index & 7U)) {
		set_bit(data, index++);
		len--;
	}
	u32 bulklen = len & ~7UL;
	memset(data + (index >> 3U), -1, bulklen >> 3U);
	index += bulklen;
	len -= bulklen;
	while(len--) {
		set_bit(data, index++);
	}
}

void vdie(const char *fmt, va_list args) {
	const char *errmsg = strerror(errno);
	vfprintf(stderr, fmt, args);
	fprintf(stderr, "Error: %s\n", errmsg);
	exit(EXIT_FAILURE);
}
void die(const char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	vdie(fmt, args);
	va_end(args);
}
void die_on(bool condition, const char *fmt, ...) {
	va_list args;
	if (!condition) {
		return;
	}
	va_start(args, fmt);
	vdie(fmt, args);
	va_end(args);
}

static inline unsigned get_parity_stripes(u32 type) {
	if (!(type & BTRFS_BLOCK_GROUP_PROFILE_MASK &
			~BTRFS_BLOCK_GROUP_RAID0)) {
		return 0;
	} else if (type & (BTRFS_BLOCK_GROUP_RAID5|BTRFS_BLOCK_GROUP_RAID1|BTRFS_BLOCK_GROUP_DUP)) {
		return 1;
	} else if (type & (BTRFS_BLOCK_GROUP_RAID6|BTRFS_BLOCK_GROUP_RAID1C3)) {
		return 2;
	} else if (type & BTRFS_BLOCK_GROUP_RAID1C4) {
		return 3;
	}
	die("Internal error, invalid block group type %x accepted\n", type);
	return -1;
}

void check_parallel_block(struct work_item_data *work, unsigned iteration, unsigned phys_ind) {
	struct scrub_io_ctx const *ctx = (work->io_contexts + iteration % CSCRUB_AIO_INTERLEAVE_STAGES);
	const unsigned log_per_phys = (work->num_stripes - get_parity_stripes(work->type));
	const unsigned meta_space = (BTRFS_BLOCK_GROUP_DATA & work->type) ?
		0 : CHECKSUM_SPACE_IN_META_NODE;
	const unsigned sector_size = (meta_space) ?
		work->parent->fsinfo.nodesize : work->parent->fsinfo.sectorsize;
	const unsigned stride = work->stripe_len / sector_size;
	const u64 phys_off_in_pblock = phys_ind * sector_size;
	const u64 stripe_ind_in_bg = iteration * (PARALLEL_BLOCK_SIZE / work->stripe_len) + phys_ind / stride;
	const unsigned stripe_rotation = stripe_ind_in_bg % work->num_stripes;
	u8 *rotated_buffers[work->num_stripes];
	u8 *parity[MAX_INDEPENDENT_PARITY_STRIPES];
	parity[0] = work->parity + omp_get_thread_num() *
		sector_size * MAX_INDEPENDENT_PARITY_STRIPES;
	parity[1] = parity[0] + sector_size;
	memset(parity[0], 0, sector_size +
		((BTRFS_BLOCK_GROUP_RAID6 & work->type) ? sector_size : 0));
	for (unsigned rot_ind = 0; rot_ind < work->num_stripes; rot_ind++)
		rotated_buffers[rot_ind] = ctx->disk_buffers[(rot_ind + stripe_rotation) % work->num_stripes];
	bool parity_valid = false;
	for (unsigned check_disk = 0; check_disk < log_per_phys; check_disk++) {
		const u64 check_table_ind = (stripe_ind_in_bg * log_per_phys + check_disk) * stride + (phys_ind % stride);
		const unsigned long long log_off_in_fs = check_table_ind * sector_size + work->logical_offset;

		if (log_per_phys != work->num_stripes && !(BTRFS_BLOCK_GROUP_RAID6 & work->type)) {
			for (unsigned i = 0; i < sector_size; i++) {
				parity[0][i] ^= rotated_buffers[check_disk][i + phys_off_in_pblock];
			}
		}
		if (get_bit(work->bitmap, check_table_ind)) {
			parity_valid = true;
			u8 *block_start = rotated_buffers[check_disk] + phys_off_in_pblock;
			u32 wanted_checksum = meta_space ?
				(*(u32*)block_start) : work->checksums[check_table_ind];
			u32 checksum = cscrub_crc(block_start + meta_space, sector_size - meta_space);
			if (checksum != wanted_checksum) {
				fprintf(stderr, "logical %llu wanted 0x%.8lx got 0x%.8lx\n",
					log_off_in_fs,
					(unsigned long)wanted_checksum, (unsigned long)checksum);
			}
		}
	}
	if (parity_valid && (BTRFS_BLOCK_GROUP_RAID6 & work->type)) {
		void *raid6disks[work->num_stripes];
		for (unsigned i = 0; i < log_per_phys; i++) {
			raid6disks[i] = rotated_buffers[i] + phys_off_in_pblock;
		}
		raid6disks[log_per_phys] = parity[0];
		raid6disks[log_per_phys + 1] = parity[1];
		raid6_gen_syndrome(work->num_stripes, sector_size, raid6disks);
	}
	for (unsigned stripe = log_per_phys; parity_valid && stripe < work->num_stripes; stripe++) {
		const unsigned stripe_select =
			((BTRFS_BLOCK_GROUP_RAID6 & work->type) && log_per_phys != stripe) ? 1 : 0;
		if (memcmp(parity[0 + stripe_select], rotated_buffers[stripe] + phys_off_in_pblock,
				sector_size)) {
			fprintf(stderr, "chunk at logical %llu parity at offset %llu wrong\n",
				(unsigned long long)(work->logical_offset),
				(unsigned long long)(PARALLEL_BLOCK_SIZE * iteration + phys_off_in_pblock));
		}
	}
}
void start_read_parallel_block(struct work_item_data *work, unsigned iteration) {
	const unsigned log_per_phys = (work->num_stripes - get_parity_stripes(work->type));
	const u64 chunk_phys_len = work->length / log_per_phys;
	const u64 pblock_phys_off = PARALLEL_BLOCK_SIZE * iteration;
	struct scrub_io_ctx const *ctx = (work->io_contexts + iteration % CSCRUB_AIO_INTERLEAVE_STAGES);
	struct iocb iocbs[work->num_stripes];
	struct iocb *iocbsp;

	u64 pblock_phys_len = PARALLEL_BLOCK_SIZE;
	if (pblock_phys_off + pblock_phys_len > chunk_phys_len) {
		pblock_phys_len -= pblock_phys_off + pblock_phys_len - chunk_phys_len;
	}
	memset(iocbs, 0, sizeof(iocbs));
	for (unsigned stripe = 0; stripe < work->num_stripes; stripe++) {
		iocbsp = iocbs + stripe;
		io_prep_pread(iocbsp, work->diskfds[stripe], ctx->disk_buffers[stripe], pblock_phys_len, work->disk_offsets[stripe] + pblock_phys_off);
		s64 err = io_submit(ctx->ctxp, 1, &iocbsp);
		die_on(1 != err, "aio submission on stripe %d failed\n", stripe);
	}
}
void end_read_parallel_block(io_context_t ctxp, unsigned num_stripes, unsigned pblock_phys_len) {
	struct io_event io_events[num_stripes];

	memset(io_events, 0, sizeof(io_events));
	s64 err = io_getevents(ctxp, num_stripes, num_stripes, io_events, NULL);
	die_on(err != num_stripes, "Only %d out of %d io events completed\n", err, num_stripes);
	for (unsigned stripe = 0; stripe < num_stripes; stripe++) {
		die_on(pblock_phys_len != io_events[stripe].res,
			"Short read %lld of %lld at stripe %d\n",
			(long long)io_events[stripe].res, (long long)pblock_phys_len, stripe);
	}
}
int consumer(void *private) {
	struct shared_data *shared_data = private;
	mtx_lock(&shared_data->mutex);
	while(1) {
		if (shared_data->producer_offset > shared_data->consumer_offset) {
		} else {
			cnd_wait(&shared_data->condition, &shared_data->mutex);
		}
		struct work_item_data *work = shared_data->work + (shared_data->consumer_offset % WORK_ITEM_COUNT);
		printf("consumer processing %llu\n", (unsigned long long)work->logical_offset);
		mtx_unlock(&shared_data->mutex);
		if (!work->num_stripes)
			break;

		const unsigned sector_size = (BTRFS_BLOCK_GROUP_DATA & work->type) ?
			shared_data->fsinfo.sectorsize : shared_data->fsinfo.nodesize;
		const unsigned log_per_phys = (work->num_stripes - get_parity_stripes(work->type));
		const u64 chunk_phys_len = work->length / log_per_phys;
		const unsigned iterations = (chunk_phys_len + PARALLEL_BLOCK_SIZE - 1) / PARALLEL_BLOCK_SIZE;
		
		start_read_parallel_block(work, 0);
		for (unsigned iteration = 0; iteration < iterations; iteration++) {
			const u64 pblock_phys_off = PARALLEL_BLOCK_SIZE * iteration;
			u64 pblock_phys_len = PARALLEL_BLOCK_SIZE;
			if (pblock_phys_off + pblock_phys_len > chunk_phys_len) {
				pblock_phys_len -= pblock_phys_off + pblock_phys_len - chunk_phys_len;
			}
			const unsigned phys_iterations = pblock_phys_len / sector_size;
			end_read_parallel_block((work->io_contexts + iteration % CSCRUB_AIO_INTERLEAVE_STAGES)->ctxp, work->num_stripes, pblock_phys_len);
			if (iteration + 1 < iterations) {
				start_read_parallel_block(work, iteration + 1);
			}
#pragma omp parallel for schedule(static,1)
			for (unsigned phys_ind = 0; phys_ind < phys_iterations; phys_ind++) {
				check_parallel_block(work, iteration, phys_ind);
			}
		}

		mtx_lock(&shared_data->mutex);
		printf("consumer processed %llu\n", (unsigned long long)work->logical_offset);
		shared_data->consumer_offset++;
		cnd_broadcast(&shared_data->condition);
	}
	return 0;
}

static int search_checksum_cb(void *data, struct btrfs_ioctl_search_header *sh, void *private) {
	assert(sh->objectid == BTRFS_EXTENT_CSUM_OBJECTID);
	assert(sh->type == BTRFS_EXTENT_CSUM_KEY);
	assert(sh->len%CHECKSUMSIZE == 0);
	int nsums = sh->len/CHECKSUMSIZE;
	struct work_item_data *work = private;
	u32 *copy_data = data;
	
	int start_index = (sh->offset - work->logical_offset) / work->parent->fsinfo.sectorsize;
	if (0 > start_index) {
		nsums += start_index;
		copy_data -= start_index;
		start_index = 0;
	}
	if (0 >= nsums)
		return 0;
	int spillover_items = start_index + nsums - work->length / work->parent->fsinfo.sectorsize;
	if (0 < spillover_items) {
		nsums -= spillover_items;		
	}
	assert(0 < nsums);
	memcpy(work->checksums + start_index, copy_data, nsums * CHECKSUMSIZE);
	set_bits(work->bitmap, start_index, nsums);
	return 0;
}
static int search_meta_extent_cb(void *data, struct btrfs_ioctl_search_header *sh, void *private) {
	(void)data;
	struct work_item_data *work = private;
	if (BTRFS_METADATA_ITEM_KEY != sh->type)
		return 0;
	
	assert(sh->objectid < work->logical_offset + work->length);
	assert(sh->objectid >= work->logical_offset);
	set_bit(work->bitmap,
		(sh->objectid - work->logical_offset) / work->parent->fsinfo.nodesize);
	return 0;
}
int chunk_callback(void* data, struct btrfs_ioctl_search_header* hdr, void *private){
	struct btrfs_chunk *chunk = data;
	struct shared_data *shared_data = private;

	if (BTRFS_CHUNK_ITEM_KEY != hdr->type)
		return 0;
	if ((chunk->type & BTRFS_BLOCK_GROUP_PROFILE_MASK) &
			~(BTRFS_BLOCK_GROUP_RAID5 | BTRFS_BLOCK_GROUP_RAID1 |
			BTRFS_BLOCK_GROUP_DUP | BTRFS_BLOCK_GROUP_RAID6 |
			BTRFS_BLOCK_GROUP_RAID1C3 | BTRFS_BLOCK_GROUP_RAID1C4)) {
		return 0;
	}

	const bool is_data = chunk->type & BTRFS_BLOCK_GROUP_DATA;
	struct btrfs_stripe *stripes = &chunk->stripe;
	mtx_lock(&shared_data->mutex);
	if (shared_data->producer_offset > shared_data->consumer_offset + 1) {
		cnd_wait(&shared_data->condition, &shared_data->mutex);
	}
	struct work_item_data *work = shared_data->work + (shared_data->producer_offset % WORK_ITEM_COUNT);
	work->logical_offset = hdr->offset;
	work->length = chunk->length;
	work->stripe_len = chunk->stripe_len;
	work->num_stripes = chunk->num_stripes;
	work->type = chunk->type;
	die_on(shared_data->fsinfo.num_devices < work->num_stripes,
		"%d exceeds num devices %d\n", work->num_stripes,
		shared_data->fsinfo.num_devices);
	for (unsigned i = 0; i < work->num_stripes; i++) {
		unsigned devid = stripes[i].devid;
		die_on(MAX_DEVID <= devid || shared_data->diskfds[devid] == -1,
			"Unknown devid %u\n", devid);
		work->diskfds[i] = shared_data->diskfds[devid];
		work->disk_offsets[i] = stripes[i].offset;
	}
	printf("chunk %lld type %lld\n", (long long)hdr->offset, (long long)chunk->type);
	mtx_unlock(&shared_data->mutex);

	unsigned max_checksums = MAX_CHECKSUMS_PER_STRIPE * (work->num_stripes - get_parity_stripes(work->type));
	memset(work->bitmap, 0, max_checksums / CHAR_BIT);
	if (is_data) {
		struct btrfs_ioctl_search_key key;
		memset(&key, 0 ,sizeof(key));
		key.min_objectid = BTRFS_EXTENT_CSUM_OBJECTID;
		key.max_objectid = BTRFS_EXTENT_CSUM_OBJECTID;
		key.min_type = BTRFS_EXTENT_CSUM_KEY;
		key.max_type = BTRFS_EXTENT_CSUM_KEY;
		key.min_offset = work->logical_offset - shared_data->fsinfo.nodesize;
		key.max_offset = work->logical_offset + work->length - 1;
		key.max_transid = -1ULL;
		btrfs_iterate_tree(shared_data->mountfd, BTRFS_CSUM_TREE_OBJECTID, work, search_checksum_cb, &key);
	} else {
		struct btrfs_ioctl_search_key key;
		memset(&key, 0 ,sizeof(key));
		key.min_objectid = work->logical_offset;
		key.max_objectid = work->logical_offset + work->length - 1;
		key.min_type = BTRFS_METADATA_ITEM_KEY;
		key.max_type = BTRFS_METADATA_ITEM_KEY;
		key.min_offset = 0;
		key.max_offset = -1ULL;
		key.max_transid = -1ULL;
		btrfs_iterate_tree(shared_data->mountfd, BTRFS_EXTENT_TREE_OBJECTID,
			work, search_meta_extent_cb, &key);
	}

	mtx_lock(&shared_data->mutex);
	printf("submitted chunk %lld type %lld\n", (long long)hdr->offset, (long long)chunk->type);
	shared_data->producer_offset++;
	cnd_broadcast(&shared_data->condition);
	mtx_unlock(&shared_data->mutex);
	return 0;
}

int main (int argc, char **argv) {
	const unsigned omp_threads = omp_get_max_threads();
	int devices[MAX_DEVID];
	struct shared_data shared_data;

	die_on(argc != 2, "usage: %s <mount point>\n", argv[0]);
	shared_data.mountfd = open(argv[1], O_RDONLY);
	die_on(0 > shared_data.mountfd, "Mount point open failed\n");
	int err = ioctl(shared_data.mountfd, BTRFS_IOC_FS_INFO, &shared_data.fsinfo);
	die_on(0 > err, "fsinfo ioctl failed\n");
	printf("fs UUID=");
	for (int i = 0; i < BTRFS_FSID_SIZE; i++)
		printf("%.2x", shared_data.fsinfo.fsid[i]);
	printf(", %d threads\n", omp_threads);
	memset(devices, -1, sizeof(devices));
	unsigned devices_found = 0;
	for (int i = 0; i < MAX_DEVID && devices_found < shared_data.fsinfo.num_devices; i++) {
		struct btrfs_ioctl_dev_info_args devinfo;
		memset(&devinfo, 0, sizeof(devinfo));
		devinfo.devid = i;
		err = ioctl(shared_data.mountfd, BTRFS_IOC_DEV_INFO, &devinfo);
		if (0 > err && ENODEV == errno)
			continue;
		die_on(0 > err, "devinfo ioctl failed\n");
		printf("found device %lld %.*s\n", (long long)devinfo.devid,
			BTRFS_DEVICE_PATH_NAME_MAX, (char*)devinfo.path);
		devices_found++;
		int tempfd = open((char*)devinfo.path, O_RDONLY|O_DIRECT);
		die_on(0 > tempfd, "Opening device %s failed\n", devinfo.path);
		devices[i] = tempfd;
	}
	die_on(devices_found < shared_data.fsinfo.num_devices, "Expected %d devices, found %d\n",
		shared_data.fsinfo.num_devices, devices_found);

	mtx_init(&shared_data.mutex, mtx_plain);
	cnd_init(&shared_data.condition);
	shared_data.consumer_offset = 0;
	shared_data.producer_offset = 0;
	shared_data.diskfds = devices;
	for (int i = 0; WORK_ITEM_COUNT > i; i++) {
		unsigned max_checksums = MAX_CHECKSUMS_PER_STRIPE * shared_data.fsinfo.num_devices;
		shared_data.work[i].checksums = malloc(max_checksums * sizeof(shared_data.work[i].checksums[0]));
		shared_data.work[i].bitmap = malloc(max_checksums / CHAR_BIT);
		shared_data.work[i].disk_offsets = malloc(shared_data.fsinfo.num_devices * sizeof(shared_data.work[i].disk_offsets[0]));
		shared_data.work[i].diskfds = malloc(shared_data.fsinfo.num_devices * sizeof(shared_data.work[i].diskfds[0]));
		shared_data.work[i].parent = &shared_data;
		shared_data.work[i].io_contexts = calloc(CSCRUB_AIO_INTERLEAVE_STAGES, sizeof(shared_data.work[i].io_contexts[0]));
		die_on(!shared_data.work[i].io_contexts, "Out of memory\n");
		shared_data.work[i].io_contexts[0].disk_buffers = malloc(CSCRUB_AIO_INTERLEAVE_STAGES * shared_data.fsinfo.num_devices * sizeof(u8*));
		die_on(!shared_data.work[i].io_contexts[0].disk_buffers, "Out of memory\n");
		err = posix_memalign((void**)&shared_data.work[i].parity, sysconf(_SC_PAGESIZE),
			MAX_INDEPENDENT_PARITY_STRIPES * shared_data.fsinfo.sectorsize * omp_threads);
		die_on(err, "Out of memory\n");
		const size_t disk_buffers_size = CSCRUB_AIO_INTERLEAVE_STAGES *
			shared_data.fsinfo.num_devices * PARALLEL_BLOCK_SIZE * sizeof(u8);
		err = posix_memalign((void**)shared_data.work[i].io_contexts[0].disk_buffers,
			sysconf(_SC_PAGESIZE), disk_buffers_size);
		die_on(err, "Out of memory\n");
		memset(shared_data.work[i].io_contexts[0].disk_buffers[0], 0, disk_buffers_size);
		for (unsigned io_context = 0; io_context < CSCRUB_AIO_INTERLEAVE_STAGES; io_context++) {
			err = io_setup(shared_data.fsinfo.num_devices, &shared_data.work[i].io_contexts[io_context].ctxp);
			die_on(err, "Out of memory\n");
			shared_data.work[i].io_contexts[io_context].disk_buffers =
				shared_data.work[i].io_contexts[0].disk_buffers + io_context * shared_data.fsinfo.num_devices;
			shared_data.work[i].io_contexts[io_context].disk_buffers[0] =
				shared_data.work[i].io_contexts[0].disk_buffers[0] + io_context * shared_data.fsinfo.num_devices * PARALLEL_BLOCK_SIZE;
			for (unsigned disk = 1; disk < shared_data.fsinfo.num_devices; disk++)
				shared_data.work[i].io_contexts[io_context].disk_buffers[disk] = shared_data.work[i].io_contexts[io_context].disk_buffers[0] + disk * PARALLEL_BLOCK_SIZE;
		}
	}

	/* work */
	thrd_t consumer_thread;
	thrd_create (&consumer_thread, consumer, &shared_data);
	btrfs_iterate_tree(shared_data.mountfd, BTRFS_CHUNK_TREE_OBJECTID, &shared_data, chunk_callback, NULL);

	/* exit */
	mtx_lock(&shared_data.mutex);
	if (shared_data.producer_offset > shared_data.consumer_offset + 1) {
		cnd_wait(&shared_data.condition, &shared_data.mutex);
	}
	struct work_item_data *work = shared_data.work + (shared_data.producer_offset % WORK_ITEM_COUNT);
	work->num_stripes = 0;
	work->logical_offset = ULLONG_MAX;
	shared_data.producer_offset++;
	cnd_broadcast(&shared_data.condition);
	mtx_unlock(&shared_data.mutex);
	thrd_join (consumer_thread, &err);
	
	/* cleanup */
	mtx_destroy(&shared_data.mutex);
	cnd_destroy(&shared_data.condition);
	for (int i = 0; WORK_ITEM_COUNT > i; i++) {
		free(shared_data.work[i].checksums);
		free(shared_data.work[i].bitmap);
		free(shared_data.work[i].disk_offsets);
		free(shared_data.work[i].diskfds);
		free(shared_data.work[i].io_contexts[0].disk_buffers[0]);
		free(shared_data.work[i].io_contexts[0].disk_buffers);
		free(shared_data.work[i].parity);
		for (unsigned io_context = 0; io_context < CSCRUB_AIO_INTERLEAVE_STAGES; io_context++) {
			io_destroy(shared_data.work[i].io_contexts[io_context].ctxp);
		}
		free(shared_data.work[i].io_contexts);
	}

	return EXIT_SUCCESS;
}
