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
#include "libbtrfs.h"

struct work_item_data {
	unsigned char *bitmap;
	u32 *checksums;
	u64 *disk_offsets;
	int *diskfds;
	u64 logical_offset;
	u64 length;
	u64 stripe_len;
	u32 num_stripes;
};

struct shared_data {
	mtx_t mutex;
	cnd_t condition;
	int *diskfds;
	unsigned consumer_offset;
	unsigned producer_offset;
	unsigned max_stripes;
	struct work_item_data work[2];
};

#define MAX_DEVID 1024
#define MAX_CHECKSUMS_PER_STRIPE (1ULL << 18)

void die(const char *msg) {
	perror(msg);
	exit(EXIT_FAILURE);
}

int consumer(void *private) {
	struct shared_data *shared_data = private;
	mtx_lock(&shared_data->mutex);
	while(1) {
		if (shared_data->producer_offset > shared_data->consumer_offset) {
		} else {
			cnd_wait(&shared_data->condition, &shared_data->mutex);
		}
		struct work_item_data *work = shared_data->work + (shared_data->consumer_offset % 2);
		printf("consumer processing %llu\n", (unsigned long long)work->logical_offset);
		mtx_unlock(&shared_data->mutex);
		if (!work->num_stripes)
			break;
		mtx_lock(&shared_data->mutex);
		printf("consumer processed %llu\n", (unsigned long long)work->logical_offset);
		shared_data->consumer_offset++;
		cnd_broadcast(&shared_data->condition);
	}
	return 0;
}

int chunk_callback(void* data, struct btrfs_ioctl_search_header* hdr, void *private){
	struct btrfs_chunk *chunk = data;
	struct shared_data *shared_data = private;
	if (BTRFS_CHUNK_ITEM_KEY != hdr->type || chunk->type != (BTRFS_BLOCK_GROUP_DATA|BTRFS_BLOCK_GROUP_RAID5))
		return 0;
	struct btrfs_stripe *stripes = &chunk->stripe;
	mtx_lock(&shared_data->mutex);
	if (shared_data->producer_offset > shared_data->consumer_offset + 1) {
		cnd_wait(&shared_data->condition, &shared_data->mutex);
	}
	struct work_item_data *work = shared_data->work + (shared_data->producer_offset % 2);
	work->logical_offset = hdr->offset;
	work->length = chunk->length;
	work->stripe_len = chunk->stripe_len;
	work->num_stripes = chunk->num_stripes;
	if (shared_data->max_stripes < work->num_stripes)
		die("max_stripes");
	for (unsigned i = 0; i < work->num_stripes; i++) {
		unsigned devid = stripes[i].devid;
		if (MAX_DEVID <= devid)
			die("devid");
		work->diskfds[i] = shared_data->diskfds[devid];
		work->disk_offsets[i] = stripes[i].offset;
	}
	printf("chunk %lld type %lld\n", (long long)hdr->offset, (long long)chunk->type);
	mtx_unlock(&shared_data->mutex);

	unsigned max_checksums = MAX_CHECKSUMS_PER_STRIPE * (work->num_stripes - 1);
	memset(work->bitmap, 0, max_checksums / CHAR_BIT);

	mtx_lock(&shared_data->mutex);
	printf("submitted chunk %lld type %lld\n", (long long)hdr->offset, (long long)chunk->type);
	shared_data->producer_offset++;
	cnd_broadcast(&shared_data->condition);
	mtx_unlock(&shared_data->mutex);
	return 0;
}

int main (int argc, char **argv) {
	int devices[MAX_DEVID]; 
	if (argc != 2)
		die("usage: aa <filename>");
	int mountfd = open(argv[1], O_RDONLY);
	if (0 > mountfd)
		die("open");
	struct btrfs_ioctl_fs_info_args fsinfo;
	int err = ioctl(mountfd, BTRFS_IOC_FS_INFO, &fsinfo);
	if (0 > err)
		die("fsinfo");
	printf("fs UUID=");
	for (int i = 0; i < BTRFS_FSID_SIZE; i++)
		printf("%.2x", fsinfo.fsid[i]);
	printf("\n");
	memset(devices, -1, sizeof(devices));
	unsigned devices_found = 0;
	for (int i = 0; i < MAX_DEVID && devices_found < fsinfo.num_devices; i++) {
		struct btrfs_ioctl_dev_info_args devinfo;
		memset(&devinfo, 0, sizeof(devinfo));
		devinfo.devid = i;
		err = ioctl(mountfd, BTRFS_IOC_DEV_INFO, &devinfo);
		if (0 > err && ENODEV == errno)
			continue;
		if (0 > err)
			die("devinfo");
		printf("found device %lld %.*s\n", (long long)devinfo.devid,
			BTRFS_DEVICE_PATH_NAME_MAX, (char*)devinfo.path);
		devices_found++;
		int tempfd = open((char*)devinfo.path, O_RDONLY); 
		if (0 > tempfd)
			die("open dev");
		devices[i] = tempfd;
	}
	if (devices_found < fsinfo.num_devices)
		die("some devices not found");

	struct shared_data shared_data;
	mtx_init(&shared_data.mutex, mtx_plain);
	cnd_init(&shared_data.condition);
	shared_data.consumer_offset = 0;
	shared_data.producer_offset = 0;
	shared_data.max_stripes = fsinfo.num_devices;
	shared_data.diskfds = devices;
	for (int i = 0; 2 > i; i++) {
		unsigned max_checksums = MAX_CHECKSUMS_PER_STRIPE * (fsinfo.num_devices - 1);
		shared_data.work[i].checksums = malloc(max_checksums * sizeof(shared_data.work[i].checksums[0]));
		shared_data.work[i].bitmap = malloc(max_checksums / CHAR_BIT);
		shared_data.work[i].disk_offsets = malloc(fsinfo.num_devices * sizeof(shared_data.work[i].disk_offsets[0]));
		shared_data.work[i].diskfds = malloc(fsinfo.num_devices * sizeof(shared_data.work[i].diskfds[0]));
	}

	/* work */
	thrd_t consumer_thread;
	thrd_create (&consumer_thread, consumer, &shared_data);
	btrfs_iterate_tree(mountfd, 3, &shared_data, chunk_callback, NULL);

	/* exit */
	mtx_lock(&shared_data.mutex);
	if (shared_data.producer_offset > shared_data.consumer_offset + 1) {
		cnd_wait(&shared_data.condition, &shared_data.mutex);
	}
	struct work_item_data *work = shared_data.work + (shared_data.producer_offset % 2);
	work->num_stripes = 0;
	work->logical_offset = ULLONG_MAX;
	shared_data.producer_offset++;
	cnd_broadcast(&shared_data.condition);
	mtx_unlock(&shared_data.mutex);
	thrd_join (consumer_thread, &err);
	
	/* cleanup */
	mtx_destroy(&shared_data.mutex);
	cnd_destroy(&shared_data.condition);
	for (int i = 0; 2 > i; i++) {
		free(shared_data.work[i].checksums);
		free(shared_data.work[i].bitmap);
		free(shared_data.work[i].disk_offsets);
		free(shared_data.work[i].diskfds);
	}

	return EXIT_SUCCESS;
}
