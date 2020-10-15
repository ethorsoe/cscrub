#define _XOPEN_SOURCE 9000
#define _GNU_SOURCE
#include <stdint.h>
#include <unistd.h>
//#include <limits.h>
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
#include "libbtrfs.h"

#define MAX_DEVID 1024

void die(const char *msg) {
	perror(msg);
	exit(EXIT_FAILURE);
}

int chunk_callback(void* data, struct btrfs_ioctl_search_header* hdr, void *private){
	struct btrfs_chunk *chunk = data;
	if (BTRFS_CHUNK_ITEM_KEY != hdr->type || chunk->type != (BTRFS_BLOCK_GROUP_DATA|BTRFS_BLOCK_GROUP_RAID5))
		return 0;
	printf("chunk %lld type %lld\n", (long long)hdr->offset, (long long)chunk->type);
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

	btrfs_iterate_tree(mountfd, 3, NULL, chunk_callback, NULL);

	return EXIT_SUCCESS;
}
