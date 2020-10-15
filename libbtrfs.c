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
#include "libbtrfs.h"


#define MEBI (1024*1024)
int64_t btrfs_iterate_tree(int fd, uint64_t tree, void *private, int (*callback)(void*, struct btrfs_ioctl_search_header*, void*), struct btrfs_ioctl_search_key *key) {
	assert(0<=fd);
	assert(NULL != callback);
	struct btrfs_ioctl_search_args_v2 *args=calloc(MEBI+offsetof(struct btrfs_ioctl_search_args_v2,buf),1);
	if (NULL == args)
		return -ENOMEM;
	if (NULL != key) {
		memcpy(&args->key, key, offsetof(struct btrfs_ioctl_search_args_v2,buf));
	} else {
		args->key.tree_id=tree;
		args->key.max_objectid=-1ULL;
		args->key.max_type=-1U;
		args->key.max_offset=-1ULL;
		args->key.max_transid=-1ULL;
		args->buf_size=MEBI;
	}
	struct btrfs_ioctl_search_header *sh;
	int64_t ret=0;

	do {
		args->key.nr_items=-1U;
		if (ioctl(fd, BTRFS_IOC_TREE_SEARCH_V2, args)) {
			ret=-errno;
			goto out;
		}
		//assume Buffer of MEBI does not fit MEBI items
		assert(MEBI > args->key.nr_items);
		if (0 == args->key.nr_items)
			break;

		sh=(struct btrfs_ioctl_search_header*)args->buf;
		for (uint64_t i=0; i < args->key.nr_items; i++) {
			char *temp=(char*)(sh+1);
			if ((ret=callback(temp, sh, private)))
				goto out;

			args->key.min_offset=sh->offset+1;
			args->key.min_type=sh->type;
			args->key.min_objectid=sh->objectid;
			sh=(struct btrfs_ioctl_search_header*)(sh->len+temp);
		}
		ret+=args->key.nr_items;
	} while (1);

out:
	free(args);
	return ret;
}
