#pragma once

#include <btrfs/ioctl.h>
int64_t btrfs_iterate_tree(int fd, uint64_t tree, void *private, int (*callback)(void*, struct btrfs_ioctl_search_header*, void*), struct btrfs_ioctl_search_key *key);
