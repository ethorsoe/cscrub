# Cscrub
This is a tool to check btrfs checksums on whole filesystem.

Supported data profiles
* Single
* BTRFS_BLOCK_GROUP_RAID5
* BTRFS_BLOCK_GROUP_RAID6
* BTRFS_BLOCK_GROUP_RAID0
* BTRFS_BLOCK_GROUP_RAID1
* BTRFS_BLOCK_GROUP_DUP (slow)
* BTRFS_BLOCK_GROUP_RAID1C3
* BTRFS_BLOCK_GROUP_RAID1C4

Not supported
raid10

# Compilation
* install libaio(-dev)
* gcc -o cscrub -O2 -fopenmp -Wall -Wextra -pedantic -std=c11 *.c -lpthread -laio

# Usage
```bash
cscrub /mount/point
```

# Caveats
Dup profile checking is slow and seeky, as the code things the allocations are
on different disks. Probably not worth fixing.

The code is subject to race conditions causing false positives on disk
stripes that see new writes during operation.

Currently only checksum checking is implemented and other means have to
be used for repair, like file rewrites. This feature could be added, but
this would maybe require adding code to recheck failures for false positives
using some more stable slow path. Checksums could be repaired by relocating
the block group with balance, reading the file (if appropriate) or rewriting
the file contents and using EXTENT_SAME ioctl to replace the broken data.

Big endian is not supported, adding support is trivial but would require at least
testing.

# Reporting bugs
Currently github is used to track bugs.

Unless the bug is quite obvious, attach at least output of
```bash
btrfs inspect-internal dump-tree -t 3 /dev/mapper/home
```
