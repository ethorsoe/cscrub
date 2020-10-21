# Cscrub
This is a tool to check btrfs data checksums.

Supported data profiles
* Single
* BTRFS_BLOCK_GROUP_RAID5
* BTRFS_BLOCK_GROUP_RAID1
* BTRFS_BLOCK_GROUP_DUP (slow)
* BTRFS_BLOCK_GROUP_RAID1C3
* BTRFS_BLOCK_GROUP_RAID1C4

# Compilation
* install libaio(-dev)
* gcc -o cscrub -O2 -fopenmp -Wall -Wextra -pedantic -std=c11 *.c -lpthread -laio

# Caveats
Only data checksums are supported, supporting metadata
would be possible, but as using raid5 metadata is not sane, support
is not added for now.

Dup profile checking is slow and seeky, as the code things the allocations are
on different disks. Probably not worth fixing.

Raid 6 support is missing algebra, pull requests welcome.

The code is subject to race conditions causing false positives on disk
stripes that see new writes during operation.

Currently only checksum checking is implemented and other means have to
be used for repair, like file rewrites.