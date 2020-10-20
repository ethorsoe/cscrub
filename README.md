# Cscrub
This is a tool to check btrfs raid5 data checksums.

# Compilation
* install libaio(-dev)
* gcc -o cscrub -O2 -fopenmp -Wall -Wextra -pedantic -std=c11 *.c -lpthread -laio

# Caveats
Only raid5 data checksums are supported, supporting metadata raid5
would be trivial, but as using raid5 metadata is not sane, support
is not added for now.

The code is subject to race conditions causing false positives on disk
stripes that see new writes during operation.

Currently only checksum checking is implemented and other means have to
be used for repair, like file rewrites.