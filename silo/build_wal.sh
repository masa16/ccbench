rm -rf build_wal
mkdir build_wal
cd build_wal
cmake -D WAL=1 -D DURABLE_EPOCH=1 ..
make -j`grep '^processor' /proc/cpuinfo | wc -l`
