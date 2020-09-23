mkdir -p build
cd build
cmake ..
make -j`grep '^processor' /proc/cpuinfo | wc -l`
