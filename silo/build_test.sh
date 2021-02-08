case "$1" in
    "d") o="-D CMAKE_BUILD_TYPE=Debug" ;;
    "r") o="-D CMAKE_BUILD_TYPE=Release" ;;
    *) o="-D CMAKE_BUILD_TYPE=Release" ;;
esac

c=$(grep '^processor' /proc/cpuinfo | wc -l)

build() {
  d=$1
  shift
  opt="$*"
  rm -rf $d
  mkdir $d
  cd $d
  cmake $o $opt ..
  make -j$c
  cd ..
}

build test_0log -D DURABLE_EPOCH=0
build test_nlog -D DURABLE_EPOCH=1
build test_nlog_deqmin -D DURABLE_EPOCH=1 -D DEQ_MIN_EPOCH=1
build test_nlog_diff2 -D DURABLE_EPOCH=1 -D DEQ_MIN_EPOCH=1 -D MAX_EPOCH_DIFF=2
build test_nlog_diff3 -D DURABLE_EPOCH=1 -D DEQ_MIN_EPOCH=1 -D MAX_EPOCH_DIFF=3
build test_nlog_diff4 -D DURABLE_EPOCH=1 -D DEQ_MIN_EPOCH=1 -D MAX_EPOCH_DIFF=4
#build test_0log_noidx -D DURABLE_EPOCH=0 -D MASSTREE_USE=0
#build test_nlog_noidx -D DURABLE_EPOCH=1 -D MASSTREE_USE=0
#build test_walpmem -D WAL=1 -D WALPMEM=0
