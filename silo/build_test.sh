case "$1" in
    "d") o="-D CMAKE_BUILD_TYPE=Debug" ;;
    "r") o="-D CMAKE_BUILD_TYPE=Release" ;;
    *) o="-D CMAKE_BUILD_TYPE=Release" ;;
esac

c=$(grep '^processor' /proc/cpuinfo | wc -l)

build() {
  local d=$1
  shift
  local opt="$*"
  rm -rf $d
  mkdir $d
  cd $d
  cmake $o $opt ..
  make -j$c
  cd ..
}

build test_0log -D DURABLE_EPOCH=0 &
build test_nlog -D DURABLE_EPOCH=1 &
build test_0log_noidx -D MASSTREE_USE=0 -D DURABLE_EPOCH=0 &
build test_nlog_noidx -D MASSTREE_USE=0 -D DURABLE_EPOCH=1 &
#build test_walpmem -D WAL=1 -D WALPMEM=0 &
wait
