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

build test1 -D WAL=0 -D DURABLE_EPOCH=0 -D NOTIFIER_THREAD=0
#build test2 -D WAL=1 -D DURABLE_EPOCH=0 -D NOTIFIER_THREAD=0
build test3 -D WAL=0 -D DURABLE_EPOCH=1 -D NOTIFIER_THREAD=0
build test4 -D WAL=0 -D DURABLE_EPOCH=1 -D NOTIFIER_THREAD=1
#build test5 -D WAL=1 -D DURABLE_EPOCH=0 -D NOTIFIER_THREAD=0 -D WALPMEM=1
#build test6 -D WAL=1 -D DURABLE_EPOCH=0 -D NOTIFIER_THREAD=0 -D WALPMEM=1 -D PMEMCPY=1
#build test7 -D WAL=0 -D DURABLE_EPOCH=1 -D NOTIFIER_THREAD=0 -D WALPMEM=1
#build test8 -D WAL=0 -D DURABLE_EPOCH=1 -D NOTIFIER_THREAD=0 -D WALPMEM=0 -D PMEMCPY=1
#build test9 -D WAL=0 -D DURABLE_EPOCH=1 -D NOTIFIER_THREAD=1 -D WALPMEM=1
