#ycsbA-xrs-cache.sh(si)
tuple=100000000
maxope=10
thread=224
rratio=50
rmw=off
skew=0.9
ycsb=on
cpumhz=2100
gc_inter_us=10
extime=3
epoch=3

host=`hostname`
chris41="chris41.omni.hpcc.jp"
dbs11="dbs11"

#basically
thread=24
if  test $host = $dbs11 ; then
thread=224
fi

result=result_si_ycsbA_tuple100m_skew09_val4-1k.dat
rm $result
echo "#worker threads, avg-tps, min-tps, max-tps, avg-ar, min-ar, max-ar, avg-camiss, min-camiss, max-camiss" >> $result
echo "#sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ../si.exe tuple $maxope $thread $rratio $rmw $skew $ycsb $cpumhz $gc_inter_us $extime" >> $result

for ((val = 4; val <= 1000; val += 100))
do
  if test $val = 104 ; then
    val=100
  fi
  cd ../
  make clean; make -j10 VAL_SIZE=$val
  cd script

  echo "sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ../si.exe $tuple $maxope $thread $rratio $rmw $skew $ycsb $cpumhz $gc_inter_us $extime"
  echo "value size: $val"
  
  sumTH=0
  sumAR=0
  sumCA=0
  maxTH=0
  maxAR=0
  maxCA=0
  minTH=0
  minAR=0
  minCA=0
  for ((i = 1; i <= epoch; ++i))
  do
    if test $host = $dbs11 ; then
      sudo perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ../si.exe $tuple $maxope $thread $rratio $rmw $skew $ycsb $cpumhz $gc_inter_us $extime > exp.txt
    fi
    if test $host = $chris41 ; then
      perf stat -e cache-misses,cache-references -o ana.txt numactl --interleave=all ../si.exe $tuple $maxope $thread $rratio $rmw $skew $ycsb $cpumhz $gc_inter_us $extime > exp.txt
    fi
  
    tmpTH=`grep Throughput ./exp.txt | awk '{print $2}'`
    tmpAR=`grep abort_rate ./exp.txt | awk '{print $2}'`
    tmpCA=`grep cache-misses ./ana.txt | awk '{print $4}'`
    sumTH=`echo "$sumTH + $tmpTH" | bc`
    sumAR=`echo "scale=4; $sumAR + $tmpAR" | bc | xargs printf %.4f`
    sumCA=`echo "$sumCA + $tmpCA" | bc`
    echo "tmpTH: $tmpTH, tmpAR: $tmpAR, tmpCA: $tmpCA"
  
    if test $i -eq 1 ; then
      maxTH=$tmpTH
      maxAR=$tmpAR
      maxCA=$tmpCA
      minTH=$tmpTH
      minAR=$tmpAR
      minCA=$tmpCA
    fi
  
    flag=`echo "$tmpTH > $maxTH" | bc`
    if test $flag -eq 1 ; then
      maxTH=$tmpTH
    fi
    flag=`echo "$tmpAR > $maxAR" | bc`
    if test $flag -eq 1 ; then
      maxAR=$tmpAR
    fi
    flag=`echo "$tmpCA > $maxCA" | bc`
    if test $flag -eq 1 ; then
      maxCA=$tmpCA
    fi
  
    flag=`echo "$tmpTH < $minTH" | bc`
    if test $flag -eq 1 ; then
      minTH=$tmpTH
    fi
    flag=`echo "$tmpAR < $minAR" | bc`
    if test $flag -eq 1 ; then
      minAR=$tmpAR
    fi
    flag=`echo "$tmpCA < $minCA" | bc`
    if test $flag -eq 1 ; then
      minCA=$tmpCA
    fi
  
  done
  avgTH=`echo "$sumTH / $epoch" | bc`
  avgAR=`echo "scale=4; $sumAR / $epoch" | bc | xargs printf %.4f`
  avgCA=`echo "$sumCA / $epoch" | bc`
  echo "sumTH: $sumTH, sumAR: $sumAR, sumCA: $sumCA"
  echo "avgTH: $avgTH, avgAR: $avgAR, avgCA: $avgCA"
  echo "maxTH: $maxTH, maxAR: $maxAR, maxCA: $maxCA"
  echo "minTH: $minTH, minAR: $minAR, minCA: $minCA"
  echo ""
  echo "$val $avgTH $minTH $maxTH $avgAR $minAR $maxAR, $avgCA $minCA $maxCA" >> $result
done
