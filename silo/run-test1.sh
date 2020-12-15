#! /bin/bash

numa_nodes=8
repeat=5
opt='-extime 2 -clocks_per_us 2095 -rratio 0'

run() {
  local b=$1
  mkdir -p $b
  for w in $core_nums; do
    for j in $(seq $repeat); do
      if [ $w -gt $logger_per_node ]; then
        sleep 4
        if [ $logger_per_node -gt 0 ]; then
          local numa="$(dirname $0)/numa.rb $numa_nodes $((w-logger_per_node)) $logger_per_node"
          echo $numa | tee -a $b.log
          local a=`$numa`
          local c="numactl --interleave=all ../test_nlog/silo.exe $opt -buffer_num $buffer_num -buffer_size $buffer_size -affinity $a"
        else
          local c="numactl --interleave=all ../test_0log/silo.exe $opt -thread_num $((w*numa_nodes))"
        fi
        echo $c | tee -a $b.log
        $c |& tee -a $b.log
        if [ -f latency.dat ]; then
          mv latency.dat $b/latency-l$logger_per_node-w$((w*numa_nodes))-bn$buffer_num-bs$buffer_size-r$j.dat
        fi
      fi
    done
  done
}

log2csv() {
  local b=$1
  echo "thread_num,logger_num,buffer_num,buffer_size,clocks_per_us,epoch_time,extime,max_ope,rmw,rratio,tuple_num,ycsb,zipf_skew,throughput[tps],abort_rate,durabule_latency,log_throughput[B/s],backpressure_latency_rate,write_latency_rate,write_count,buffer_count,byte_count" | tee $b.csv
  awk '
  /#FLAGS_clocks_per_us:/ {c1=$2;
    c2=c3=c4=c5=c6=c7=c8=c9=c10=c11=c12=c13=a=d=t=b=x=w=n=f=s=r="";}
  /#FLAGS_epoch_time:/ {c2=$2}
  /#FLAGS_extime:/ {c3=$2}
  /#FLAGS_max_ope:/ {c4=$2}
  /#FLAGS_rmw:/ {c5=$2}
  /#FLAGS_rratio:/ {c6=$2}
  /#FLAGS_thread_num:/ {c7=$2}
  /#FLAGS_logger_num:/ {c8=$2}
  /#FLAGS_buffer_num:/ {c9=$2}
  /#FLAGS_buffer_size:/ {c10=$2}
  /#FLAGS_tuple_num:/ {c11=$2}
  /#FLAGS_ycsb:/ {c12=$2}
  /#FLAGS_zipf_skew:/ {c13=$2}
  /^abort_rate:/ {a=$2}
  /^durable_latency\[ms\]:/ {d=$2}
  /^throughput\(elap\)\[B\/s\]:/ {t=$2}
  /^backpressure_latency_rate:/ {b=$2}
  /^wait_time\[s\]:/ {x=$2}
  /^write_time\[s\]:/ {w=$2}
  /^write_count:/ {n=$2}
  /^buffer_count:/ {f=$2}
  /^byte_count\[B\]:/ {s=$2}
  /^throughput\[tps\]:/ { if(x+w>0){r=w/(x+w)}
    printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
           c7,c8,c9,c10,c1,c2,c3,c4,c5,c6,c11,c12,c13,$2,a,d,t,b,r,n,f,s);
  }
' $b.log | tee -a $b.csv
}

b=`basename ${0%.sh}`

for logger_per_node in 0 1 2 3; do
  core_nums='1 2 4 8 12 16 20 24 28'
  #core_nums='28'
  if [ $logger_per_node -eq 0 ]; then
    run $b
  else
    for buffer_num in 2 4 8; do
      #for buffer_size in 64 128 256 512 768 1024 1280 1536 2048; do
      for buffer_size in 512; do
        run $b
        rm -f log*/*
      done
    done
  fi
done

log2csv $b
