affinity_list='
112:0,1+116:4,5
112:0,1,2,3+116:4,5,6,10
112:0,1,2,3,7,8,9,14+116:4,5,6,10,11,12,13,18
112:0,1,2,3,7,8,9,14,15,16+116:4,5,6,10,11,12,13,18,19,20
112:0,1,2,3,7,8,9,14,15,16,17,21+116:4,5,6,10,11,12,13,18,19,20,24,25
112:0,1,2,3,7,8,9,14,15,16,17,21,22,23+116:4,5,6,10,11,12,13,18,19,20,24,25,26,27
112:0,1,2,3,7,8,9,14,15,16,17,21,22,23,4,5,6,10,11,12,13+140:28,29,30,31,35,36,37,42,43,44,45,49,50,51,32,33,34,38,39,40,41
112:0,1,2,3,7,8,9,14,15,16,17,21,22,23,4,5,6,10,11,12,13,18,19,20,24,25,26,27+140:28,29,30,31,35,36,37,42,43,44,45,49,50,51,32,33,34,38,39,40,41,46,47,48,52,53,54,55
'
repeat=1
cmd='./silo.exe -extime 1 -clocks_per_us 2095 -buffer_num 4'

b=`basename ${0%.sh}`

for a in $affinity_list; do
  for j in $(seq $repeat); do
    sleep 0
    c="numactl --localalloc $cmd -affinity $a"
    echo $c
    $c | tee -a $b.log
  done
done

echo "thread_num,logger_num,throughput[tps],abort_rate,durabule_latency,log_throughput[B/s],backpressure_latency_rate,write_latency_rate" | tee $d.csv

awk '
  /#FLAGS_thread_num:/ {i=$2}
  /#FLAGS_logger_num:/ {l=$2}
  /^abort_rate:/ {a=$2}
  /^durable_latency\[ms\]:/ {d=$2}
  /^throughput\(elap\)\[B\/s\]:/ {t=$2}
  /^backpressure_latency_rate:/ {b=$2}
  /^wait_time\[s\]:/ {x=$2}
  /^write_time\[s\]:/ {w=$2}
  /^throughput\[tps\]:/ {printf("%s,%s,%s,%s,%s,%s,%s,%f\n",i,l,$2,a,d,t,b,w/(x+w));i=l=a=d=t=b=x=w=""}
' $b.log | tee -a $b.csv
