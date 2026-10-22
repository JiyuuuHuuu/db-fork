#!/bin/bash
S="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A HOST LAN PUB
while read n lan pub h; do HOST[$n]=$h; LAN[$n]=$lan; PUB[$n]=$pub; done < nodes.txt
mkdir -p bw2
for i in 0 1 2 3 4 5 6 7; do $S JiyuHu23@${HOST[b$i]} 'pkill iperf3; iperf3 -s -D'; done
for i in 0 1 2 3 4 5 6 7; do for P in 1 8; do for R in "" "-R"; do tag=ab; [ -n "$R" ] && tag=ba
  f=bw2/${tag}_a$i-b${i}_P$P.json
  $S JiyuHu23@${HOST[a$i]} "iperf3 -c ${PUB[b$i]} -t 10 -O 2 -P $P $R -J" > $f
  python3 -c "import json;e=json.load(open('$f'))['end'];print('%s a$i-b$i P$P %.0f'%('$tag',e['sum_received']['bits_per_second']/1e6))"
done; done; done
for i in 0 1 2 3 4 5 6 7; do $S JiyuHu23@${HOST[b$i]} 'pkill iperf3'; done
