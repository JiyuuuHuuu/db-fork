#!/bin/bash
# iperf3 (10 s, 1 stream and 4 streams) on representative pairs, one at a time. Usage: iperf.sh OUTDIR
set -u
D=$(cd "$(dirname "$0")/.." && pwd); OUT=$1; mkdir -p $OUT
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A LAN PUB HOST
while read n lan pub host; do LAN[$n]=$lan; PUB[$n]=$pub; HOST[$n]=$host; done < $D/nodes.txt
for t in "a0 a1 lan" "b0 b1 lan" "a0 a1 pub" "a2 b2 pub" "b3 a3 pub" "a4 b4 pub"; do read s d p <<<"$t"
  ip=${LAN[$d]}; [ $p = pub ] && ip=${PUB[$d]}
  $SSH JiyuHu23@${HOST[$d]} "pkill iperf3; iperf3 -s -D -1"; sleep 1
  $SSH JiyuHu23@${HOST[$s]} "iperf3 -c $ip -t 10 -J" > $OUT/${s}_${d}_${p}_P1.json
  $SSH JiyuHu23@${HOST[$d]} "pkill iperf3; iperf3 -s -D -1"; sleep 1
  $SSH JiyuHu23@${HOST[$s]} "iperf3 -c $ip -t 10 -P 4 -J" > $OUT/${s}_${d}_${p}_P4.json
  echo "$s->$d $p: $(python3 -c "import json,sys;[print(round(json.load(open(f))['end']['sum_received']['bits_per_second']/1e9,2),end=' Gbit/s ') for f in sys.argv[1:]]" $OUT/${s}_${d}_${p}_P1.json $OUT/${s}_${d}_${p}_P4.json)(P1, P4)"
done
