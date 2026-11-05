#!/bin/bash
# LAN throughput, iperf3 10 s. (1) pairs one at a time, 1 and 4 streams; (2) the pgcopy seed pattern: n0 sends to
# all 15 other nodes at once (one iperf3 server per client on n0, -R so n0 is the sender), 1 stream each, then the
# reverse (15 nodes send to n0). Usage: iperf.sh OUTDIR
set -u
D=$(cd "$(dirname "$0")/.." && pwd); OUT=$1; mkdir -p $OUT
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A LAN HOST; ORDER=()
while read n lan pub host; do LAN[$n]=$lan; HOST[$n]=$host; ORDER+=($n); done < $D/nodes.txt
gb(){ python3 -c "import json,sys;print(' '.join(str(round(json.load(open(f))['end']['sum_received']['bits_per_second']/1e9,2)) for f in sys.argv[1:]))" "$@"; }
for t in "n0 n1" "n1 n0" "n2 n9" "n5 n14" "n7 n8" "n12 n3"; do read s d <<<"$t"
  for P in 1 4; do
    $SSH JiyuHu23@${HOST[$d]} "pkill -x iperf3; iperf3 -s -D -1"; sleep 1
    $SSH JiyuHu23@${HOST[$s]} "iperf3 -c ${LAN[$d]} -t 10 -P $P -J" > $OUT/pair_${s}_${d}_P$P.json
  done
  echo "$s->$d: $(gb $OUT/pair_${s}_${d}_P1.json $OUT/pair_${s}_${d}_P4.json) Gbit/s (P1 P4)"
done
for dir in fanout fanin; do
  $SSH JiyuHu23@${HOST[n0]} "pkill -x iperf3; for p in \$(seq 5201 5215); do iperf3 -s -D -1 -p \$p; done"; sleep 1
  i=0; for n in "${ORDER[@]:1}"; do p=$((5201+i)); i=$((i+1))
    R=""; [ $dir = fanout ] && R="-R"
    $SSH JiyuHu23@${HOST[$n]} "iperf3 -c ${LAN[n0]} -p $p -t 10 $R -J" > $OUT/${dir}_${n}.json &
  done; wait
  echo "$dir (n0 $( [ $dir = fanout ] && echo sends to || echo receives from ) 15 nodes): per node $(gb $OUT/${dir}_n*.json)"
  python3 -c "import json,sys;print('  total', round(sum(json.load(open(f))['end']['sum_received']['bits_per_second'] for f in sys.argv[1:])/1e9,2), 'Gbit/s')" $OUT/${dir}_n*.json
done
