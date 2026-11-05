#!/bin/bash
# All-pairs probe of the 16-node xl170 cluster: 15 rounds (circle method) of 8 disjoint pairs run in parallel, each
# pair probed in both directions, one test at a time. Per ordered pair:
#   same zone : ping + TCP ping-pong over LAN and public; ib_write_lat over LAN (mlx5_3)
#   cross zone: ping + TCP ping-pong over public; 3-probe LAN reachability check; ib_write_lat over public (mlx5_0)
# Usage: probe.sh OUTDIR     (~10 min; run from a0)
set -u
D=$(cd "$(dirname "$0")/.." && pwd); RAW=$1; mkdir -p $RAW/{ping,tcp,rdma,trace}
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A LAN PUB HOST; ORDER=()
while read n lan pub host; do LAN[$n]=$lan; PUB[$n]=$pub; HOST[$n]=$host; ORDER+=($n); done < $D/nodes.txt
for n in "${ORDER[@]}"; do (
  scp -q -i ~/.ssh/dassl_rsa $D/scripts/tcprtt.py JiyuHu23@${HOST[$n]}:/tmp/tcprtt.py
  $SSH JiyuHu23@${HOST[$n]} 'pkill -f "^python3 /tmp/tcprtt.py serve"; setsid nohup python3 /tmp/tcprtt.py serve 45678 </dev/null >/dev/null 2>&1 &' ) &
done; wait; sleep 1
one(){ s=$1; d=$2; sh=${HOST[$s]}; dh=${HOST[$d]}
  if [ ${s:0:1} = ${d:0:1} ]; then paths="lan pub"; rdev=mlx5_3; rip=${LAN[$d]}; rpath=lan
  else paths="pub"; rdev=mlx5_0; rip=${PUB[$d]}; rpath=pub
    $SSH JiyuHu23@$sh "ping -q -c 3 -i 0.2 -W 1 ${LAN[$d]}" > $RAW/ping/${s}_${d}_lan.txt 2>&1; fi
  for p in $paths; do ip=${LAN[$d]}; [ $p = pub ] && ip=${PUB[$d]}
    $SSH JiyuHu23@$sh "sudo ping -q -c 500 -i 0.005 -W 1 $ip" > $RAW/ping/${s}_${d}_$p.txt 2>&1
    $SSH JiyuHu23@$sh "timeout 30 python3 /tmp/tcprtt.py $ip 45678 2000 64" > $RAW/tcp/${s}_${d}_$p.txt 2>&1
  done
  $SSH JiyuHu23@$dh "timeout 30 ib_write_lat -d $rdev -x 3 -F" > /dev/null 2>&1 & sleep 2
  $SSH JiyuHu23@$sh "timeout 30 ib_write_lat -d $rdev -x 3 -F $rip" > $RAW/rdma/${s}_${d}_$rpath.txt 2>&1; wait
  $SSH JiyuHu23@$sh "tracepath -n ${PUB[$d]}" > $RAW/trace/${s}_${d}_pub.txt 2>&1
}
M=${#ORDER[@]}
for r in $(seq 0 $((M-2))); do
  # circle method: fix ORDER[0], rotate the others
  rot=("${ORDER[0]}"); for i in $(seq 0 $((M-2))); do rot+=("${ORDER[$(( 1 + (i + r) % (M-1) ))]}"); done
  echo "round $r: $(for i in $(seq 0 $((M/2-1))); do echo -n "${rot[$i]}-${rot[$((M-1-i))]} "; done)"
  for i in $(seq 0 $((M/2-1))); do x=${rot[$i]}; y=${rot[$((M-1-i))]}; ( one $x $y; one $y $x ) & done; wait
done
for n in "${ORDER[@]}"; do $SSH JiyuHu23@${HOST[$n]} 'pkill -f "^python3 /tmp/tcprtt.py serve"' & done; wait
