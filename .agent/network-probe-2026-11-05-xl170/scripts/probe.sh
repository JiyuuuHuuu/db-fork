#!/bin/bash
# All-pairs LAN probe of the 16-node xl170 cluster (one LAN, 10.10.1.0/24, ens1f1np1 / mlx5_3), run from the
# off-cluster coordinator. 15 rounds (circle method) of 8 disjoint pairs in parallel, each pair probed in both
# directions. Per ordered pair: ping (500 x 5 ms), TCP ping-pong (2000 x 64 B, tcprtt.py), ib_write_lat (RoCE v2,
# GID 3). Usage: probe.sh OUTDIR   (~10 min)
set -u
D=$(cd "$(dirname "$0")/.." && pwd); RAW=$1; mkdir -p $RAW/{ping,tcp,rdma}
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A LAN HOST; ORDER=()
while read n lan pub host; do LAN[$n]=$lan; HOST[$n]=$host; ORDER+=($n); done < $D/nodes.txt
for n in "${ORDER[@]}"; do (
  scp -q -i ~/.ssh/dassl_rsa $D/scripts/tcprtt.py JiyuHu23@${HOST[$n]}:/tmp/tcprtt.py
  $SSH JiyuHu23@${HOST[$n]} 'pkill -f "^python3 /tmp/tcprtt.py serve"; setsid nohup python3 /tmp/tcprtt.py serve 45678 </dev/null >/dev/null 2>&1 &' ) &
done; wait; sleep 1
one(){ s=$1; d=$2; sh=${HOST[$s]}; dh=${HOST[$d]}; ip=${LAN[$d]}
  $SSH JiyuHu23@$sh "sudo ping -q -c 500 -i 0.005 -W 1 $ip" > $RAW/ping/${s}_${d}.txt 2>&1
  $SSH JiyuHu23@$sh "timeout 30 python3 /tmp/tcprtt.py $ip 45678 2000 64" > $RAW/tcp/${s}_${d}.txt 2>&1
  $SSH JiyuHu23@$dh "timeout 30 ib_write_lat -d mlx5_3 -x 3 -F" > /dev/null 2>&1 & sleep 2
  $SSH JiyuHu23@$sh "timeout 30 ib_write_lat -d mlx5_3 -x 3 -F $ip" > $RAW/rdma/${s}_${d}.txt 2>&1; wait
}
M=${#ORDER[@]}
for r in $(seq 0 $((M-2))); do
  rot=("${ORDER[0]}"); for i in $(seq 0 $((M-2))); do rot+=("${ORDER[$(( 1 + (i + r) % (M-1) ))]}"); done
  echo "round $r: $(for i in $(seq 0 $((M/2-1))); do echo -n "${rot[$i]}-${rot[$((M-1-i))]} "; done)"
  for i in $(seq 0 $((M/2-1))); do x=${rot[$i]}; y=${rot[$((M-1-i))]}; ( one $x $y; one $y $x ) & done; wait
done
for n in "${ORDER[@]}"; do $SSH JiyuHu23@${HOST[$n]} 'pkill -f "^python3 /tmp/tcprtt.py serve"' & done; wait
