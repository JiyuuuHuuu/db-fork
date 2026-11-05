#!/bin/bash
# Does host tc netem delay RoCE? Applies ONE_WAY_US egress delay on the control NIC of a0 and b0 for packets
# to the other's public IP, runs ping/TCP/ib_*_lat/ib_write_bw a0->b0 over the control network, removes the delay.
# Usage: netem_rdma_pair.sh OUTDIR [ONE_WAY_US=400]   (run on a0 = hp155)
set -u
D=$(cd "$(dirname "$0")/.." && pwd); OUT=$1; US=${2:-400}; mkdir -p $OUT
DEV=eno49np0; A=128.110.218.194; B=128.110.218.179
SB="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes JiyuHu23@hp140.utah.cloudlab.us"
setq(){ echo "sudo tc qdisc del dev $DEV root 2>/dev/null; sudo tc qdisc add dev $DEV root handle 1: prio bands 4 && sudo tc qdisc add dev $DEV parent 1:4 handle 40: netem delay ${US}us limit 100000 && sudo tc filter add dev $DEV parent 1: protocol ip prio 1 u32 match ip dst $1/32 flowid 1:4"; }
run(){ tag=$1
  sudo ping -c 500 -i 0.005 $B > $OUT/ping_$tag.txt
  for t in ib_write_lat ib_read_lat ib_send_lat ib_write_bw; do
    $SB "timeout 90 $t -d mlx5_0 -x 3 -F" > $OUT/${t}_${tag}_server.txt 2>&1 & sleep 2
    timeout 90 $t -d mlx5_0 -x 3 -F $B > $OUT/${t}_$tag.txt 2>&1; wait
  done
  tc -s qdisc show dev $DEV > $OUT/tc_a0_$tag.txt; $SB "tc -s qdisc show dev $DEV" > $OUT/tc_b0_$tag.txt
}
run base
eval "$(setq $B)"; $SB "$(setq $A)"
run netem${US}
sudo tc qdisc del dev $DEV root; $SB "sudo tc qdisc del dev $DEV root"
tc qdisc show dev $DEV | head -2
