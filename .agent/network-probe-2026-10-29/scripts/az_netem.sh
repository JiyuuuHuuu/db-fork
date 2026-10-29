#!/bin/bash
# Emulate two cloud availability zones: g1* = zone 1, g2* = zone 2 (nodes.txt).
# Adds ONE_WAY_US of egress delay on the public NIC for packets to the other zone's public IPs, so cross-zone
# RTT grows by 2*ONE_WAY_US. Same-zone traffic (LAN or public) and ssh from outside are untouched.
# Usage: az_netem.sh apply [ONE_WAY_US=400] | remove | status
set -eu
D=$(cd "$(dirname "$0")/.." && pwd)
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
DEV=enp1s0f0
ACTION=${1:-status}; US=${2:-400}
mapfile -t N < $D/nodes.txt
for l in "${N[@]}"; do read n lan pub host <<<"$l"
  peers=$(for m in "${N[@]}"; do read pn plan ppub ph <<<"$m"; if [ "${pn:0:2}" != "${n:0:2}" ]; then echo $ppub; fi; done)
  case $ACTION in
  apply)
    filters=$(for ip in $peers; do echo "sudo tc filter add dev $DEV parent 1: protocol ip prio 1 u32 match ip dst $ip/32 flowid 1:4;"; done)
    # prio with 4 bands: the default priomap only uses bands 1-3, so band 4 (netem) gets only the filtered traffic.
    $SSH JiyuHu23@$host "sudo tc qdisc del dev $DEV root 2>/dev/null; \
      sudo tc qdisc add dev $DEV root handle 1: prio bands 4 && \
      sudo tc qdisc add dev $DEV parent 1:4 handle 40: netem delay ${US}us limit 10000 && $filters true"
    echo "$n: +${US}us one-way to" $peers ;;
  remove)
    $SSH JiyuHu23@$host "sudo tc qdisc del dev $DEV root 2>/dev/null; true"; echo "$n: removed (back to default mq)" ;;
  status)
    echo "== $n"; $SSH JiyuHu23@$host "tc qdisc show dev $DEV | grep -v fq_codel; tc filter show dev $DEV | grep -E 'match|flowid'" ;;
  esac
done
