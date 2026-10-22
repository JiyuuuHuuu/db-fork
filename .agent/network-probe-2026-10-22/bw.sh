#!/bin/bash
# usage: run from this directory (reads nodes.txt); writes bw/*.json
S="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A HOST LAN PUB
while read n lan pub h; do HOST[$n]=$h; LAN[$n]=$lan; PUB[$n]=$pub; done < nodes.txt
mkdir -p bw
for n in "${!HOST[@]}"; do $S JiyuHu23@${HOST[$n]} 'pkill iperf3; iperf3 -s -D' & done; wait
run() { # label src dstip P reverse
  local label=$1 src=$2 ip=$3 P=$4 R=$5
  $S JiyuHu23@${HOST[$src]} "iperf3 -c $ip -t 10 -O 2 -P $P $R -J" > bw/$label.json
  python3 -c "import json,sys;d=json.load(open('bw/$label.json'));e=d['end'];print('%-28s %8.0f Mbit/s retr=%s'%('$label',e['sum_received']['bits_per_second']/1e6,e['sum_sent'].get('retransmits')))"
}
run aa_lan_a0-a1_P1 a0 ${LAN[a1]} 1 ""
run aa_lan_a0-a1_P8 a0 ${LAN[a1]} 8 ""
run aa_lan_a2-a7_P8 a2 ${LAN[a7]} 8 ""
run aa_pub_a0-a1_P1 a0 ${PUB[a1]} 1 ""
run aa_pub_a0-a1_P8 a0 ${PUB[a1]} 8 ""
run bb_lan_b0-b1_P1 b0 ${LAN[b1]} 1 ""
run bb_lan_b0-b1_P8 b0 ${LAN[b1]} 8 ""
run bb_lan_b2-b7_P8 b2 ${LAN[b7]} 8 ""
run bb_pub_b0-b1_P1 b0 ${PUB[b1]} 1 ""
run bb_pub_b0-b1_P8 b0 ${PUB[b1]} 8 ""
for p in "a0 b0" "a3 b5" "a6 b2"; do set -- $p
  run ab_pub_$1-$2_P1 $1 ${PUB[$2]} 1 ""
  run ab_pub_$1-$2_P8 $1 ${PUB[$2]} 8 ""
  run ba_pub_$2-$1_P1 $1 ${PUB[$2]} 1 "-R"
  run ba_pub_$2-$1_P8 $1 ${PUB[$2]} 8 "-R"
done
# aggregate: all 8 cross pairs at once, each a_i -> b_i with 4 streams, then reverse
for dir in "" "-R"; do tag=ab; [ -n "$dir" ] && tag=ba
  for i in 0 1 2 3 4 5 6 7; do $S JiyuHu23@${HOST[a$i]} "iperf3 -c ${PUB[b$i]} -t 15 -O 3 -P 4 $dir -J" > bw/agg_${tag}_a$i-b$i.json & done; wait
  python3 -c "import json,glob;t=0
for f in sorted(glob.glob('bw/agg_${tag}_*.json')):
  b=json.load(open(f))['end']['sum_received']['bits_per_second']/1e6; t+=b; print('  %-24s %8.0f'%(f[3:-5],b))
print('agg_${tag} total %.0f Mbit/s'%t)"
done
# aggregate within zone over LAN: a_i -> a_{i+4}, b_i -> b_{i+4}, 4 pairs each at once
for g in a b; do for i in 0 1 2 3; do j=$((i+4)); $S JiyuHu23@${HOST[$g$i]} "iperf3 -c ${LAN[$g$j]} -t 15 -O 3 -P 4 -J" > bw/agglan_${g}_$g$i-$g$j.json & done; done; wait
python3 -c "import json,glob
for g in 'ab':
  t=0
  for f in sorted(glob.glob('bw/agglan_%s_*.json'%g)):
    b=json.load(open(f))['end']['sum_received']['bits_per_second']/1e6; t+=b; print('  %-24s %8.0f'%(f[3:-5],b))
  print('agglan_%s total %.0f Mbit/s'%(g,t))"
for n in "${!HOST[@]}"; do $S JiyuHu23@${HOST[$n]} 'pkill iperf3' & done; wait
