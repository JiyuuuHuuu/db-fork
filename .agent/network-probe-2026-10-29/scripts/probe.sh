#!/bin/bash
# Probe RTT between every ordered pair of nodes in nodes.txt, over LAN and public IPs, one pair at a time.
set -u
D=$(cd "$(dirname "$0")/.." && pwd); RAW=${1:-$D/raw}; mkdir -p $RAW/ping $RAW/tcp $RAW/trace
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
mapfile -t N < $D/nodes.txt
for l in "${N[@]}"; do read n lan pub host <<<"$l"
  scp -q -i ~/.ssh/dassl_rsa $D/scripts/tcprtt.py JiyuHu23@$host:/tmp/tcprtt.py
  $SSH JiyuHu23@$host 'pkill -f "^python3 /tmp/tcprtt.py serve"; setsid nohup python3 /tmp/tcprtt.py serve 45678 </dev/null >/dev/null 2>&1 &'
done
sleep 1
for s in "${N[@]}"; do read sn slan spub shost <<<"$s"
  for d in "${N[@]}"; do read dn dlan dpub dhost <<<"$d"; [ $sn = $dn ] && continue
    for path in lan pub; do ip=$dlan; [ $path = pub ] && ip=$dpub
      echo "$sn -> $dn ($path $ip)"
      $SSH JiyuHu23@$shost "sudo ping -q -c 1000 -i 0.005 -W 1 $ip" > $RAW/ping/${sn}_${dn}_$path.txt 2>&1
      $SSH JiyuHu23@$shost "timeout 30 python3 /tmp/tcprtt.py $ip 45678 2000 64" > $RAW/tcp/${sn}_${dn}_$path.txt 2>&1
    done
    $SSH JiyuHu23@$shost "tracepath -n $dpub" > $RAW/trace/${sn}_${dn}_pub.txt 2>&1
  done
done
for l in "${N[@]}"; do read n lan pub host <<<"$l"; $SSH JiyuHu23@$host 'pkill -f "^python3 /tmp/tcprtt.py serve"'; done
