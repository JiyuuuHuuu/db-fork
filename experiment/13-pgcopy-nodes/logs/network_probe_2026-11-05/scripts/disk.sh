#!/bin/bash
# /mydata disk (CloudLab blockstore: LVM on the SATA SSD's sda4) on n0 and n1: fio, 20 s each, 1 GB files.
# seq write 1 MB direct (1 job and 8 jobs), seq read 1 MB direct, random read 8 KB direct QD32. Usage: disk.sh OUTDIR
set -u
D=$(cd "$(dirname "$0")/.." && pwd); OUT=$1; mkdir -p $OUT
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
declare -A HOST; while read n lan pub host; do HOST[$n]=$host; done < $D/nodes.txt
F="--directory=/mydata/fio --size=1G --runtime=20 --time_based --direct=1 --ioengine=libaio --group_reporting --output-format=json"
for n in n0 n1; do
  $SSH JiyuHu23@${HOST[$n]} "mkdir -p /mydata/fio && fio --name=sw1 --rw=write --bs=1M --iodepth=16 --numjobs=1 $F" > $OUT/${n}_seqwrite_1job.json
  $SSH JiyuHu23@${HOST[$n]} "fio --name=sw8 --rw=write --bs=1M --iodepth=16 --numjobs=8 $F" > $OUT/${n}_seqwrite_8jobs.json
  $SSH JiyuHu23@${HOST[$n]} "fio --name=sr --rw=read --bs=1M --iodepth=16 --numjobs=1 $F" > $OUT/${n}_seqread.json
  $SSH JiyuHu23@${HOST[$n]} "fio --name=rr --rw=randread --bs=8k --iodepth=32 --numjobs=1 $F && rm -rf /mydata/fio" > $OUT/${n}_randread8k.json
  python3 - $OUT $n <<'PY'
import json,sys
o,n=sys.argv[1:]
for t,k in (("seqwrite_1job","write"),("seqwrite_8jobs","write"),("seqread","read"),("randread8k","read")):
    j=json.load(open(f"{o}/{n}_{t}.json"))["jobs"][0][k]
    print(f"{n} {t}: {j['bw']/1024:.0f} MB/s, {j['iops']:.0f} IOPS")
PY
done
