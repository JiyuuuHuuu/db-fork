# Network and disk probe, 2026-11-05: 16 xl170 nodes on one LAN

Run from the off-cluster coordinator `pc20.cloudlab.umass.edu` (ssh to the public addresses, 51 ms away) for
experiment 13 (`../exp13-pgcopy-plan-2026-11-05.md`). Cluster in `../cloudlab.md`. `nodes.txt`: name, LAN IP, public IP,
host; n0-n15 = CloudLab node0-node15. No netem on any interface (checked: default mq/fq_codel only).

Scripts (run in this order, ~15 min in all; raw output in `raw/`):
- `scripts/probe.sh raw/pairs`: all 240 ordered pairs over the LAN (`ens1f1np1`, `mlx5_3`), 8 disjoint pairs at a
  time: `ping` 500 x 5 ms, TCP ping-pong 2000 x 64 B (`tcprtt.py`, Python, 2 ms between probes), `ib_write_lat`
  (RoCE v2, GID 3, 2 B). `scripts/summarize.py raw/pairs` -> `summary.txt`.
- `scripts/iperf.sh raw/iperf`: iperf3 10 s on 6 pairs (1 and 4 streams), then the pgcopy seed pattern: n0 sends to
  all 15 other nodes at once (one stream each, `-R`), and the reverse.
- `scripts/disk.sh raw/disk`: fio on `/mydata` of n0 and n1 (direct I/O, 20 s each).

## Results

| Measure | Value |
|---|---|
| ping RTT, per-pair mean | 71-147 us, median 111 us; no loss on any pair |
| TCP 64 B ping-pong (Python) | median 238 us, p99 252 us (max 564); includes Python and wake-up cost, so compare only within this probe |
| RDMA write latency (t_typical) | 1.64-2.42 us, two clusters: ~1.65-1.75 us and ~2.35 us |
| iperf3 pair, 1 stream / 4 streams | 12.9-13.7 / 23.2-23.4 Gbit/s (all 6 pairs) |
| n0 -> 15 nodes at once | 23.9 Gbit/s total (n0's NIC is the limit), 0.79-1.94 Gbit/s per receiver, uneven |
| 15 nodes -> n0 at once | 23.5 Gbit/s total, 1.29-2.0 per sender |
| `/mydata` seq write 1 MB (1 or 8 jobs) | 348-350 MB/s |
| `/mydata` seq read 1 MB | 360-373 MB/s |
| `/mydata` random read 8 KB QD32 | 46-47k IOPS (~365 MB/s) |

Switches (inferred from the RDMA latency gap at 2.05 us, as on 2 Nov): {n0 n2 n5 n6 n7 n9 n11 n12 n13} and
{n1 n3 n4 n8 n10 n14 n15}. Crossing them adds ~0.6 us of RDMA latency, which is negligible for TCP. n13's ping is
~145 us from every node, including its own group (its RDMA latency is normal).

## What it means for experiment 13 (pgcopy)

- The disk, not the network, bounds copies: 350 MB/s is ~2.8 Gbit/s of write, against 13 Gbit/s for one TCP
  stream. A 0.3 GB copy with `--no-sync` lands in the page cache (dirty limit ~12 GB of 62 GB), so one fork is
  network/CPU bound (~0.2 s on the wire). A setup phase writing ~24 GB per host is disk bound: >= ~70 s per host.
- n0 serves every copy: at most ~3 GB/s in total, shared unevenly among the hosts copying at once.
- Ping between nodes is ~0.1 ms, about the same as on 2 Nov (0.07-0.12 ms on the same LAN).
