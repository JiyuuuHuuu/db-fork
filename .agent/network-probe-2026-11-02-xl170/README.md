# Hardware, RDMA and netem probe, 2026-11-02: 16 xl170 nodes on two subnets

Plan: `../xl170-session-2026-11-02.md` (goals 2-5 done). Run from hp155 (= a0).

Nodes (`nodes.txt`: name, LAN IP, public IP, host): a0-a7 on VLAN 10.10.1.0/24, b0-b7 on VLAN 10.10.2.0/24, all at
CloudLab Utah, experiment `branchbench`, project rasl-PG0. Public IPs all in 128.110.216.0/21.

## Hardware (`scripts/hwprobe.sh`, output in `raw/hw/<node>.txt`)

All 16 nodes are identical:

| | |
|---|---|
| CPU | 1x Xeon E5-2640 v4 @ 2.40 GHz (turbo 3.4), 10 cores / 20 threads, 1 NUMA node, 25 MB L3 |
| RAM | 64 GB (4x 16 GB DDR4, running at 2133 MT/s), 62 GiB usable, 8 GB swap |
| Disk | one 447 GB SATA SSD (HPE `VK000480GWCNQ`) as `sda`. Root `/dev/sda3` is only **63 GB**; ~380 GB of `sda` is unpartitioned and needs a partition + filesystem before databases go on it |
| Control NIC | `eno49np0`, **10 GbE**, ConnectX-4 Lx (`mlx5_0`), public IP, MTU 1500 |
| Experimental NIC | `ens1f1np1`, 25 GbE, ConnectX-4 Lx (`mlx5_3`), 10.10.x IP, MTU 1500 |
| Unused ports | `eno50np1` (`mlx5_1`), `ens1f0np0` (`mlx5_2`), link down |
| OS | Ubuntu 22.04.2, kernel 5.15.0-198-generic, inbox mlx5 driver (no MLNX_OFED), rdma-core 39.0 |
| Network defaults | cubic, `rmem_max`/`wmem_max` 208 KB, `tcp_rmem` max 6 MB, `tcp_wmem` max 4 MB, no firewall |
| CPU freq | governor `schedutil`, turbo on; **b7 exposes no cpufreq driver** (governor unknown) |

Note the plan's "ConnectX-4 25 GbE" is ConnectX-4 **Lx**, and the control network is 10 GbE, not 1 GbE as on c220g*.

Installed on all nodes on 2026-11-02: `rdma-core ibverbs-utils infiniband-diags perftest iperf3 fio numactl`
(perftest 4.4+0.37-1, iperf3 3.9).

## Reachability

As on 29 Oct, the two experimental VLANs do not route to each other: a0 -> 10.10.2.10 is 100% loss. The control
network is one L2 segment for all 16 nodes (tracepath a0 -> b0: 1 hop, ping 0.08-0.10 ms) and carries RoCE v2.

## RDMA (`raw/rdma_smoke/`, `raw/rdma_netem/*_base.txt`)

RoCE v2 over IPv4 (GID index 3 on both ports), active MTU 1024, default perftest settings (1000 iterations for
latency, 5000 x 64 KB for bandwidth), one pair each:

| Path | write lat | send lat | read lat | write BW 64 KB |
|---|---|---|---|---|
| a0 -> a1, experimental 25 GbE (`mlx5_3`) | 1.67 us | - | - | 2,758 MB/s (22.1 Gbit/s) |
| a0 -> b0, control 10 GbE (`mlx5_0`) | 1.64 us | 1.63 us | 3.03 us | 1,103 MB/s (8.8 Gbit/s) |

perftest's write/send latency is half the round trip; read latency is a full round trip.

## Does host netem delay RDMA? No (`scripts/netem_rdma_pair.sh`, `raw/rdma_netem/`)

Applied the `az_netem.sh` mechanism (prio root qdisc, `netem delay 400us` only for the peer's public IP) on both
a0 and b0's control NIC, then repeated the tests a0 -> b0 over `mlx5_0`:

| | ping avg | write lat | send lat | read lat | write BW |
|---|---|---|---|---|---|
| no delay | 0.076 ms | 1.64 us | 1.63 us | 3.03 us | 1,103.3 MB/s |
| +400 us each way | 0.955 ms | 1.64 us | 1.63 us | 3.03 us | 1,103.4 MB/s |

The netem qdisc counted only 677 packets (the 500 pings plus perftest's TCP connection setup): RoCE frames are
built and sent by the NIC and never pass the kernel qdisc. Delaying RDMA needs a delay node or switch in the path.
The delay was removed afterwards (both NICs back to `mq`).

## Disks (`scripts/setup_ssd.sh`, `raw/ssd/`)

On all 16 nodes the free gap between root (`sda3`) and swap (`sda99`) became `sda4`: 374.9 GB ext4, label `ssd`,
mounted at `/mnt/ssd` (`defaults,noatime,nofail` in fstab by UUID), owned by JiyuHu23, 350 GB free. mkfs takes
~10 min per node (it discards the whole partition).

## All-pairs network probe (`scripts/probe.sh`, `scripts/iperf.sh`, `scripts/summarize.py`)

All 240 ordered pairs, as 15 rounds of 8 disjoint pairs in parallel (~12 min per pass). Per ordered pair: 500 pings
at 5 ms, 2000 TCP 64-byte ping-pongs (`tcprtt.py`, Python, so ~100 us of host overhead), `ib_write_lat` (2 B, over
the LAN for same-zone pairs, over the public network for cross-zone pairs), `tracepath`. Same-zone pairs are probed
over both LAN and public; cross-zone pairs over public, plus a 3-probe check that the LAN is unreachable. iperf3 10 s
on six representative pairs with 1 and 4 streams. Raw output in `raw/base/` (no delay) and `raw/az420/` (delay on);
summaries in `summary_base.txt` and `summary_az420.txt` (per-class table on top, then every pair).

| Class | ping avg | TCP p50 | TCP p99 | RDMA write lat | iperf3 1 / 4 streams |
|---|---|---|---|---|---|
| same zone, LAN | 69-115 us | 171-242 us | 186-586 us | 1.64-2.36 us | 13.2-14.8 / 23.3 Gbit/s |
| same zone, public | 69-115 us | 173-241 us | 186-552 us | - | 9.37 / 9.38 Gbit/s |
| cross zone, public, no delay | 69-121 us | 173-245 us | 192-567 us | 1.61-2.84 us | 9.38 / 9.38 Gbit/s |
| cross zone, public, +420 us each way | **1006-1024 us** | 1113-1177 us | 1161-1471 us | 1.61-2.84 us | **7.1-7.4** / 9.31-9.34 Gbit/s |
| cross zone, LAN | 100% loss | | | | |

Ranges are over per-pair values. No packet loss on any reachable path; every public path is 1 hop.

- **Switch topology.** The RDMA matrix splits the nodes into {a0-a7, b0} (hp125-hp159) and {b1-b7} (hp171-hp192):
  1.6-1.7 us inside a group, 2.8 us between groups (2.4 us for b0 to b1-b7 over the LAN). b0 is physically with the
  a nodes, and its LAN traffic to its own zone crosses the inter-switch link. Ping does not show this: its 70 vs
  110 us bimodality is host noise and does not follow the groups. Inter-switch bandwidth under many simultaneous
  cross-group flows was not measured.
- **Delay.** `scripts/az_netem.sh apply 420` (DEV `eno49np0`, zone = first letter of the node name, netem `limit
  100000`) on all 16 nodes since 2026-11-02; status in `raw/az_netem_status_420.txt`. 420 us one-way gives a 1.01 ms
  cross-zone ping (netem adds ~80 us of timer overhead on top of 2 x 420). Same-zone LAN and public paths unchanged.
  RDMA unchanged, as in the pair test above.
- **Single-stream TCP at 1 ms** falls to ~7.2 Gbit/s; 4 streams still fill the 10 GbE link. Raising `rmem_max`/
  `wmem_max`/`tcp_{r,w}mem` max to 64 MB on a2/b2 only reached 8.0 Gbit/s with ~350 retransmits in 10 s (netem
  dropped nothing), so the window is not the only limit (`raw/az420/iperf_bigbuf/`). Buffers were set back to the
  defaults; no node has tuned TCP buffers.

## Not done yet

- fio on `/mnt/ssd`.
- RDMA read/send latency and bandwidth for all pairs (only `ib_write_lat` was run on all pairs).
- Bandwidth of the inter-switch link with many pairs crossing at once.
