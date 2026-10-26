# CloudLab specs

## Current cluster (from 22 Oct 2026): two zones

Profile `branchDB` in project `rasl-PG0`. Sixteen nodes in two groups with no routers between them. Group A is 8
c220g2 nodes at CloudLab Wisconsin, group B is 8 r320 nodes at the APT cluster (Utah). Traffic within a group
uses that cluster's 10 GbE experimental LAN; traffic between groups crosses the wide area over the nodes' public
control-network addresses (path via Internet2). The experimental LANs are separate: a 10.10.1.x address cannot
reach a 10.10.2.x address, so cross-zone services must bind and connect on the public IPs.

The ssh key is `~/.ssh/dassl_rsa`; user `JiyuHu23`, passwordless sudo, outbound internet works.

| Node | Hostname | LAN IP | Public IP | Type | Notes |
|---|---|---|---|---|---|
| a0 | `c220g2-011312.wisc.cloudlab.us` | 10.10.1.10 | 128.105.145.147 | c220g2 | |
| a1 | `c220g2-011127.wisc.cloudlab.us` | 10.10.1.11 | 128.105.145.130 | c220g2 | LAN NIC is `enp6s0f0` |
| a2 | `c220g2-030631.wisc.cloudlab.us` | 10.10.1.12 | 128.105.145.168 | c220g2 | fastest WAN path to Utah |
| a3 | `c220g2-011014.wisc.cloudlab.us` | 10.10.1.13 | 128.105.145.88 | c220g2 | |
| a4 | `c220g2-011023.wisc.cloudlab.us` | 10.10.1.14 | 128.105.145.97 | c220g2 | |
| a5 | `c220g2-010822.wisc.cloudlab.us` | 10.10.1.15 | 128.105.145.64 | c220g2 | |
| a6 | `c220g2-011302.wisc.cloudlab.us` | 10.10.1.16 | 128.105.145.137 | c220g2 | 128 GB RAM (8 DIMMs), not 160 GB |
| a7 | `c220g2-011131.wisc.cloudlab.us` | 10.10.1.17 | 128.105.145.134 | c220g2 | no HDDs: only the SSD, as `sda`, holds root; LAN NIC `enp6s0f0` |
| b0 | `apt107.apt.emulab.net` | 10.10.2.10 | 128.110.96.107 | r320 | |
| b1 | `apt116.apt.emulab.net` | 10.10.2.11 | 128.110.96.116 | r320 | |
| b2 | `apt095.apt.emulab.net` | 10.10.2.12 | 128.110.96.95 | r320 | |
| b3 | `apt118.apt.emulab.net` | 10.10.2.13 | 128.110.96.118 | r320 | |
| b4 | `apt101.apt.emulab.net` | 10.10.2.14 | 128.110.96.101 | r320 | |
| b5 | `apt109.apt.emulab.net` | 10.10.2.15 | 128.110.96.109 | r320 | |
| b6 | `apt115.apt.emulab.net` | 10.10.2.16 | 128.110.96.115 | r320 | |
| b7 | `apt112.apt.emulab.net` | 10.10.2.17 | 128.110.96.112 | r320 | |

```
ssh -i ~/.ssh/dassl_rsa JiyuHu23@c220g2-011312.wisc.cloudlab.us   # a0
ssh -i ~/.ssh/dassl_rsa JiyuHu23@apt107.apt.emulab.net            # b0
```

### Hardware (probed 2026-10-22 on all 16 nodes)

| | Group A: c220g2 (Wisconsin) | Group B: r320 (APT, Utah) |
|---|---|---|
| CPU | 2x Xeon E5-2660 v3 @ 2.60 GHz, 20 cores / 40 threads, 2 NUMA nodes | 1x Xeon E5-2450 @ 2.10 GHz, 8 cores / 16 threads, 1 NUMA node |
| RAM | 157 GiB usable (a6: 125 GiB) | 15 GiB usable |
| Disks | `sda`, `sdb` 1.1 TB HDD; `sdc` 447 GB Intel SSD DC S3500 (a7: SSD only, as `sda`) | `sda`-`sdd` 4x 500 GB 7200 rpm HDD (WD5003ABYX). **No SSD.** |
| Root FS | `/dev/sda3`, 63 GB (on HDD; on the SSD for a7) | `/dev/sda3`, 63 GB, on HDD |
| Experimental NIC | 10 GbE (`enp6s0f1`, or `enp6s0f0` on a1/a7), MTU 1500 | 10 GbE Mellanox ConnectX-3 (`enp8s0d1`, mlx4_en), MTU 1500 |
| Control NIC | 1 GbE `enp1s0f0` | 1 GbE `eno1` |

Both groups: Ubuntu 22.04.2 LTS, kernel 5.15.0-187-generic, python3 3.10, no firewall (iptables INPUT ACCEPT, ufw
inactive), default TCP buffers (`tcp_rmem` max 6 MB, `tcp_wmem` max 4 MB) and cubic congestion control. `iperf3`
3.9 was installed on every node on 2026-10-22. Nothing else is set up yet: no `/mnt/ssd`, no repo checkout, no
database servers.

### Network (measured 2026-10-22)

Cross-zone latency is about 270 times the within-zone latency, and steady. Ping, 20 probes per pair over every ordered
pair (56 within each zone, 128 across), no packet loss anywhere:

| Path | Mean RTT | Range of per-pair means |
|---|---|---|
| A to A, experimental LAN | 0.15 ms | 0.11-0.17 ms |
| A to A, public network | 0.17 ms | 0.12-0.19 ms |
| B to B, experimental LAN | 0.22 ms | 0.17-0.25 ms |
| B to B, public network | 0.42 ms | 0.37-0.45 ms |
| A to B, public network | 38.9 ms | 38.8-38.9 ms (jitter about 0.02 ms) |
| A to B, LAN addresses | unreachable | |

Cross-zone bandwidth is far lower than within a zone, asymmetric, and varies by node. iperf3 TCP, 10 s runs:

| Path | Throughput |
|---|---|
| Within A or B, LAN, 1 stream | 9.4 Gbit/s |
| Within A or B, LAN, 4 pairs at once | 9.4 Gbit/s per pair (no switch bottleneck) |
| Within A or B, public 1 GbE | 0.94 Gbit/s |
| Wisconsin to Utah, 1 stream | 56-155 Mbit/s (a2: 430) |
| Wisconsin to Utah, 8 streams | 250-560 Mbit/s |
| Utah to Wisconsin, 1 stream | 150-620 Mbit/s, mostly 350-600 |
| Utah to Wisconsin, 8 streams | 360-680 Mbit/s |
| All 8 pairs at once, Wisconsin to Utah, 4 streams each | 2.1 Gbit/s total, 150-210 Mbit/s per node (a2: 890) |
| All 8 pairs at once, Utah to Wisconsin, 4 streams each | 5.1 Gbit/s total, 540-800 Mbit/s per node |

Each node's cross-zone traffic is capped at 1 Gbit/s by its control NIC even on a clean path. Single streams fall
well short of that because of loss on the WAN, not the TCP buffers. The 1 GbE within-zone public network is also 10
times slower than the LAN, so keep within-zone traffic on 10.10.x addresses. Scripts and raw iperf3/ping output are
in `.agent/network-probe-2026-10-22/`.

### Experiment 4 layout (self-hosted Neon across the two zones, from 22 Oct 2026)

Set up by `experiment/4-neon-cross-zone-low-load/scripts/setup_neon_cluster.sh` (roles in its `scripts/cluster.env`).
Storage layer in Wisconsin, client and computes in Utah; compute-to-storage traffic uses the public addresses.

| Node | Role |
|---|---|
| a0 | storage controller + its Postgres, storage broker, compute-hook stub, safekeeper 1 |
| a1, a3 | safekeepers 2 and 3 |
| a2 | pageserver (`/mnt/ssd/ps`, "remote storage" `/mnt/ssd/ps-remote`) |
| a4 | seeding compute only (agent with `--computes`, LAN to the storage layer, statement log off) |
| a5 | one-zone setup: benchmark client (`.env` with `DBFORK_SETUP=onezone`) |
| a6 | one-zone setup: compute node, computes on its SSD (`/mnt/ssd/computes`), storage reached over public IPs |
| a7 | spare (room for another pageserver) |
| b0 | benchmark client (`~/db-fork`, `.env` from `neon_ctl.py env-init --from-seed-json`) |
| b1 | compute node: computes on a 6 GB tmpfs at `/mnt/tmpfs/computes` (no SSD in Utah) |
| b2-b7 | spare (room for more compute nodes) |

On a0-a4 `/dev/sdc` is XFS at `/mnt/ssd` and holds everything (binaries in `/mnt/ssd/neon-bin`, units' wrappers
in `/mnt/ssd/neon-svc`); on b1 the binaries are in `~/neon/bin`. Every agent node runs `neon-agent` on port 9000
and keeps `~/neon-archive` (compute logs, start metrics, 1 s counter samples). `RemoveIPC=no` everywhere.
Agent replies must not wait on Nagle (the agent sets `disable_nagle_algorithm`): with kept-alive connections
each reply otherwise waited 40 ms for a delayed ACK.

### Experiment 5 layout (Neon across the two zones, scaling, from 22 Oct 2026)

Set up by `experiment/5-neon-cross-zone-scaling/scripts/setup_neon_cluster.sh` and `setup_remote_storage_nfs.sh`
(roles in its `scripts/cluster.env`). Experiment 4's storage layer and placement, plus a second pageserver and
seven compute nodes.

| Node | Role |
|---|---|
| a0 | storage controller + its Postgres, broker, compute-hook stub (threaded since 23 Oct), safekeeper 1 |
| a1, a3 | safekeepers 2 and 3 |
| a2 | pageserver node 1; exports its remote storage `/mnt/ssd/ps-remote` over NFS (LAN) |
| a5 | pageserver node 2 (`/mnt/ssd/ps`; `/mnt/ssd/ps-remote` is a2's, NFS); registered with scheduling Pause, Active since the shard split |
| a4 | seeding compute only |
| a6, a7 | unused (a6 runs Xata from experiment 9, see below) |
| b0 | benchmark client (`RemoveIPC=no`; `py-spy` in `~/.local/bin`) |
| b1-b7 | compute nodes: LFC files on a 12 GB tmpfs `/mnt/tmpfs/computes`, data directories of many-compute runs in `~/neon-computes` |

State after the experiment (23 Oct 2026): tenant `490d45eece1d4d08aa89462e927b90a7` (W=4 seed) split into 2 shards
(shard 0 on a2, shard 1 on a5), scheduling Pause; only its seed timeline left; bench role has
`statement_timeout = 600s`. The W=60 tenant of experiment 4 is still attached to a2 (scheduling Pause). Services and
agents left running. Agents' experiment-4 archives moved to `~/neon-archive-exp4` on every node.

State after experiment 8 (25 Oct 2026, same layout): new tenant `625258ca0fcb429fabea4ca7adc237a5` (W=3 seed, bench
role `statement_timeout = 600s` set by the seed) split into 2 shards (shard 0 on a2, shard 1 on a5), scheduling
Pause; only its seed timeline left, no computes running. Experiment 5's tenant `490d45ee...` is still there with only
its seed timeline. Pageserver node 2 is Active (it was Paused while seeding, and a new tenant would now be placed on
it unless pinned). Agents' experiment-5 archives are in `~/neon-archive-exp5`, experiment 8's in `~/neon-archive`.

## Previous cluster (experiments 1-3, released by 22 Oct 2026)

Profile `branchdb` in project `rasl-pg0`, eight c220g2 nodes at Wisconsin on one LAN. These hostnames no longer
answer; the notes are kept because experiments 1-3 refer to them.

### Machine specs (probed 2026-10-15)

Profile `branchdb` in project `rasl-pg0`, Wisconsin cluster, type **c220g2**. node0 and node1 probed; the
others are the same type.

| | |
|---|---|
| CPU | 2x Intel Xeon E5-2660 v3 @ 2.60 GHz, 10 cores / 20 threads per socket (40 logical), 2 NUMA nodes |
| RAM | 157 GiB |
| Disks | `sda`, `sdb`: 1.1 TB HDD (HUC101212CSS600, rotational); `sdc`: 447 GB Intel SSD DC S3500 (SSDSC2BB480G4) |
| Root FS | `/dev/sda3`, 63 GB, on the HDD; `/users/JiyuHu23` is on it |
| OS | Ubuntu 22.04.2 LTS, kernel 5.15.0-187-generic, x86_64 |
| Access | user `JiyuHu23`, key `~/.ssh/dassl_rsa`, passwordless sudo; outbound internet works |
| Pre-installed | python3 3.10, git 2.34, rsync, curl. No go, dolt, doltgres, uv, protoc, numactl, xfsprogs |

Benchmark layout (set up by `experiment/1-dolt-branch-scaling/scripts/setup_cloudlab_dolt.sh`):

| Node | Hostname | Role |
|---|---|---|
| node0 | `c220g2-010806.wisc.cloudlab.us` | `dolt_mysql`: Dolt 2.4.2 `sql-server` on 127.0.0.1:3306, data `/mnt/ssd/dolt/databases` |
| node1 | `c220g2-010812.wisc.cloudlab.us` | `dolt`: Doltgres 1.4.0 on 127.0.0.1:5432, data `/mnt/ssd/doltgres/databases` |

On each node `/dev/sdc` is formatted XFS and mounted at `/mnt/ssd` (root is on a spinning disk). The server is
pinned to NUMA node 0 (`numactl --cpunodebind=0 --membind=0`, systemd unit), the benchmark driver (this repo under
`~/db-fork`, uv-managed venv) runs on NUMA node 1 on the same node. Dolt auto-GC is disabled in the server config;
GC runs only between benchmark runs.

### Experiment 3 layout (self-hosted Neon, from 19 Oct 2026)

Set up by `experiment/3-neon-branch-scaling/scripts/setup_neon_cluster.sh` (roles in its `scripts/cluster.env`).
Dolt (node0) and Doltgres (node1) are stopped (`systemctl disable --now`), their data is still on `/mnt/ssd`.

| Node | Hostname | Role |
|---|---|---|
| node0 | `c220g2-010806` | benchmark client (`~/db-fork`, `.env` with `NEON_LOCAL_*`) |
| node1 | `c220g2-010812` | Neon compute node (computes in `/mnt/ssd/computes`) |
| node2 | `c220g2-010815` | pageserver (`/mnt/ssd/ps`, remote storage `/mydata/ps-remote`) |
| node3 | `c220g2-010831` | safekeeper 1, storage_broker, storage_controller + its Postgres, compute-hook stub |
| node4 | `c220g2-010818` | safekeeper 2 |
| node5 | `c220g2-010802` | safekeeper 3 |
| node6 | `c220g2-010805` | Neon compute node |
| node7 | `c220g2-010810` | Neon compute node |

All Neon services are systemd units named `neon-*`; every node but node0 runs `neon-agent` on port 9000.
`/dev/sdc` is XFS at `/mnt/ssd` on every node. `RemoveIPC=no` is set in `/etc/systemd/logind.conf` everywhere
(otherwise logind deletes Postgres shared memory when the last ssh session ends). Nodes cannot ssh to each other.
LAN round trip 0.12-0.15 ms on `enp6s0f1` (10 GbE).
