# CloudLab specs

```
ssh JiyuHu23@c220g2-010806.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010812.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010815.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010831.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010818.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010802.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010805.wisc.cloudlab.us
ssh JiyuHu23@c220g2-010810.wisc.cloudlab.us
```

The sshkey is `~/.ssh/dassl_rsa `.
## Machine specs (probed 2026-10-15)

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

## Experiment 3 layout (self-hosted Neon, from 19 Oct 2026)

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
