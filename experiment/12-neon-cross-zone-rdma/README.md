# Experiment 12: Neon across emulated availability zones, over TCP and over RDMA

Experiment 5's Neon workloads (storage in one zone, computes and client in the other) rerun on the 16-node xl170
cluster at CloudLab Utah, where the two zones are 1 ms apart (emulated) instead of experiment 5's 39 ms WAN. Two
arms:

- **tcp**: Neon as in experiments 5 and 8, every connection on TCP; cross-zone traffic crosses the emulated delay.
- **rdma**: every Neon service-to-service connection that experiment 10 moved to Unix sockets moves to an RDMA byte
  stream (RoCE v2, ConnectX-4 Lx), the wire protocols unchanged. Host netem cannot delay RoCE (probe of 2 Nov), so
  this arm runs at the native cross-zone latency; a TCP run without netem (`tcp/lfc_nonetem`) separates the gain of
  the transport from the removed delay.

Scope (user, 3 Nov 2026): one cache variant, experiment 8's: W = 3, LFC 36 MB prewarmed with 3,674 heap pages and
then the primary key, so the LFC holds 28% of the stock heap (experiment 8: 28.6% of point reads served by the
compute's caches at low load). Both arms use it; it is now the default of `scripts/setup/topo.py`
(`WORKLOAD_DEFAULTS`). No "nolfc" and no sharded variants. Each arm runs the four workloads (low contention, scaling
with data operations + ceiling check, branch creation, cold start) on the unsharded tenant.

Superseded, not analysed (`data/tcp/superseded/`): the first tcp run at W = 4 with 3,400 prewarmed pages (`lfc_w4`:
46 runs, all passed, but the LFC held about 20% of the heap; measured point-read hit share 20.7%), the W = 4 no-LFC
sweep stopped after 28 of 32 runs (`nolfc_stopped`), the W = 4 netem-off control stopped at the start
(`lfc_nonetem_w4_stopped`) and the W = 4 seed record (`seed_w4`).

Status (3 Nov 2026, 23:00 MST): tcp/lfc 46 runs, tcp/lfc_nonetem 11, rdma/lfc 46, and the 2-pageserver /
2-safekeeper-set round: {tcp,rdma}/{read2ps,write2sk}, 16 points each, all passed; figures, report, REPRODUCE
updated. Results: at 1 ms the cross-zone penalty is about one round trip per operation and RDMA removes it (matches TCP
without the delay within 5%); the single-node benchmark client capped the first round's ceilings; with clients on 4-5
nodes, 2 pageservers double the read ceiling (112k point reads/s) and 2 safekeeper sets double inserts (64-68k/s),
equal for TCP and RDMA.

### 2 pageservers / 2 safekeeper sets (3 Nov 2026)

User's scope: lift the storage limit per workload: reads with the tenant split over 2 pageservers, writes with 2
safekeeper sets, both arms, no rerun of the single-pageserver round. Same 8/8 zones; roles per phase:
`scripts/setup/topologies/cloudlab_xl170{,_rdma}_{read2ps,write2sk}.toml`. New code (all outside the shared runner):

- `microbench/fast_client/` (opt-in with `BENCH_FAST_CLIENT=1 PYTHONPATH=.../microbench/fast_client`, tests in
  `tests/test_fast_client.py`): runtime patches of the runner's client-side costs (Faker values from a per-process
  pool, comment-stripping fast path in the SQL classifier, set-based modified-key tracking); and, with
  `BENCH_SYNC_ADDR`/`BENCH_SYNC_KEY`, a cross-node start (every client process waits for one GO) and no tenant-wide
  purge in `neon_local.setup_run`/`teardown_run` (the first trial's runners deleted each other's branches).
- `scripts/workloads/multi_client/` (`multi_scaling.py deploy|run|remerge`, `node_run.sh`; tests in
  `tests/test_multi_client.py`): scaling_data with each point's branches split over K client nodes, the environment
  and runner arguments of `scaling_data.sh`, one merged summary per point (same file names, so `plot.py` reads it).
- `scripts/setup`: `safekeeper_groups` in the topology (`topo.py`), `--safekeepers` groups separated by `;` and a
  per-timeline choice by hash (`neon.py`, `neon_node_agent.py`); `neon_ctl.py shard-split` now compacts every shard
  with forced image layers and moves the branch point above them (`compact-shards`); `neon.py wipe` removes the NFS
  export of the first pageserver's remote storage (a stale export of a wiped phase broke the next `exportfs`).
- Neon fork, both builds: `pgxn/neon/communicator.c` `prefetch_register_bufferv` routed every block of a vectored read
  to the shard of the first block (the pageserver answers "routed to wrong shard" and drops the connection; every
  sharded prewarm failed): fixed to `get_shard_number(&slot->buftag)`. tcp arm: `code/neon_fork_tcp_shardfix.patch`,
  `neon.so` rebuilt on a0 with the system clang 14 (`pg_config` asks for clang-17, absent here) by the pgxs make in
  `build/pgxn-v17`, installed by hand (its `pgxs` install script path is dassl-serv-01's); experiment 11's `neon.so`
  is kept as `/mnt/ssd/neon-src-neon.so.exp11`; hashes in `logs/neon.so_shardfix.sha256`. rdma arm: in
  `code/neon_fork_rdma.patch`.

Netem stays applied during the rdma arm: it delays only kernel TCP, so the benchmark client's own cross-zone paths
(client -> storage controller and pageserver HTTP) are the same in both arms, while RoCE traffic between Neon services
crosses at native latency.

Before the rdma arm: about 7,600 stale Postgres DSM segments (3.8 GB of `/dev/shm`) left on each of b1-b7 by the tcp
arm's killed computes were deleted (no compute running), so both arms start with the same free memory.

Report variants (user, 3 Nov 2026): the report and figures compare four systems: Doltgres on one machine with 10
cores and Neon local with 10 cores (both from experiment 11's tables), and this experiment's Neon over TCP with the 1 ms
cross-zone delay and Neon over RDMA. Experiment 8 and the netem-off TCP control are not plotted; the control is quoted
in the report's validity section.

## Question

How large is Neon's cross-zone penalty at a moderate 1 ms round trip, and how much of it is left when the
service-to-service transport is RDMA?

## Cluster and placement

16 xl170 nodes (10 cores / 20 threads, 64 GB, ConnectX-4 Lx: 25 GbE LAN, 10 GbE public), see `.agent/cloudlab.md`
and `.agent/network-probe-2026-11-02-xl170/README.md`. Zone a = a0-a7 (LAN 10.10.1.x), zone b = b0-b7 (10.10.2.x); the
LANs do not route to each other, so cross-zone traffic uses the public network, where `az_netem.sh apply 420` adds
0.42 ms each way (1.01 ms ping).

| Node | Role |
|---|---|
| a0 (hp155) | coordinator; storage controller + its Postgres, compute-hook stub, storage broker, seeding compute. Neon services pinned to cores 0-5,10-15; the other 4 cores build and test the rdma arm (`local_a0_rdma.toml`) |
| a1-a3 | safekeepers 1-3 |
| a4-a7 | pageservers 1-4 (2-4 registered paused; NFS share of a4's remote storage for shard splits) |
| b0 (hp140) | benchmark client (20 client processes at most). Physically on the a-side switch |
| b1-b7 | compute agents; computes on a 16 GB tmpfs, scaling runs' data directories on /mnt/ssd |

Topologies: `scripts/setup/topologies/cloudlab_xl170.toml` (tcp), `cloudlab_xl170_rdma.toml` (rdma: same nodes and
roles, node data under `/mnt/ssd/rdma`, state_dir `/mnt/ssd/bench-xl170-rdma`), `cloudlab_xl170_mini.toml` (5-node
smoke of the setup paths), `local_a0_rdma.toml` (rdma test bed on a0).

## Data on /mnt/ssd

The repo on hp155 lives on the 63 GB root filesystem, so raw data is outside it: `data` is a symlink to
`/mnt/ssd/exp12` (on a0; the client writes the same path on b0 and `scripts/pull.sh` copies it back).

```
/mnt/ssd/exp12/<arm>/<variant>/neon/<workload>/   arm: tcp | rdma; variant: lfc, lfc_nonetem (tcp control),
                                                  read2ps, write2sk (2 PS / 2 SK sets; + <variant>_archive)
               <arm>/seed/, archive/, netem/, superseded/ (smoke runs, the mini-topology ladder)
/mnt/ssd/bench-xl170/                             state_dir of the tcp arm (binary snapshot, env, seed record)
/mnt/ssd/neon-src/                                the fork as used by experiments 10/11 (tcp arm binaries)
/mnt/ssd/neon-rdma/                               the fork, branch rdma (rdma arm)
```

## Code

- tcp arm: the fork at `/mnt/ssd/neon-src` (branch local-uds, 4187e2da7 + experiment 11's patches); its binaries are
  byte-identical to experiment 11's (`code/bin_sha256.txt` matches `experiment/11-*/code/bin_sha256.txt`).
- rdma arm: branch `rdma` of the fork in `/mnt/ssd/neon-rdma`: commit 6de97fbc3 = the local-uds working tree, then
  the rdma changes, frozen as `code/neon_fork_rdma.patch` (`git diff 6de97fbc3` without `experiments/`, new files
  included; `git apply --check` on 6de97fbc3 passes). Per-node RDMA environment (links, MTU 1024, memlock of the
  services, `rdma res`) at the start of the rdma arm: `code/rdma_env/<node>.txt`.

Fix found on the cluster (not visible on the single-host test bed): the walproposer treats READABLE on an idle
safekeeper connection as EOF, and the RDMA fd also fires for send completions and returned credits. The first rdma
seed's walproposer therefore dropped healthy connections mid-election and exited (`WAL acceptor ... rejects our
connection request with term 17`), so a compute stopped right after a CHECKPOINT lost the record. Fixed in
`walprop_pg_wait_event_set_rdma`: once connected, READABLE is reported only for received data, EOF or an error.

### The RDMA transport (fork, branch rdma)

| Piece | What it is |
|---|---|
| `pgxn/neon/rdma_stream.{c,h}` | Reliable byte stream over an RC queue pair: rdma_cm connection setup (device and GID from the route to the IP), SEND_WITH_IMM messages into pre-posted slots (16 KiB, 32 data + 24 control per connection), credits returned in the immediate, inline sends for small messages, half close by a FIN control message. One pollable fd per connection (an epoll fd over the rdma_cm channel and the completion channel); waiting protocol: on EAGAIN call `rs_arm`, sleep on the fd only if it returns 0. Optional busy poll before sleeping (`NEON_RDMA_SPIN_US`) |
| `libs/rdma_stream` | The same C file built with `cc`, wrapped as tokio `AsyncRead`/`AsyncWrite` (`AsyncFd` on the connection's fd) and listeners |
| `libs/utils/src/listen_addr.rs` | `rdma:ip:port` listen addresses: `ListenAddr`, `StdListener`, `TokioListener` and `AnyStream` gain `Rdma` variants, so every listener that experiment 10 made Unix-socket capable (page service, safekeeper WAL service, HTTP servers, broker) also binds RDMA |
| `libs/postgres_connection` | `rdma:` host prefix, `connect_no_tls_maybe_rdma` (tokio-postgres `connect_raw` over the stream): pageserver WAL pull, safekeeper peer recovery |
| `pgxn/neon/pqrdma.{c,h}` | A minimal libpq v3 frontend (startup, cleartext auth, simple query into CopyBoth/CopyOut, CopyData) over the stream, with libpq's return conventions |
| `pgxn/neon/libpagestore.c` | `rdma://` pageserver connstrings use pqrdma (GetPage) |
| `pgxn/neon/walproposer_pg.c`, `walproposer.c`, `neon_walreader.c`, `libpqwalproposer.h` | `rdma:<ip>:<port>` safekeepers: a `WP_CONN_RDMA` connection kind (WAL push, sync-safekeepers, donor reads) |
| `compute_tools` | basebackup and the safekeeper sync check over RDMA for `rdma://` / `rdma:` addresses |
| `pageserver/client`, `pagebench` | `rdma://` connstrings (GetPage microbenchmark) |

Stays on TCP (host-local or the benchmark client's): storage controller to its Postgres and to the hook stub, the
neon extension to compute_ctl, the client to computes and to the HTTP APIs.

### Shared code changed in this repo

- `scripts/setup/topo.py`, `neon.py`, `neon_node_agent.py`: transport `rdma` (listeners `rdma:0.0.0.0:<port>`, peers
  `rdma:<ip>:<port>`, `rdma://` pageserver URLs for computes; same-host roles reached on the LAN address);
  `LimitMEMLOCK=infinity` on every unit (`nodes.py`); `libpq5` on every node.
- `scripts/setup/neon_ctl.py`: `smoke` passes the shard map to its compute (after a shard split it looped forever).
- New topologies listed above.

## Layout

```
README.md, plan.md, REPRODUCE.md, report.md
figures/   low_load, scaling, branch_create, cold_start (.png + .csv)
code/      base_commit.txt, bin_sha256.txt, neon_fork_commit.txt, neon_fork.patch (tcp arm, from neon.py install)
scripts/   launch.sh (detached runs on b0), chain_tcp.sh (tcp run, then the netem-off control), nonetem.sh,
           rdma_precheck.sh (rdma driver smoke + one N=1024 point, into rdma/superseded/precheck),
           chain_phases.sh (the 2 PS / 2 SK phases after the rdma write phase: archive, wipe, tcp read, tcp write),
           pull.sh (collect from b0), rdma_pingpong.c (RDMA/TCP probe of the transport), refs.py (Neon local
           and Doltgres, 10 cores, from experiment 11's figures/ tables; raw data not on hp155), plot.py (figures)
logs/      toolchain, builds, install/start/seed/smoke logs, netem status captures
data ->    /mnt/ssd/exp12
```
