# Experiment 12: Neon cross-zone on the xl170 cluster, TCP arm then RDMA arm

## Context

Experiment 5 measured Neon with its storage layer one WAN hop (38.9 ms RTT) away from the computes and found
every operation 8-22x slower than in one zone. The new 16-node xl170 cluster at CloudLab Utah (`.agent/cloudlab.md`)
has two LAN subnets that do not route to each other and an emulated cross-zone delay of 1.01 ms RTT on the public
10 GbE network (`az_netem.sh apply 420`, active on all nodes). The question is twofold: (1) how big is Neon's
cross-zone penalty at a moderate 1 ms, and (2) how much of it disappears when every Neon service-to-service
connection runs over RDMA (RoCE v2 on the ConnectX-4 Lx NICs) instead of TCP, the way experiment 10 moved every
connection to Unix sockets on one host.

Decisions already taken (user, 2 Nov):
- Neon fork source: copied by the user to `/mnt/ssd/neon-src` (branch `local-uds`, commit 4187e2da7, exp-10 UDS
  patch applied as working-tree changes, `vendor/tokio-epoll-uring` present). The copy also holds `target/release`
  and `pg_install/` whose binaries **hash-match experiment 11's `code/bin_sha256.txt` exactly**.
- Delay: keep host netem for the TCP arm; the RDMA arm runs at native RoCE latency (host netem cannot delay RoCE,
  measured 2 Nov: 1.64 us with or without netem). The report must say so. Recommended cheap control: one short TCP
  run with netem removed (same-zone-speed TCP) so the RDMA gain can be separated from the removed AZ delay.
- Scope: all six Neon service-to-service paths of exp 10's table move to RDMA. The benchmark client's libpq
  connections to computes and its HTTP calls to storcon/agents stay TCP.
- Mechanism: one RDMA byte-stream transport (RC queue pairs) behind a new `rdma:` listener form next to exp 10's
  `unix:`; wire protocols unchanged. No eRPC, no one-sided reads in this round.

Facts verified on the cluster (this session): `/dev/infiniband/uverbs*` and `rdma_cm` are 0666 (non-root OK);
rdma-core 39.0, `libibverbs-dev`/`librdmacm-dev` 39.0-1 installable; GID index 3 = RoCE v2 IPv4 on both `mlx5_3`
(25 GbE LAN) and `mlx5_0` (10 GbE public); cross-zone RoCE works over the public port (8.8 Gbit/s); `ulimit -l` is
8 GB for shells but systemd units get 8 MiB unless `LimitMEMLOCK` is set. a0 (hp155) can ssh to itself with
`~/.ssh/dassl_rsa`. No cargo/rustup/uv/protoc/cmake/clang on any node. `libpq5` is missing on all nodes and the
prebuilt `pageserver` links `libpq.so.5`. dassl-serv-01 is unreachable from here.

## Layout

```
experiment/12-neon-cross-zone-rdma/
  README.md, plan.md (this plan), REPRODUCE.md, report.md, figures/, logs/, scripts/, code/
  data -> /mnt/ssd/exp12            (symlink; raw data must stay off the 63 GB root FS; README says so)
/mnt/ssd/exp12/
  tcp/{lfc,nolfc,lfc_2ps[,lfc_4ps][,lfc_nonetem]}/   <- run_all.sh --outdir /mnt/ssd/exp12/tcp/<variant>
       (the drivers append /neon/<workload>/ themselves)
  tcp/{seed,archive,netem,superseded}/, tcp/clock_offsets.json
  rdma/...                          (same shape, phase 2)
/mnt/ssd/bench-xl170/               state_dir (binary snapshot, code/, env files, seed json, workload.lock)
/mnt/ssd/neon-src/                  the fork (build_dir)
```

Roles (a0 = this machine, coordinator and storage-zone host, named by hostname so `neon.py` treats it like every
other node: with `host = "local"` it would get no CPU agent and every run would CHECK-FAIL):

| Zone a (storage, LAN 10.10.1.x) | Zone b (compute, LAN 10.10.2.x) |
|---|---|
| a0 storcon + its Postgres + hook + broker + seed compute | b0 benchmark client (on the a-side switch physically; irrelevant for a TCP client, note in README) |
| a1-a3 safekeepers | b1-b7 compute agents, computes on a 16 GB tmpfs (as exp 5), scaling runs put data dirs on `/mnt/ssd` |
| a4-a7 pageservers (node ids 1-4; 2-4 registered Paused, NFS-share a4's remote storage; 2-shard split as exp 5, 4-shard optional) | |

Cross-zone paths (public IPs, delayed in the TCP arm): compute -> pageserver GetPage, walproposer -> safekeepers,
compute_ctl -> pageserver basebackup, client -> storcon/pageserver HTTP. Same-zone (LAN): pageserver <-> safekeeper,
<-> broker, <-> storcon; client -> computes.

## Phase 1: TCP arm

### 1.1 Files to add
- `scripts/setup/topologies/cloudlab_xl170.toml`: 16 nodes from `.agent/network-probe-2026-11-02-xl170/nodes.txt`
  (`host = hpNNN.utah.cloudlab.us`, `lan`, `public`, `zone = "a"|"b"`), every node `data_root = "/mnt/ssd"`,
  `disk_dev = "sda4"`, **no `ssd_dev`** (the SSD is the root disk and `/mnt/ssd` is already mounted), b1-b7
  `tmpfs_size = "16G"`; `[ssh] user/key`; `[neon] build_dir = "/mnt/ssd/neon-src"`, `transport = "tcp"`, roles as
  above; `[workload] max_procs = 20`; `state_dir = "/mnt/ssd/bench-xl170"`. Defaults otherwise (exp 5's).
- `scripts/setup/topologies/cloudlab_xl170_nolfc.toml`: identical plus `[neon.workload] lfc_mb = 0` (the LFC size
  reaches computes per run via `compute_opts()` in `scripts/workloads/common.sh`, no restart needed).
- `scripts/setup/topologies/cloudlab_xl170_mini.toml`: a0, a4, a5, b0, b1 (`state_dir = /mnt/ssd/bench-mini`,
  safekeepers = [a0], pageservers = [a4, a5], agents = [b1]) to exercise NFS, Pause, split and the client path at
  small scale first. All three are picked up by `tests/test_topo.py::test_examples_load`.
- `experiment/12-neon-cross-zone-rdma/scripts/`: plot/analysis scripts copied from
  `experiment/8-neon-cross-zone-w3/scripts/` (`load_runs.py`, `plot_scaling.py`, `resource_use.py`,
  `plot_branch_create.py`, `plot_cold_start.py`, `breakdown.py`, `plot_low_load.py`, `summarize.py`), re-rooted to
  `EXP/data/<arm>/<variant>/neon/...` with an `--arm tcp|rdma` option; exp 5's reference data is not on this
  machine, so its reference curves come from `experiment/5-*/figures/*.csv` or are made optional. Plus `pull.sh`
  (rsync b0 results, `pull-archive`, netem status) and a `launch` helper (transient systemd unit on b0, as exp 5).
- `scripts/setup/neon.py` `prepare()`: add `libpq5` to the apt list (pageserver needs `libpq.so.5`); in phase 2
  also `libibverbs1 librdmacm1 rdmacm-utils`. `scripts/setup/nodes.py::systemd_unit`: `LimitMEMLOCK=infinity`
  (harmless now, required in phase 2).

### 1.2 Binaries: use the copied build, record provenance
`neon.py install` only snapshots `<build_dir>/target/release/*` and `pg_install/v17`; it does not compile. The copied
binaries are byte-identical to experiment 11's (sha256 checked), so phase 1 runs on them: `code/bin_sha256.txt`
and `code/neon_fork_commit.txt` + `neon_fork.patch` (written by `install` into `state_dir/code/`) record it, README
states "same binaries as experiment 11, built on dassl-serv-01 25-26 Oct". The Rust toolchain is still installed on
a0 in parallel (needed for phase 2, and as fallback): `CARGO_HOME=/mnt/ssd/cargo RUSTUP_HOME=/mnt/ssd/rustup`,
rustup with `rust-toolchain.toml` (1.88.0), apt `build-essential libtool libreadline-dev zlib1g-dev flex bison
libseccomp-dev libssl-dev clang libclang-dev pkg-config libpq-dev cmake libcurl4-openssl-dev libicu-dev unzip
libibverbs-dev librdmacm-dev`, protoc 32.1 and uv into `~/.local/bin` (same recipe as `nodes.py deploy_repo`), then
`uv sync --group dev && uv run python build_protos.py` in `~/db-fork` and `uv run pytest tests/test_topo.py`.
Before phase 2's first build: `rm -rf target build` is NOT done (keep the reference binaries until the new ones are
snapshotted); build into the same tree, re-snapshot with a new `state_dir` (`/mnt/ssd/bench-xl170-rdma`).

### 1.3 Smoke ladder (the remote paths of scripts/setup were never run for real)
1. `topo.py check`, `pytest tests/test_topo.py`, `topo.py env --view b1` (addresses: compute -> 128.110.218.171:64000,
   safekeepers public, broker/storcon 10.10.1.10).
2. `neon.py <T> install|start|seed|deploy-client|shard-split --shards 2 --dry-run`: read units, wrappers,
   `pageserver.toml`, NFS export line, agent argv (`--disk-compute-root /mnt/ssd/neon/computes-disk`, `--lfc-mb 36`).
3. Mini topology end to end (~40 min): `install --no-fanout`, `start`, `status`, `seed --scale 1 --no-deploy`,
   `deploy-client`, `smoke`, `clock-check`, on b0 `run_all.sh --smoke --steps low_contention,cold_start`, then
   `shard-split --shards 2`, `deploy-client`, `scaling_data.sh --smoke --ns 1,4 --ops READ`, `wipe --yes`.
   Known risk points to watch: `nfs_remote_storage()` first real run (exports/mount on a5), `systemd_unit`
   `LimitNOFILE=1048576`, `register_pageservers()` JSON grep idempotence, `RemoveIPC=no` + logind restart over
   ssh-to-self, tmpfs mount, `CLIENT_IS_LOCAL=1` on b0 (address match, hostnames differ), CPU agents answering on
   public IPs from b0.
   **b0 needs `~/.ssh/dassl_rsa`** (copy it): `common.sh` runs `neon.py metrics|purge` from b0 through ssh-to-self;
   without the key purge never runs from the drivers and `metrics/` snapshots are missing (silently).
4. Full topology: `install --no-fanout` (7 x 325 MB over 10 GbE; skip the exp-5 HTTP fan-out), `start`, `status`
   (storcon node 1 Active/Active, 2-4 Active/Pause; `findmnt` nfs4 on a5-a7, tmpfs on b1-b7), `seed --no-deploy`
   (W=4: stock 400k rows, image layers > 0), `env-init`, `deploy-client`, `clock-check` (< 10 ms), `smoke`,
   `run_all.sh --smoke` (~20 min).

### 1.4 Measured runs (from b0 via the launch helper; netem status captured before and after each)
1. `tcp/lfc`: `run_all.sh` (A low_contention, B scaling_data --ceiling, C scaling_branch MIXED, D cold_start).
   Expect ~4-5 h total (exp 5 took ~8 h at 39 ms; windows are shorter at 1 ms, report `elapsed_time` per run).
2. `tcp/nolfc`: `--steps scaling_data` with the nolfc topology.
3. `neon.py purge`, `shard-split --shards 2`, `deploy-client` (refreshes `NEON_LOCAL_PS_OPTS`), `smoke`, then
   `tcp/lfc_2ps`: `--steps scaling_data` (exp 5 used `--ns 1,64,256,512,1024 --ops READ,RANGE_READ,UPDATE`).
4. Optional `tcp/lfc_4ps`: `wipe --yes`, `start`, `seed`, `shard-split --shards 4`, `deploy-client`, scaling only.
   Do it only if the 2-shard sweep still shows a server-side knee.
5. Recommended control `tcp/lfc_nonetem`: `az_netem.sh remove` on all 16, `low_contention` + `scaling_data --ns
   1,64,256 --ops READ,UPDATE` on the unsharded tenant (needs a fresh seed if done after the split; simplest is to
   run it between steps 2 and 3), then `az_netem.sh apply 420` and verify 1.01 ms again.
6. Collect: rsync `/mnt/ssd/exp12/` from b0, `neon.py pull-archive /mnt/ssd/exp12/tcp/archive`, driver logs to
   `logs/remote_b0/`, smoke output to `superseded/`, `neon.py purge`. Plot; write README, REPRODUCE (with measured
   durations), report (TCP arm section; exp 5's 39 ms and exp 3's one-zone numbers as references).

## Phase 2: RDMA arm

### 2.1 The transport: `libs/rdma_stream` in the fork (one crate, three faces)
- Thin bindings we control: `build.rs` with bindgen (already a workspace dep via `postgres_ffi`) over
  `<infiniband/verbs.h>` and `<rdma/rdma_cma.h>`, a small C shim exporting the static-inline verbs
  (`ibv_post_send/recv`, `ibv_poll_cq`, `ibv_req_notify_cq`), link `-libverbs -lrdmacm`. No external RDMA crate.
- `conn.rs` sync core (no threads): rdma_cm id + PD + CQ + completion channel (`O_NONBLOCK`) + RC QP + two
  registered slot rings. SEND/RECV with pre-posted receive slots and piggybacked credits (not WRITE-with-IMM
  rings: same credit need, more complexity, no measurable gain at this NIC). Frame = `u16 len | u16 credits |
  payload`; 16 KiB slots, 64 recv + 64 send per connection in the services (2 MiB registered), 16 in computes
  (512 KiB, env-tunable); sends <= inline limit use `IBV_SEND_INLINE` (GetPage requests ~40 B); larger messages
  segmented; explicit credit frame once >= half the slots are pending; EOF = FIN frame + `rdma_disconnect`.
  API: `connect_start/connect_poll`, `Listener::bind/try_accept`, `try_send`, `try_recv`, `arm`, `drain_events`,
  `fd()` (comp-channel fd), `ready()`, `shutdown`, `peer_addr`.
- `tokio.rs`: `TokioStream` (AsyncRead/AsyncWrite: poll CQ, then arm, then re-poll before registering the waker,
  which closes the lost-wakeup race), `TokioListener` (`AsyncFd` on the cm event channel), one lazy poller thread
  per process blocking in `ibv_get_cq_event` and waking tasks. Services afford a thread; Postgres backends do not.
- `ffi.rs` + cbindgen header `rdma_stream.h`: `neon_rdma_connect_start/poll`, `neon_rdma_fd`, `send`, `recv`,
  `arm`, `drain`, `ready`, `error`, `close`. Exported through the staticlib the fork already links into `neon.so`
  (`pgxn/neon/communicator`, the `walstore_*` FFI is the precedent). Called only from the backend's main thread.
- Connection setup: `rdma_resolve_addr` on the destination IP picks the device (10.10.x -> `mlx5_3`, 128.110.x ->
  `mlx5_0`) and GID index 3; passive side binds `0.0.0.0` (all devices). `RDMA_PS_TCP` port space is separate from
  kernel TCP, so port numbers stay those of the TCP arm. `ibv_fork_init()` in `_PG_init` when any address is
  `rdma:` (no RDMA resource lives in the postmaster anyway; backends open their own context after fork).

### 2.2 Address forms
| Where | TCP arm | RDMA arm |
|---|---|---|
| Listeners (`listen_pg_addr`, `--listen-pg`, broker `--listen-addr`, new HTTP-RDMA flags) | `0.0.0.0:64000` | `rdma:0.0.0.0:64000` |
| Safekeeper `--advertise-pg` / broker `safekeeper_connstr` | `10.10.1.11:5454` | `rdma:10.10.1.11:5454` |
| `broker_endpoint`, `control_plane_api`, storcon's pageserver/safekeeper base URLs | `http://ip:port/` | `http+rdma://ip:port/` |
| `neon.pageserver_connstring` / spec `libpq_url` | `postgres://no_user@ip:64000` | `rdma://ip:64000` |
| `neon.safekeepers` | `ip:5454,...` | `rdma:ip:5454,...` |

### 2.3 Rust integration (exp 10's hook points, all verified in the fork)
- `libs/utils/src/listen_addr.rs`: `Rdma` variants of `ListenAddr`, `StdListener`, `TokioListener`, `AnyStream`
  (`set_nodelay` no-op, `peer_addr` from rdma_cm, `AsRawFd` = comp-channel fd). `page_service.rs`,
  `safekeeper/src/wal_service.rs`, `libs/http-utils/src/server.rs`, the broker accept loop already take
  `AnyStream`/`TokioListener`; `PostgresBackend<IO>` is generic.
- `libs/postgres_connection/src/lib.rs`: `Transport {Tcp, Unix, Rdma}`; `parse_host_port` strips `rdma:`; a
  transport-aware connect using `tokio_postgres::Config::connect_raw(TokioStream, NoTls)`. Callers: pageserver
  `walreceiver_connection.rs` (WAL pull), `safekeeper/src/recovery.rs`, compute_ctl `check_safekeepers_synced_async`,
  compute_ctl `try_get_basebackup_libpq` (today the blocking `postgres` crate; switch to tokio-postgres under
  `Handle::current().block_on`, as the gRPC basebackup path already does), pagebench `LibpqClient`
  (gives the stage-2 microbenchmark for free). `libs/compute_api/src/spec.rs` accepts the `rdma` scheme.
- `storage_broker/src/lib.rs::connect`: `http+rdma` branch with `connect_with_connector_lazy` (next to the UDS one).
- `libs/uds_http`: `http+rdma://` branch (`hyper::client::conn::http1::handshake` over `TokioStream`); callers
  (`pageserver/client/src/mgmt_api.rs`, `safekeeper/client`, `controller_upcall_client.rs`,
  `storage_controller/src/{node,safekeeper,compute_hook}.rs`) only need `base_url()` to emit the scheme.
- Dual HTTP listeners so the Python client and agents keep TCP: pageserver `listen_http_rdma_addr`, safekeeper
  `--listen-http-rdma`, storcon `--listen-rdma` (second `Server` on the same service). Stays host-local and
  untouched: storcon -> hook stub, storcon -> its Postgres, neon extension -> compute_ctl (libcurl), agent ->
  compute_ctl, client -> computes. TLS unsupported on `rdma:` listeners (as on `unix:`).

### 2.4 C side (keep the libpq protocol, replace libpq's transport role)
- New `pgxn/neon/pqrdma.c/.h` (~500 lines): minimal v3 frontend over the byte stream: StartupMessage, auth
  (Trust here; cleartext password for `NEON_AUTH_TOKEN`), `Query`, CopyBothResponse, CopyData in/out, Error/Notice,
  with non-blocking `get_copy_data` (>0 data | 0 need input | -1 EOF | -2 error) and `flush` returning WouldBlock
  when credits are exhausted. Makefile: add the object, `-libverbs -lrdmacm` to `SHLIB_LINK`, include `rdma_stream.h`.
- `libpagestore.c` (`page_server_api`: `pageserver_connect` state machine, `call_PQgetCopyData`,
  `pageserver_send/flush/receive/try_receive`): `rdma://` prefix selects the new path; the comp-channel fd replaces
  `PQsocket()` in `WaitLatchOrSocket` and the per-shard `wes_read` WaitEventSet (`WL_SOCKET_READABLE` only; call
  `drain` then `ready` after each wake, exactly the `walproposer_objstore.c` protocol). `ReserveExternalFD()` x3.
- `walproposer_pg.c` / `libpqwalproposer.h`: new `WP_CONN_RDMA` kind next to `WP_CONN_OBJSTORE`; an `sk_is_rdma()`
  branch at each of the 12 `sk_is_objstore()` dispatch sites; `walprop_socket` -> rdma fd; the virtual-readiness
  loop in `walprop_pg_wait_event_set` generalised. `walproposer.c` strips `rdma:` per list entry. `neon_walreader.c`
  (donor reads) switches its 5 direct `pg_conn` uses on the kind. Rejected alternative: a socketpair bridge thread
  per backend (two copies and context switches per message, erases the gain).

### 2.5 Configuration generation (shared tree)
- `scripts/setup/topo.py`: `transport in ("uds", "tcp", "rdma")`; `listen(kind="pg"|"grpc")` -> `rdma:0.0.0.0:<port>`;
  `listen_rdma_http()` for the dual listeners; `pg_host()` -> `rdma:<addr>`; new `svc_url()` -> `http+rdma://...`
  for service-to-service endpoints; `http_url()` (client-facing) unchanged. `addr_from()` is already right.
- `scripts/setup/neon.py`: safekeeper `--listen-pg/--advertise-pg/--listen-http-rdma`, `pageserver.toml`
  (`listen_pg_addr`, `listen_http_rdma_addr`, `broker_endpoint`, `control_plane_api`), broker `--listen-addr`,
  storcon `--listen-rdma`, `register_pageservers()` posting the rdma HTTP address, agent argv
  (`--transport rdma --pageserver rdma:ip:64000 --safekeepers rdma:ip:5454,...`), apt `libibverbs1 librdmacm1
  rdmacm-utils`. `nodes.py`: `LimitMEMLOCK=infinity` on every unit (the agent's unit covers compute_ctl and all
  Postgres instances it spawns). `neon_node_agent.py`: `libpq_url()` emits `rdma://ip:port`; `--transport rdma`.
  `dblib/neon_local.py`: no change. `tests/test_topo.py`: rdma cases.

### 2.6 Stages, each with its test (first reportable milestone is stage 3)
| Stage | Work | Test |
|---|---|---|
| 0 | toolchain on a0, clean release build of the unchanged fork (`BUILD_TYPE=release CARGO_BUILD_FLAGS="--features=testing" make -j20 -s`, ~1 h), `cargo test -p utils -p postgres_connection -p uds_http` | binaries built; UDS/TCP tests green |
| 1 | `libs/rdma_stream` core + tokio + FFI, `examples/pingpong.rs` and a C pingpong against the FFI, loopback unit tests | GetPage-shaped RTT (48 B / 8.2 KB) p50 <= ~12 us same zone (`mlx5_3`), <= ~15 us cross zone (`mlx5_0`) with channel wakeups; 16 KiB streaming >= 2 GB/s LAN, >= 1 GB/s public; 64 concurrent connections clean; 4,000 contexts + QPs on one node (compute-node scale); TCP reference from exp 10's `rtt_probe.c` with a tcp mode |
| 2 | `listen_addr` Rdma variants, pageserver pg listener, pagebench `rdma://` | `pagebench get-page-latest-lsn` over `rdma://` vs TCP on the seed tenant |
| 3 | `pqrdma.c`, `libpagestore.c`, `ibv_fork_init`, agent `rdma://`; safekeepers still TCP | one compute on b1 against a4: `neon.py smoke`, point reads TCP vs RDMA; 150 computes on one node (contexts, memory) |
| 4 | safekeeper listener + walproposer/walreader kinds, pageserver -> safekeeper pull, compute_ctl sync check + basebackup | inserts/updates, compute restart (basebackup), safekeeper restart (recovery) |
| 5 | broker gRPC over `http+rdma`, dual HTTP listeners, storcon <-> pageserver over RDMA | branch create end to end, WAL path end to end |
| 6 | config generation, 16-node deploy with `transport = "rdma"` (new topology `cloudlab_xl170_rdma.toml`, `state_dir /mnt/ssd/bench-xl170-rdma`), the same variants as phase 1 into `/mnt/ssd/exp12/rdma/` | phase-1 pass checks; `rdma link`/`ibv_devinfo`/`ulimit -l` captured per node into `code/rdma_env/` |

Rough effort: 3-4 weeks of implementation. Stage 1's microbenchmark decides whether channel-wakeup latency is
acceptable before any Neon code is touched.

### 2.7 Code provenance
Commit the UDS working tree on `local-uds` as a commit `U`; branch `rdma` from it. `code/neon_fork_commit.txt`
records 4187e2da7, `U`, and the rdma head; `code/neon_fork_rdma.patch` = `git diff U..rdma` plus untracked files
(`libs/rdma_stream/`, `pgxn/neon/pqrdma.*`), `git apply --check`ed; `code/shared_code.patch` for the repo changes
(`scripts/setup/*`, `tests/test_topo.py`) if uncommitted; `code/bin_sha256.txt` per arm.

## Pitfalls to design around
- Comp-channel wakeups cost 5-10 us: poll before arming, drain all CQEs per event, ack events in bulk.
- rdma_cm resolves the device from the route; a LAN address of the other zone fails at `rdma_resolve_addr` (no
  route) - surface the address in the error.
- QP + MR creation is ~0.3-0.5 ms per connection; keep the reconnect backoff for restart storms (2k computes).
- Peer death surfaces after RC retry exhaustion (~0.5-1 s); existing timeouts still bound it.
- Postgres WaitEventSets cannot drop an fd; close the comp channel only after the set is rebuilt (walproposer's
  `rm_safekeeper_event_set`), as libpq's socket today.
- Thousands of `ibv_open_device` contexts per compute node (one per backend): stress-test in stage 1; fallback is a
  shared context per process.
- Active MTU is 1024; raising netdev MTU to 4096 needs sudo on all nodes and switch support (unknown; test later).
- The user's memory note: big files only under `/mnt/ssd` (cargo/rustup homes, builds, state dirs, data).

## Verification (end to end)
- Phase 1: every `progress.log` line `DONE ... ok` after the retry pass (failed_ops 0, `num_processes` intended,
  `remote_cpu.a*.node_cores` present for a0-a7, pageserver `proc_cores`); `ceiling_check.csv` rows for N=128..1024 x
  4 kinds; LFC hit share ~0.20 at N=1 (exp 5: 20.4%); after the split, GetPage counts on a4/a5 within ~10% or exp 8's
  stripe skew; `az_netem.sh status` shows 16 nodes with `delay 420us` at every capture and a b -> a public ping of
  ~1.0 ms; `clock_offsets.json` < 10 ms. Figures regenerate from `data/` with the re-rooted scripts.
- Phase 2: stage tests above; in the full run the same pass checks plus: no TCP listener on pg/broker ports on any
  Neon node (`ss -ltn`), `rdma res show qp` counts growing with N, no `IBV_WC_RETRY_EXC_ERR`/RNR in service logs,
  memory on compute nodes > 4 GB available at N=1024. Report compares tcp (1 ms) vs rdma (native) per operation and
  the `tcp/lfc_nonetem` control separates transport gain from the removed delay.

## Not verified yet (check during implementation)
`tokio_postgres::Config::connect_raw` exported by the neon `rust-postgres` fork; the generic bound at the
walreceiver connection polling site; cbindgen picking up FFI re-exported from a dependency crate (fallback:
generate the header from `rdma_stream`'s own build.rs); mlx5 UAR limits above ~2k contexts; build time on 10c/20t
(est. 45-90 min); first real pass through `nfs_remote_storage()`, `systemd_unit` and `register_pageservers()`.
