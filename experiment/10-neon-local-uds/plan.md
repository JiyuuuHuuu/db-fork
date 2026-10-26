# Experiment 10: self-hosted Neon on one machine with all IPC over Unix sockets

## Context

Experiment 7 gave a local single-machine baseline with Doltgres, but Dolt's write path is slow by design, so its
curves show Dolt's engineering rather than the resource limits of a single-machine branchable DB. This experiment adds a
second local baseline: the Neon fork (`/mydata/jiyu/neon`, branch `dev`, HEAD 4187e2da7) deployed as plain local
processes, the way its own `experiments/1-branch-interference-local/LOCAL_CLUSTER.md` describes (one pageserver, one
safekeeper, broker, storage controller + its Postgres, compute_ctl per branch), but pinned to NUMA nodes 2 and 3 like
experiment 7, and with every inter-process network connection replaced by local IPC. The three benchmarks of
experiments 5/8 are rerun: A ops latency at low contention, B data-ops scaling with busy branches, C branch-creation
latency under busy branches. The report attributes saturation to CPU, disk or memory of the DB side (node 2).

User decisions (asked 2026-10-25/26): data AND control path on Unix domain sockets; Neon on all 24 cores and the memory
of node 2, benchmark client on node 3; experiment 5/8 workload parameters (W=4 generated seed, exp 5's op counts and N
lists, MIXED background, connect every 5th create); one safekeeper; **no local file cache** (on one machine an LFC hit is
a file read that is either an undeclared RAM cache or an NVMe read no faster than a GetPage over the socket), and instead
**shared_buffers sized to roughly 30% of the dataset per compute**. Dataset = what the workload touches: `stock` heap
139.5 MB + `stock_pkey` 9.0 MB = 148.5 MB (exp 5 `data/seed/seed.json`), so `shared_buffers = 45MB` (5,760 pages);
prewarm into shared buffers (`pg_prewarm(..., 'buffer', ...)`, the first 4,350 heap pages then the whole key, 5,449
pages, as exp 5's prewarm did before the LFC caught the evictions). Plus a cheap TCP-loopback arm with the same binaries
to quantify the IPC change.

Memory consequence (exp 5's probe: 47 MB per compute without LFC at 8 MB shared_buffers, so ~84 MB at 45 MB once
prewarmed): node 2 (~57 GB free, pageserver ~10 GB) holds 512 computes (~43 GB) but not 1,024 (~86 GB). Scaling runs
N = 1..512; N = 1,024 is attempted only if the per-compute memory measured at the smoke run (PSS from
`/proc/<pid>/smaps_rollup`, summed per compute) predicts a fit; otherwise it is skipped and the report says so (the
fallback of letting computes spill to node 3's memory with `--membind=2,3` is not used unless the user asks: it would
measure cross-NUMA memory at that one point).

Why Unix sockets and not shared memory: every Neon edge is a byte stream (libpq, HTTP, gRPC) whose server code is
already generic over the socket type; a shm GetPage transport would be a multi-week rewrite of the C extension and the
pageserver read path. Noted in the report as the next step, out of scope here.

## IPC map (verified in the fork; TCP today, target UDS)

| # | Edge | Protocol | Change needed |
|---|---|---|---|
| 1 | compute backends -> pageserver GetPage | libpq `pagestream_v3` | server listener only (`page_service.rs`); client unchanged (`host=/dir port=N` or `postgres://no_user@%2Fdir:N`) |
| 2 | compute_ctl -> pageserver basebackup / LSN lease | libpq (`postgres` crate) | none |
| 3 | walproposer -> safekeeper WAL push | libpq | server listener only (`wal_service.rs`); `neon.safekeepers='/dir:5454'` works as is |
| 4 | pageserver walreceiver -> safekeeper | tokio-postgres | `libs/postgres_connection` host parser rejects `/dir` (fix) |
| 5 | safekeeper, pageserver -> broker | tonic gRPC | server accept loop + client connector |
| 6 | pageserver -> storcon re-attach/validate | reqwest HTTP | reqwest 0.12.4 has no UDS: small `uds_http` shim |
| 7 | storcon -> pageserver mgmt API, storcon -> hook stub | reqwest HTTP | same shim; storcon node URL/DNS check |
| 8 | compute_ctl HTTP; neon ext -> compute_ctl (libcurl) | axum / libcurl | socket flags; `CURLOPT_UNIX_SOCKET_PATH` GUC |
| 9 | client, compute_ctl -> Postgres | libpq | config only (`listen_addresses=''`, `unix_socket_directories`) |
| 10 | storcon -> its Postgres | tokio-postgres | config only (percent-encoded host in `--database-url`) |

Not needed: storcon -> safekeeper (walproposer sends `allow_timeline_creation 'true'` when generations are off,
`pgxn/neon/walproposer.c:661-666`, and `--timelines-onto-safekeepers` defaults to false); WAL redo (pipes) and
remote storage (`local_path`) are already local. CPU sampler and hook stub are instrumentation, see Part 2.

## Part 1: Neon fork changes (`/mydata/jiyu/neon`, new branch `local-uds` off `dev`)

Convention (one place to learn, TCP stays default so the same binaries do the TCP arm):
- Listen addresses: `unix:/abs/path` anywhere a `host:port` string is accepted; libpq servers must bind
  `<dir>/.s.PGSQL.<port>` so libpq clients find them with `host=<dir> port=<port>`.
- libpq endpoints: native forms, `postgres://no_user@%2Fdir:64000` and `/dir:5454`.
- HTTP/gRPC endpoints: `http+unix://<hex(abs socket path)>/<http path>` (hex, because `http::Uri` rejects `%` in the
  authority and `url::Url` needs a host; `control_plane_api` needs a path after the socket).
- Socket root `/mydata/jiyu/neon-exp10/run/` (28 bytes); per-compute dirs `run/c<slot>/`; parser rejects > 100 bytes.

Steps, in order (data path first so the cluster can be brought up and seeded while the control path is finished):

1. Helpers. `libs/utils/src/listen_addr.rs` (new): `ListenAddr {Tcp, Unix}` with FromStr/Display, `bind()` ->
   `StdListener {Tcp, Unix}` (unlink a stale socket file first), `into_tokio()` -> `TokioListener` whose `accept()`
   yields `(AnyStream, SocketAddr)` with a dummy `127.0.0.1:0` peer for Unix; `AnyStream` implements
   AsyncRead/AsyncWrite/AsRawFd, `set_nodelay` is a no-op on Unix. Register in `libs/utils/src/lib.rs` next to
   `tcp_listener` (line 47). `libs/uds_http/` (new workspace crate): `url(sock, path)`, `socket_path(&Url)`, and
   `execute(&reqwest::Client, reqwest::Request)` that, for `http+unix` URLs, converts the request with
   `TryFrom<reqwest::Request> for http::Request`, sends it over `hyper::client::conn::http1::handshake(TokioIo<UnixStream>)`
   (the pattern already in `compute_tools/src/communicator_socket_client.rs:82-93`), buffers the body and returns a
   `reqwest::Response` via `From<http::Response<Body>>`; plain `http://` URLs go through `client.execute`. Unit tests
   for both crates.
2. Parsers. `libs/postgres_connection/src/lib.rs:14-29`: a host starting with `/` becomes `Host::Domain(path)` without
   `Host::parse` (tokio-postgres then treats it as a socket dir); test. `compute_tools/src/compute.rs:1433-1446`: a
   safekeeper entry starting with `/` is built with `Config::new().host(dir).port(port)` instead of a URL.
3. Listeners. `pageserver/src/bin/pageserver.rs:402,419,425,849-854` use the helper (keepalive only for Tcp);
   `pageserver/src/page_service.rs:136-142,211-221,308,321-327` take `TokioListener`/`AnyStream` (protocol layer
   `PostgresBackend::new_from_io` is already generic). `libs/http-utils/src/server.rs:48-107` accept `StdListener`
   (used unchanged by pageserver, safekeeper `http/mod.rs`, storcon `main.rs:506-538`). `safekeeper/src/bin/safekeeper.rs:496,507,523`
   and `safekeeper/src/wal_service.rs:28-121`. `storage_controller/src/main.rs:63-71,506-522` `--listen` as `ListenAddr`.
   After this step edges 1-4 and all mgmt listeners are on UDS.
4. Broker. `storage_broker/src/bin/storage_broker.rs:81,703-822`: `ListenAddr`, accept loop over `AnyStream` with the
   dummy `RemoteAddr`. `storage_broker/src/lib.rs:42-71 connect()`: for `http+unix` use
   `Endpoint::connect_with_connector_lazy(tower::service_fn(UnixStream::connect -> TokioIo))`; add `tower`, `hyper-util`
   (`tokio` feature), `hex` to its Cargo.toml.
5. HTTP clients and storcon. Replace `.send()` with `uds_http::execute` in `pageserver/client/src/mgmt_api.rs` (new
   `Error::SendRequestUds` variant, handled like `SendRequest` in `storage_controller/src/node.rs:363`, `service.rs:253`),
   `safekeeper/client/src/mgmt_api.rs:228-240`, `pageserver/src/controller_upcall_client.rs:140-163`,
   `storage_controller/src/compute_hook.rs:656-669`, `storage_controller/src/http.rs:2074` (passthrough).
   `storage_controller/src/node.rs:98-108 base_url()`: `unix:` prefix -> `uds_http::url`; `service.rs:7768-7790`: skip
   the DNS lookup for `unix:`. Node registration payload: `listen_http_addr: "unix:.../ps-http.sock", listen_http_port: 0,
   listen_pg_addr: "/mydata/jiyu/neon-exp10/run", listen_pg_port: 64000`.
6. compute_ctl and extension. `compute_tools/src/bin/compute_ctl.rs:78-98` add `--external-http-socket`,
   `--internal-http-socket`; `compute_tools/src/http/server.rs:40-50,210-240` serve on `tokio::net::UnixListener` when
   set (axum 0.8 implements `Listener` for it); `compute_tools/src/config.rs:54,347` write
   `neon.extension_server_socket`; `pgxn/neon/extension_server.c` new string GUC and `CURLOPT_UNIX_SOCKET_PATH` at the
   two libcurl call sites (`extension_server.c:49`, `libpagestore.c:1070`). Rebuild with `make -s -j neon-pg-ext-v17`.
7. Build and test. `BUILD_TYPE=release CARGO_BUILD_FLAGS="--features=testing" make -j -s` (protoc 25.3 from
   `~/.local/protoc`); `cargo test -p utils -p postgres_connection -p uds_http -p storage_broker -p storage_controller`;
   pagebench needs no change (`tokio_postgres::connect` on the URL form) and is the GetPage TCP-vs-UDS microbench.

Commit on the `local-uds` branch when the user asks; the experiment records the fork commit and `git diff` either way.
Estimate: 4-5 days for steps 1-7.

## Part 2: experiment in this repo (`experiment/10-neon-local-uds/`)

Layout per `.agent/CLAUDE.md`: README.md, plan.md (this plan), REPRODUCE.md, report.md, `code/` (base commit,
`git status`, `shared_code.patch` checked with `git apply --check`, `neon_fork_commit.txt`, `neon_fork.patch`,
`bin_sha256.txt`), `config/` (copies of exp 5's `compute_config_template.json` and `compute_hook_stub.py`, the stub
gaining a `--listen unix:<path>|host:port` flag), `scripts/`, `data/neon_local/{seed,low_load/{uds,tcp},scaling/{uds,tcp},branch_create,archive,status}`,
`data/superseded/smoke_*`, `logs/`, `figures/`.
Server-side files outside the repo: `$ROOT=/mydata/jiyu/neon-exp10/` with `bin/` (snapshot of the fork's binaries and
`pg_install/v17`, hashed, so a rebuild cannot change a running experiment), `run/` (sockets, pid files),
`<transport>/{ps,ps-remote,sk,sk-remote,storcon_db}` (one state tree per transport: storcon returns 409 when a node
re-registers with different addresses, so each transport gets its own storcon DB and its own 5-minute seed),
`computes/`, `archive/`, `logs/`, `env.exp10`, `transport`, `driver.lock`.

Shared code change (listed in the README): `dblib/neon_local.py` `http()` gains an `http+unix://<hex>/path` branch
using an `http.client.HTTPConnection` subclass over `AF_UNIX`; `psycopg2.connect(host=<dir>)` already means a Unix
socket, so the agent returns the socket directory as `host`. Tests in `tests/test_neon_local.py`: URL dispatch with a
recorder, a round trip against a `ThreadingUnixStreamServer`, and `_compute` passing a directory host. No `runner2.py`
change: `DBFORK_REMOTE_CPU` labels carry per-process-group CPU, `DBFORK_SERVER_PID` is the pageserver.

Scripts (copy-and-adapt sources in brackets):
- `env.sh` [exp 7]: `ROOT`, `RUN`, `NEON_BIN`, `SERVER_CPUS` = node 2, `CLIENT_CPUS` = node 3,
  `NUMA_SERVER=(numactl --cpunodebind=2 --membind=2)`, `NUMA_CLIENT=(... 3 ... 3)`, `TRANSPORT=uds|tcp`, ports (socket
  name suffixes under uds), `W=4 SB=45MB LFC_MB=0 EIC=2 PREWARM_HEAP_BLOCKS=4350 SEED_SB=8GB` (SB and the prewarm
  derived from seed.json: `SB_MB = ceil(0.30 * (stock + stock_pkey bytes) / MiB)`, `PREWARM_HEAP_BLOCKS = SB pages -
  pkey pages - 5%`), `MAX_PROCS=24`, `DISK_GUARD_GB=150`, `MEM_GUARD_GB=4`; helper functions `pg_sock`, `http_sock`, `listen_addr`, `url` (hex), `curl_url`
  (`curl --unix-socket`), `service_pid`. Never touches `.env`; loads `$ROOT/env.exp10` (written by `neon_ctl.py env-init`).
- `neon_services.sh install|start|stop|restart|status [--transport] [--wipe]` [exp 5 `setup_neon_cluster.sh` args,
  exp 7 `doltgres_server.sh` shape]: `setsid nohup numactl ... <service>` with pid files; order broker, storcon Postgres
  (initdb once, `neon_rmgr`, `listen_addresses=''`, `unix_socket_directories=$RUN`, `dynamic_shared_memory_type=mmap`),
  hook stub, storcon (`--dev --listen unix:... --database-url postgresql://jiyu@%2F...:1235/storage_controller
  --tenant-rate-limit 1000 --control-plane-url http+unix://<hex hook.sock>`), safekeeper (`--listen-pg unix:$RUN/.s.PGSQL.5454
  --advertise-pg $RUN:5454 --listen-http unix:... --broker-endpoint http+unix://... --remote-storage local_path`),
  pageserver (generated `pageserver.toml`: unix listen addrs, `broker_endpoint`, `control_plane_api
  http+unix://<hex storcon.sock>/upcall/v1/`, `remote_storage local_path`, `max_file_descriptors=65536`,
  `page_cache_size=1048576`, eviction off, `pg_distrib_dir`), readiness via curl over the sockets, node registration,
  then the agent. `status`: per-pid `Cpus_allowed_list`, `numastat -p`, `mpstat` of both nodes, node-2 meminfo,
  `df /mydata`, `ss -ltnp` (must show no TCP listener of ours in uds mode) and `ss -xlp`, snapshot hash check.
- `neon_node_agent.py` [exp 5]: one agent on node 2 (children inherit cpuset and mempolicy); `--transport`; computes'
  pgdata on NVMe `$ROOT/computes`; LFC off (`neon.max_file_cache_size=0MB`, `neon.file_cache_size_limit=0MB`, the
  `lfc_mb`/`lfc_dir` options and tmpfs handling removed), `shared_buffers` from the request (45MB), `max_connections=25`;
  spec rendering with `libpq_url`/`pageserver_connstring = postgres://no_user@%2F...run:64000`,
  `safekeeper_connstrings=[$RUN:5454]`, `listen_addresses=''`, `unix_socket_directories=$RUN/c<slot>`,
  `dynamic_shared_memory_type=mmap`; `/stats` adds per-compute PSS (sum of `smaps_rollup` Pss over the compute's
  process group, sampled for up to 16 computes) so the N=1,024 decision has a measured number; compute_ctl `--connstr postgres://cloud_admin@%2F...c<slot>:<port>/postgres
  --external-http-socket ... --internal-http-socket ...`; the agent's own HTTP on `unix:$RUN/agent.sock`
  (`ThreadingHTTPServer` with `address_family=AF_UNIX`, `disable_nagle_algorithm=False`); readiness `psql -h <dir>`;
  metrics/compute_ctl polling over AF_UNIX; orphan SysV shm cleanup kept; disk guard 40 GB; no `/cpu`, no `/time`.
- `neon_ctl.py` [exp 5]: `env-init --transport` (writes `$ROOT/env.exp10` with hex URLs and the tenant/seed ids, copies it
  to `data/neon_local/seed/`), `seed --scale 4` (through the local agent, `shared_buffers=8GB`, LFC off, statement log
  off, `statement_timeout=600s` on the bench role, image compaction + `bump_past_images`), `purge`, `status`, `metrics`,
  `smoke`; drop `shard-split`, `locate`, `clock-check`.
- `cpu_agent.py` [exp 7, extended]: runs on node 3 on loopback TCP `127.0.0.1:9010` (instrumentation polled once per
  second by the client, never on a timed path; keeps `RemoteCpuSampler` unchanged); sets `server`/`clients` with exp 7's
  CSV columns, plus `<run>.procs.csv` per process group (pageserver, walredo, safekeeper, storcon, storcon_pg, broker,
  hook, agent, compute_ctl, computes, other_node2) by walking ppid chains from the pid files, `nvme2n1` diskstats and
  node-2 meminfo. `GET /cpu?set=server` adds `proc_s` (all DB groups) and disk fields; `GET /cpu?group=<g>` per group.
  Drivers export `DBFORK_REMOTE_CPU="server=...;clients=...;pageserver=...;safekeeper=...;computes=...;compute_ctl=...;storcon=...;broker=..."`.
- `common.sh`, `run_low_load.sh`, `run_scaling.sh --transport`, `run_branch_create.sh` [exp 5, with exp 7's guards:
  numactl mask check, flock, per-run cpu agent, `check()` on the summary JSON, md5 of shared files]. Parameters verbatim
  from exp 5: low load READ 40000 / RANGE_READ 10000 / INSERT 6000 / UPDATE 6000, 100 creates connect-every-5 MIXED;
  scaling N = 1,4,16,64,128,256,512 (1024 only if the memory check passes) with exp 5's `ops_per_client` (totals
  12000/2500/6000/6000, minimums 3000/600/2500/2500, caps 1,000,000/640,000), shared_buffers 45 MB filled by the prewarm
  on every client's branch before its warm-up, no LFC, statement log off, `--setup-parallelism 64`; branch create
  N = 1,4,16,64,128,256,512. TCP arm: low load, and scaling READ at N = 1,16,64,256. `check()` also fails a run whose
  node-2 MemFree dropped under `MEM_GUARD_GB`. Run under
  `"${NUMA_CLIENT[@]}"`, detached with `setsid nohup ... > logs/driver_<step>.log`.
- `ceiling_check.py` [exp 5]: minimal asyncio client at N = 128..1024, 30 s, 24 procs, with the same CPU labels.
- `run_all.sh` [exp 5]: resumable steps `smoke, low_load, branch_create, scaling_uds, ceiling_uds, switch_tcp, scaling_tcp,
  low_load_tcp, switch_uds, pull_data`.
- Analysis: `load_runs.py`, `summarize.py`, `cpu_window.py` [exp 7, reads procs CSV: per-group cores, disk busy and
  write MB/s, node-2 min MemFree over each timed window], `plot_low_load.py` (grouped bars: this UDS p50/mean, this TCP,
  exp 5 cross-zone, exp 7 Doltgres where the op exists), `plot_scaling.py` [exp 5] (`throughput_latency.png`,
  `data_ops.png`, exp 8 lfc line as reference), new `scaling_cpu.png` (stacked DB-side cores by group vs N with the
  24-core line, disk busy, client cores), `plot_branch_create.py` [exp 5] (with exp 7 Doltgres MIXED and exp 5 lines,
  compute start), `tcp_vs_uds.py` (per op: p50/mean, throughput, GetPage per op from the pageserver metrics delta, DB
  cores per 1000 ops/s, sys+softirq share; saved cost per round trip). Figures embedded in `report.md` (Markdown only).

Budget (node 2 ~64 GB, ~57 GB free): pageserver ~10 GB; computes ~84 MB each prewarmed (45 MB shared buffers + ~28 MB
other shared memory + ~30 MB private incl. compute_ctl, from exp 5's probe) so ~43 GB at N=512; N=1,024 (~86 GB) only
if the measured PSS says otherwise. Shared buffers are anonymous mmap, allocated on touch (THP is `madvise`, so no
inflation). NVMe peak per N=512 run ~30 GB transient (safekeeper WAL 8 GB, pgdata ~20 GB) on 317 GB free: sweep guard
150 GB, agent guard 40 GB, free space and node-2 MemFree logged per tick.

## Run order and expected durations

1. Fork steps 1-3, build, `neon_services.sh install && start --transport uds`, `status`; seed (`neon_ctl.py seed --scale 4`, ~5 min), `env-init`, `smoke`.
2. Fork steps 4-6, rebuild, `install` (new snapshot), restart, `smoke` again; `ss -ltnp` shows no TCP listener of ours.
3. Smoke drivers (`--smoke`: ops/10, N 1,16 / 4,64 / 4, 10 creates) into `data/superseded/`, ~10 min; read the
   per-compute PSS from the agent during the N=64 smoke run and decide whether N=1,024 fits (1024 x PSS + 10 GB <= 55 GB);
   then the TCP switch smoke.
4. Full: A uds (~10 min), C (~12 min), B uds (~40-50 min to N=512, +25 min if N=1024 runs), ceiling (~12 min),
   switch to tcp (seed the tcp tree, ~8 min), B tcp subset (~6 min), A tcp (~10 min), switch back, `pull_data.sh`.
   About 3 h of runs; the smoke run calibrates the estimates recorded in REPRODUCE.md.
5. Analysis, figures, `report.md`, `README.md`, `REPRODUCE.md`; memory note for the experiment.

## Verification

- `uv run pytest tests/test_neon_local.py tests/test_micro_runner2.py tests/test_db_api.py` after the dblib change;
  `cargo test` for the new/changed fork crates; `make -s -j neon-pg-ext-v17` builds the extension.
- Integration smoke on the sockets: tenant + timeline via storcon socket, compute start < ~1.5 s, `psql -h <dir>` select 1,
  `pgbench -i -s 1`, a branch with a second compute, delete; in uds mode `ss -ltnp` lists only the CPU agent.
- Placement: every DB pid's `Cpus_allowed_list` is node 2's list and `numastat -p` shows node-2 memory only; the runner's
  processes are on node 3 only (status snapshot during the N=256 READ run).
- Every run's `progress.log` line is `DONE ... ok` (`failed_ops == 0`, `background.failed == 0`, `num_processes ==
  min(N,24)` or `min(N+1,24)`, `remote_cpu.server.proc_cores/node_cores/disk_util` and per-group labels present,
  node-2 MemFree min > 4 GB, disk free > 40 GB, zero computes after the purge), md5 lines identical, no `RETRY`.
- Sanity against exp 5's N=1 cross-zone numbers (one 38.9 ms RTT per round trip): point READ 40.7 ms -> expect
  ~0.1-0.5 ms p50 with about 30% of point reads served from shared buffers (bimodal: buffer hit ~0.05 ms, GetPage over
  the socket ~0.1-0.3 ms; the pageserver's GetPage delta per op is the check); RANGE_READ 161 ms -> low ms; INSERT 41 / UPDATE 56 ms -> ~0.3-1.5 ms; branch create 85 ms -> ~5-20 ms;
  compute start 5.4 s -> ~0.5-1.5 s. UDS vs TCP should differ by tens of µs per round trip; a larger or inverted gap
  means a TCP fallback or Nagle/delayed-ACK effect to investigate.
- pagebench `get-page-latest-lsn` over `postgres://postgres@%2F...run:64000` vs the TCP config, and a 30-line Python
  ping-pong RTT probe (loopback TCP with TCP_NODELAY vs AF_UNIX), both recorded in the report's IPC section.

## Pitfalls to document in REPRODUCE.md

No sudo: no systemd, `RemoveIPC` stays at its default and lingering is off, so every Postgres runs with
`dynamic_shared_memory_type=mmap` (POSIX shm in `/dev/shm` would be removed when the user's last session ends) and the
experiment runs from a tmux session that stays open; `shmmni=4096` with orphan SysV segment cleanup; sun_path <= 108
bytes (the `.lock` suffix counts); socket dirs must not contain `:` or `,` (walproposer split); storcon node re-registration
409 (per-transport state trees); root-owned Hadoop JVMs with 0-95 affinity can land on node 2 (hence per-group CPU and
the `other_node2` column); `/mydata` is shared and 82% full (guards); the hook stub must be threaded; `max_connections=25`
per compute; timeline deletes of written-to timelines take seconds (`NEON_LOCAL_PURGE_PARALLEL=64`); the transport switch
needs `env-init` again; `.env` is never written; short timed windows at N <= 16 use exact per-process CPU via `cpu_window.py`;
45 MB shared buffers per compute cap N at 512 on node 2 (N=1,024 only after the PSS check); exp 5/8's low-load and
branch-create numbers were taken with the 36 MB LFC and 8 MB shared buffers, the scaling comparison uses their `nolfc`
variant where the cache differs only in size (report states both).
