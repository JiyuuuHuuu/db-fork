# Setup: deploy Neon and Doltgres from a topology file

These scripts deploy the servers that `scripts/workloads/` measure. They cover both of the deployments earlier
experiments built by hand:

- **One host** (experiments 7, 10, 11, `dassl-serv-01`):
  - Every service runs as a local process, pinned to the topology's server CPUs and memory node.
  - Neon talks over Unix domain sockets.
- **A CloudLab cluster** (experiments 1-5, 8):
  - Services run as systemd units on several nodes, reached over ssh.
  - Binaries are rsynced from the coordinator, data goes on each node's SSD, and computes go on tmpfs.
  - Services use LAN addresses within a zone and public addresses across zones.

Nothing here runs a benchmark. The workload scripts never start a server.

## Topology file

A topology is a TOML file. Three examples are in `topologies/`:
- `local_dassl.toml`: dassl-serv-01, experiment 11's placement.
- `cloudlab_two_zone.toml`: experiment 5's placement on the cluster of 22 Oct 2026.
- `cloudlab_four_node.toml`: the 4-node cluster of 29 Oct 2026, a sketch.

| Section | What it holds |
|---|---|
| top level | `name`; `state_dir` (on the coordinator: staged binaries, `code/`, env files, seed records; on a remote client node the same path holds the copied env files) |
| `[ssh]` | `user`, `key` (CloudLab) |
| `[nodes.<name>]` | `host` (`"local"` = this machine, no ssh), `lan`, `public`, `zone`; `ssd_dev` (formatted with XFS only if blank, mounted at `data_root`, default `/mnt/ssd`), `tmpfs_size` (computes on `/mnt/tmpfs/computes`), `disk_dev` (disk counters); placement `server_cpus`/`server_mem` (taskset + numactl `--membind`; `2-38:4` is a stride range), or `server_numa`; the same for `client_*` |
| `[neon]` | `build_dir` (the Neon checkout with built binaries), `transport` (`uds` on one host, `tcp` otherwise), roles `client`, `storcon` (+ its Postgres and the compute-hook stub), `broker`, `seed_compute`, `pageservers` (the first is node id 1; the others join with scheduling Pause and share its remote storage over NFS), `safekeepers`, `compute_agents` (computes placed round-robin); optional `ps_page_cache_size` |
| `[doltgres]` | `version` (1.4.0), `server`, `client`, optional `user`/`password` |
| `[ports]` | experiment 5's ports by default (storcon 1234, pageserver 9898/64000, safekeeper 7676/5454, agent 9000, CPU agent 9010, Doltgres 5433, ...) |
| `[workload]`, `[neon.workload]`, `[doltgres.workload]` | workload parameters. The defaults are experiment 5's (`WORKLOAD_DEFAULTS` in `topo.py`); the backend sections override `[workload]` |

**Addresses.** A role is reached:
- on a Unix socket (Neon `uds`) or on loopback, when it is on the same host as the caller;
- on its LAN address, when it is in the same zone;
- on its public address, when it is in another zone (CloudLab LANs of different sites do not route to each other).

```bash
uv run python scripts/setup/topo.py check T.toml                      # validate + summary
uv run python scripts/setup/topo.py shell T.toml --backend neon       # bash exports used by the workloads
uv run python scripts/setup/topo.py env T.toml --backend neon         # <state_dir>/neon.env (never the repo's .env)
uv run pytest tests/test_topo.py
```

## Neon: `neon.py TOPO <command> [--dry-run]`

| Command | What it does (source) |
|---|---|
| `install` | Snapshots the fork's binaries and `pg_install/v17` into `<state_dir>/neon-bin`. Records the commit, `git diff` and sha256 in `<state_dir>/code/`. Remote nodes are prepared (apt, SSD, tmpfs, `RemoveIPC=no`, `kernel.shmmni`) and the binaries rsynced (experiment 5's `setup_neon_cluster.sh`; experiment 11's `neon_services.sh install`) |
| `start` / `stop` / `restart` / `status` / `wipe --yes` | Start order: broker, storage controller's Postgres, hook, storage controller (`--tenant-rate-limit`), safekeepers, pageservers (registered; extra ones paused, NFS remote storage), compute agents, and on remote nodes the CPU agent unit. Locally the services are `setsid` processes with pid files in `<neon_root>/run`. `start` refuses if Doltgres runs on the same host |
| `seed [--scale W]` | Experiment 8's seed: tenant, root timeline, seeding compute on `seed_compute` (shared_buffers 8 GB, no LFC), role and database `bench`, CH-benCHmark W, VACUUM ANALYZE, forced image layers, WAL written past them. Writes `<state_dir>/neon_seed.json` |
| `env-init [--view NODE]` | `<state_dir>/neon.env` for the runner: `NEON_LOCAL_*` including tenant and seed timeline, and `DBFORK_REMOTE_CPU` |
| `shard-split --shards K` | Experiment 5's two-pageserver variant: one-way split, stripe `workload.stripe_pages`. `ps_opts` go into the seed record and `NEON_LOCAL_PS_OPTS` |
| `purge`, `smoke`, `metrics FILE`, `clock-check` | From `neon_ctl.py` |
| `deploy-client` | rsync the repo to the client node, `uv sync`, protoc and protos, and copy the env and seed files |
| `pull-archive DEST` | Each agent's `samples.jsonl`, `starts.jsonl` and compute logs (experiment 5's `pull_data.sh`) |

Supporting files:
- `neon_ctl.py`: seed, shard split and the other commands. It runs with an env file, near the storage.
- `neon_node_agent.py`: the compute agent (experiment 5's merged with experiment 11's Unix-socket support).
- `compute_config_template.json`, `compute_hook_stub.py`.

## Doltgres: `doltgres.py TOPO <command> [--dry-run]`

| Command | What it does (source) |
|---|---|
| `install` | Release tarball into `<doltgres_root>/dl/<version>`. Writes `config.yaml` (only the keys 1.4.0 accepts) and the committer identity. Remote nodes are prepared (apt, SSD) (experiment 1's `setup_cloudlab_dolt.sh`; experiment 11's `doltgres_server.sh`) |
| `start` / `stop` / `restart` / `status` | Local: `setsid`, `taskset` + `numactl --membind`, `GOMAXPROCS` = server CPUs, pid in `<doltgres_root>/doltgres.pid`; refuses if Neon runs. Remote: systemd unit `doltgres` plus the CPU agent unit. `stop` allows 600 s |
| `seed` | Builds `ch_w<W>_seed` once with the runner's own `ensure_seed_database`. Writes `<state_dir>/doltgres_seed.json` |
| `env-init`, `deploy-client` | As for Neon (`DOLT_*`, `DBFORK_REMOTE_CPU`) |

## CPU sampling

`cpu_agent.py` serves the counters that runner2's `RemoteCpuSampler` reads (`DBFORK_REMOTE_CPU`). How it runs depends
on where the server is:

- **Server on the client's host:** the workload scripts start one per run, with a CSV (experiment 11). Neon's
  groups come from the pid files; Doltgres's from its pid file.
- **Remote nodes:** one shared systemd unit, `dbfork-cpu-agent`, started by either setup script. It finds both
  backends' process groups by name and is left running by `stop`.

## Pitfalls (from the experiments' REPRODUCE files)

- **Never run Neon and Doltgres at once on shared server CPUs.** Both `start` commands refuse.
- **Never write the repo's `.env`.** Other experiments share the checkout. The env files live in `state_dir`.
- **Linger and `RemoveIPC=no`.** Locally the user must linger (`loginctl enable-linger`), or logout removes SysV
  shared memory. On CloudLab every node, the client included, needs `RemoveIPC=no`.
- **The single-host fork needs two patches:** the storcon rustls Unix-socket fix and, on kernel 5.15, the io_uring
  IOWQ affinity patch, or the pageserver's io-wq workers escape the CPU pinning. Both are in the fork's build at
  `/mydata/jiyu/neon` (branch `local-uds`).
- **A bulk-loaded timeline has no image layers.** `seed` forces them and writes WAL past them.
- **The shard split is one-way.** Seed a new tenant to go back to one pageserver.
- **Memory:** each compute costs ~45-80 MB of the server node. `local_dassl.toml` caps Neon's N at 512.
- **Untested on CloudLab:** the remote paths (ssh, systemd, rsync, NFS) have only been checked with `--dry-run`.
  Expect fixes on the first cluster run.
