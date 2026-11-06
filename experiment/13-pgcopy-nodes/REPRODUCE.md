# Reproducing experiment 13 (pgcopy)

Everything is driven from a coordinator outside the cluster (`pc20.cloudlab.umass.edu`, Ubuntu 22.04, in
`~/db-fork`); the benchmark itself runs on the cluster's client node n1 as a transient systemd unit. Paths below are
relative to the repo root; `X=experiment/13-pgcopy-nodes`, `T=scripts/setup/topologies/cloudlab_pgcopy.toml`.
Durations are those measured on 5 Nov 2026.

## 0. Machines

- 16 CloudLab xl170 (Utah), experiment `branchbench`, project rasl-PG0, one 25 GbE LAN 10.10.1.0/24; hostnames,
  addresses and node names in `$T` (n0-n15 = CloudLab node0-node15). Each: Xeon E5-2640 v4 (10 cores / 20 threads),
  62 GiB, SATA SSD; data on the CloudLab blockstore `/mydata` (368 GB ext4 on LVM, ~350 MB/s writes). Ubuntu 22.04.2,
  kernel 5.15.0-198. Probe: `$X/logs/network_probe_2026-11-05/` (rerun: its `scripts/{probe,iperf,disk}.sh`).
- A new instantiation changes the hostnames: update `$T` (`[nodes.*]`), keep `zone = "utah"` (the coordinator is
  zone z0 and must see public addresses).
- Coordinator: ssh key `~/.ssh/dassl_rsa` accepted by the nodes; uv (`curl -LsSf https://astral.sh/uv/install.sh |
  sh`), protoc 32.1 in `~/.local/bin`, then `uv sync --group dev && uv run python build_protos.py`;
  `sudo chown $USER /mydata && mkdir /mydata/exp13` (the `$X/data` symlink points there).
- Code: repo commit `code/base_commit.txt` plus `code/shared_code.patch` (`git apply --check` clean on that commit).
  PostgreSQL 17.11 from PGDG (`17.11-1.pgdg22.04+2`), installed by `pgcopy.py install`.

## 1. Setup (about 7 min; `$X/scripts/chain.sh setup` runs all five)

```
uv run python scripts/setup/pgcopy.py $T install         # 45 s: PGDG PostgreSQL 17 (no default cluster), shmmni,
                                                          # RemoveIPC=no, cluster ssh key, agent files, all nodes
uv run python scripts/setup/pgcopy.py $T start           # 20 s: pgcopy-agent + dbfork-cpu-agent units (LAN only)
uv run python scripts/setup/pgcopy.py $T seed            # 2 min 50 s: repo to n0, seed instance, CH W=3 (283 MiB)
uv run python scripts/setup/pgcopy.py $T deploy-client   # 1 min 30 s: repo + venv + own-view pgcopy.env on n1-n15
uv run python scripts/setup/pgcopy.py $T smoke           # 15 s: one fork per host, 300,000 stock rows each, deleted
```

Pass checks: `install` prints `postgres (PostgreSQL) 17.11` and `shmmni 32768` for all 16; `ss -ltn` on a node
shows ports 9100 and 9010 bound to its 10.10.1.x address only, nothing on 5432; `smoke` ends with
`15/15 forks ok` and only `seed` and the smoke run root on n0 before its final purge.

## 2. Smoke ladder (about 7 min)

```
$X/scripts/chain.sh smoke     # run_all.sh --smoke --multi on n1 -> n1:/mydata/exp13/smoke
```

Every progress line `DONE ... ok`: low contention (4 ops + 10 creates), multi-node scaling_data N = 1, 4 (4 ops),
ceiling N = 4 (4 kinds, 5 s), multi-node scaling_branch N = 1, 4, single-node scaling_branch N = 1, 4, cold start
1 x 3,000 reads.

## 3. Full run

```
uv run python scripts/setup/pgcopy.py $T status > $X/logs/status_before_full.txt
$X/scripts/chain.sh full      # detached unit exp13-full on n1: run_all.sh --multi -> n1:/mydata/exp13/run
$X/scripts/launch.sh n1 --status exp13-full
```

Steps, in order (resumable: rerunning skips points already DONE and retries the others once):

| Step | What | Where |
|---|---|---|
| low_contention | READ 40,000 / RANGE_READ 10,000 / INSERT 6,000 / UPDATE 6,000 on one branch, one client; 100 creates (every 5th connected) with one MIXED busy branch | n1 only |
| scaling_data | N = 1, 4, 16, 64, 128, 256, 512, 1024 x 4 ops, N split over n1-n15, cross-node start | 15 hosts |
| ceiling | minimal asyncio client, N = 128, 256, 512, 1024 x 4 kinds, 30 s, synchronised | 15 hosts |
| scaling_branch | N = 1, 4, 16, 64, 128, 256, 512 busy (MIXED) split over the hosts, 100 creates on n1 (rerun 12:11-12:47 after the trim fix, 36 min) | 15 hosts |
| scaling_branch (single) | N = 1, 4, 16, 64, 128 busy branches and the creator all on n1 | n1 |
| cold_start | 3 runs x 30,000 point reads on a fresh branch, 30 s apart | n1 |

## 4. Collect, figures

```
$X/scripts/chain.sh pull                              # data/run, data/nodes/<n>/, data/archive/<node>/forks.jsonl
uv run python $X/scripts/fork_phases.py               # copy/start split of every fork -> data/fork_phases.csv
uv run python $X/scripts/plot.py                      # figures/*.png + *.csv
uv run python $X/scripts/collect_results.py           # aggregated data -> $X/results/ (the part kept in git)
```

Pass checks for the record: every progress.log line `DONE ... ok` (no `CHECK-FAIL`, no remaining `FAILED`);
`remote_cpu.server.node_cores` and `remote_cpu.seed.node_cores` present in the summaries; agents' `failures.jsonl`
empty or explained in the README; `status` after the run shows only `seed` on n0.

## 5. Cleanup

`uv run python scripts/setup/pgcopy.py $T purge` (keeps the seed), `... stop`, `... wipe --yes` (deletes the seed
too); agent archives stay in `/mydata/pgcopy/archive` on every node until deleted by hand.

## Pitfalls

- **Trim the SSDs.** `/mydata` has no `discard`; without trims the drives slow down after a few hundred GB of copies
  (fork start 1.1 s -> 3.3 s on n1). Before a run: `sudo fstrim -v /mydata` on every node; during it the agents
  trim after every purge (`--fstrim`, set by `pgcopy.py start`; check `archive/<node>/trims.jsonl`).
- Background rows: with many fast background clients the runner's in-memory rows exhaust a node; pgcopy's
  scaling_branch runs record 1 background operation in 20 (`BENCH_BG_SAMPLE`, fast_client); multiply background
  counts by 20 (multi_scaling.py does for its merged summaries).
- Check every node's memory bandwidth before a run: n4 (hp091) had about 1 GiB/s against 6.4-7.2 GiB/s on the
  others (`dd if=/dev/zero of=/dev/null bs=64M count=32`, one and four at once) and fell 2-4x behind at high N. Replace
  such a node. Multi-node throughput is the sum of the nodes' own rates (`figures/scaling.csv` `throughput`); the
  shared driver's merged number (all ops / the longest window, `throughput_slowest_window`) follows the slowest node.

- The coordinator never reaches the agents (LAN only); every agent-facing command runs on a node over ssh
  (`pgcopy_ctl.py`). A command that should talk to agents and is run on the coordinator must go through `pgcopy.py`.
- Starting a copied instance is crash recovery: PostgreSQL fsyncs the whole data directory first (about 1 s for the
  ~320 MB copy at ~350 MB/s), so `--no-sync` on the copy moves that cost into the start, it does not remove it.
- `pg_basebackup -X stream` copies the WAL written during the backup; with many copies at once each one spans more
  16 MB segments (321 MB alone, up to ~490 MB with 15 concurrent).
- Multi-node branch points: only the process with the timed creator thread passes the runner's start barrier, so
  the GO coordinator expects one connection per node (the first smoke attempt waited for two and hung; kept in
  `data/smoke/pgcopy/scaling_branch.hung_go_count/`).
- The runner's `backend_observations` (fork phases) come from its main process only; with several client processes
  the per-fork record is the agents' `forks.jsonl` (`fork_phases.py`).
