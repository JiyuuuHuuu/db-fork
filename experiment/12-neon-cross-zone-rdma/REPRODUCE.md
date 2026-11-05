# Reproducing experiment 12

Everything runs from a0 (hp155) in `~/db-fork`, except the benchmark runs, which run on b0 (hp140) as transient
systemd units started by `scripts/launch.sh`. Repo commit at the start: `code/base_commit.txt` (de59262), plus the
uncommitted `scripts/setup` and `scripts/workloads` changes listed in the README. Paths below are relative to the repo
root; `X=experiment/12-neon-cross-zone-rdma`, `NETEM=.agent/network-probe-2026-11-02-xl170/scripts/az_netem.sh`.

Measured durations are from 3 Nov 2026.

## 0. Cluster and toolchain

- 16 xl170 nodes, `nodes.txt` in `.agent/network-probe-2026-11-02-xl170/`; ssh key `~/.ssh/dassl_rsa` on a0 and b0.
- Cross-zone delay: `$NETEM apply 420` on all 16 nodes; `$NETEM status` must show `delay 420us` on 16 nodes and a
  b -> a public ping of ~1.0 ms. It is lost on reboot.
- Rust toolchain only for the rdma arm (a0): `CARGO_HOME=/mnt/ssd/cargo RUSTUP_HOME=/mnt/ssd/rustup`, toolchain
  1.88.0, apt packages as in `plan.md` 1.2 including `libibverbs-dev librdmacm-dev`; log in `logs/toolchain.log`.

## 1. The two Neon builds

- tcp arm: `/mnt/ssd/neon-src`, branch local-uds 4187e2da7 + experiment 11's patch (`code/neon_fork.patch`). Its
  binaries are experiment 11's (`code/bin_sha256.txt`); no rebuild needed.
- rdma arm: `/mnt/ssd/neon-rdma` = commit 6de97fbc3 (the local-uds working tree as one commit) plus
  `code/neon_fork_rdma.patch`:

  ```
  git -C /mnt/ssd/neon-rdma checkout 6de97fbc3 && git -C /mnt/ssd/neon-rdma apply $PWD/$X/code/neon_fork_rdma.patch
  cd /mnt/ssd/neon-rdma && taskset -c 6-9,16-19 make -j8 -s BUILD_TYPE=release CARGO_BUILD_FLAGS="--features=testing"
  ```

  Full build about 1 h on 8 threads (`logs/rdma_build_stage*.log`); after a change to `pgxn/neon` only,
  `make neon-pg-ext-v17` (about 1 min, `logs/rdma_build_fix1.log`).
- Transport microbenchmark (optional): `scripts/rdma_pingpong.c` (build line in its header), or
  `cargo run --release -p rdma_stream --example pingpong`.

## 2. tcp arm (about 2 h 10 min with setup)

```
T=scripts/setup/topologies/cloudlab_xl170.toml
uv run python scripts/setup/neon.py $T install --no-fanout      # 40 s
uv run python scripts/setup/neon.py $T start
uv run python scripts/setup/neon.py $T seed --scale 3            # 192 s (W=3 is the default now)
uv run python scripts/setup/neon.py $T env-init
uv run python scripts/setup/neon.py $T deploy-client
uv run python scripts/setup/neon.py $T clock-check               # offsets < 1 ms
uv run python scripts/setup/neon.py $T smoke
$NETEM status > $X/logs/netem_status_tcp_lfc_w3_start.txt
$X/scripts/launch.sh exp12-tcp-lfc-w3 /mnt/ssd/exp12/tcp/lfc/run.out \
    scripts/workloads/run_all.sh --topology $T --backend neon --outdir /mnt/ssd/exp12/tcp/lfc     # 1 h 35 min
```

Then the netem-off control (`scripts/chain_tcp.sh` does all of this unattended after the run above): `$NETEM remove`,
`scripts/nonetem.sh` on b0 (low contention + `scaling_data --ns 1,64,256 --ops READ,UPDATE`, 12 min), `$NETEM apply
420`, `$NETEM status`. Collect: `$X/scripts/pull.sh tcp`; stop: `neon.py $T stop`.

## 3. rdma arm (about 2 h 30 min with setup and the precheck)

Before it: no compute left running on b1-b7, and stale `/dev/shm/PostgreSQL.*` from killed computes deleted on those
nodes (`find /dev/shm -maxdepth 1 -user $USER -name 'PostgreSQL.*' -delete`, only where `pgrep -x postgres` is
empty). Netem stays applied.

```
T=scripts/setup/topologies/cloudlab_xl170_rdma.toml
uv run python scripts/setup/neon.py $T install --no-fanout
uv run python scripts/setup/neon.py $T start                     # storcon lists nodes as rdma:10.10.1.1x
uv run python scripts/setup/neon.py $T seed --scale 3            # 195 s
uv run python scripts/setup/neon.py $T env-init
uv run python scripts/setup/neon.py $T deploy-client
uv run python scripts/setup/neon.py $T clock-check
uv run python scripts/setup/neon.py $T smoke
scp $X/scripts/rdma_precheck.sh b0:/mnt/ssd/exp12/
$X/scripts/launch.sh exp12-rdma-precheck /mnt/ssd/exp12/rdma/superseded/precheck/run.out \
    bash /mnt/ssd/exp12/rdma_precheck.sh                         # 11 min: driver smoke + READ at N=1024
$NETEM status > $X/logs/netem_status_rdma_lfc_start.txt
$X/scripts/launch.sh exp12-rdma-lfc /mnt/ssd/exp12/rdma/lfc/run.out \
    scripts/workloads/run_all.sh --topology $T --backend neon --outdir /mnt/ssd/exp12/rdma/lfc   # 1 h 30 min
$X/scripts/pull.sh rdma $T
```

Per-node RDMA state was captured into `code/rdma_env/` (`rdma link`, `ibv_devinfo`, memlock of the services,
`rdma res`).

## 4. 2 pageservers (reads) / 2 safekeeper sets (writes), clients on 4-5 nodes (about 3 h for all four)

Needs the shard-routing fix in both builds (`code/neon_fork_tcp_shardfix.patch` on `/mnt/ssd/neon-src`, then the
pgxs make of `build/pgxn-v17` with `CC=clang` and a manual copy of `neon.so` into `pg_install/v17/lib/postgresql/`;
`code/neon_fork_rdma.patch` already contains it). Only one deployment can run at a time (all use the `neon-*` units):
wipe the previous one first.

```
for arm in rdma tcp; do                    # topologies cloudlab_xl170[_rdma]_{read2ps,write2sk}.toml
  P=$( [ $arm = rdma ] && echo cloudlab_xl170_rdma || echo cloudlab_xl170 )
  # read phase: 2 shards on a4/a5 (shard-split also compacts every shard and moves the branch point above the images)
  T=scripts/setup/topologies/${P}_read2ps.toml
  for s in "install --no-fanout" start "seed --scale 3" "shard-split --shards 2" env-init deploy-client smoke; do
    uv run python scripts/setup/neon.py $T $s; done
  uv run python scripts/workloads/multi_client/multi_scaling.py deploy --topology $T --clients b0,b1,b2,b3
  $X/scripts/launch.sh exp12-$arm-read2ps /mnt/ssd/exp12/$arm/read2ps/run.out uv run python \
      scripts/workloads/multi_client/multi_scaling.py run --topology $T --clients b0,b1,b2,b3 \
      --outdir /mnt/ssd/exp12/$arm/read2ps --ops READ,RANGE_READ          # 15-20 min
  uv run python scripts/setup/neon.py $T pull-archive /mnt/ssd/exp12/$arm/read2ps_archive
  uv run python scripts/setup/neon.py $T wipe --yes
  # write phase: safekeeper sets a1-a3 and a5-a7
  T=scripts/setup/topologies/${P}_write2sk.toml
  for s in "install --no-fanout" start "seed --scale 3" env-init deploy-client smoke; do
    uv run python scripts/setup/neon.py $T $s; done
  uv run python scripts/workloads/multi_client/multi_scaling.py deploy --topology $T --clients b0,b1,b2,b3,b4
  $X/scripts/launch.sh exp12-$arm-write2sk /mnt/ssd/exp12/$arm/write2sk/run.out uv run python \
      scripts/workloads/multi_client/multi_scaling.py run --topology $T --clients b0,b1,b2,b3,b4 \
      --outdir /mnt/ssd/exp12/$arm/write2sk --ops INSERT,UPDATE           # 17-20 min
  uv run python scripts/setup/neon.py $T pull-archive /mnt/ssd/exp12/$arm/write2sk_archive
  uv run python scripts/setup/neon.py $T wipe --yes
done
```

(`scripts/chain_phases.sh` ran the rdma write, tcp read and tcp write phases unattended in this order, with the
results rsynced from b0 to a0 afterwards.) The driver runs on b0 and starts `node_run.sh` on every client node over
ssh; `multi_scaling.py remerge` rebuilds the merged summaries from the per-node results.

## 5. Figures and checks

```
uv run python $X/scripts/plot.py        # figures/{low_load,scaling,branch_create,cold_start}.{png,csv}
```

`plot.py` reads `data/{tcp/lfc,rdma/lfc}` (and `data/{tcp,rdma}/{read2ps,write2sk}` for `scaling_2x2.png`) and the Neon local and Doltgres (10 cores) reference numbers from experiment 11's
`figures/*.csv` (`scripts/refs.py`).

Pass checks: every `progress.log` line `DONE ... ok` (tcp/lfc 46, rdma/lfc 46, tcp/lfc_nonetem 11; no `CHECK-FAIL`);
16 nodes with `delay 420us` in `logs/netem_status_*`; the fast (< 1 ms) share of low-contention point reads about 28%
(read from `data/<arm>/lfc/neon/low_contention/data_ops/READ/*_b1.parquet`).
