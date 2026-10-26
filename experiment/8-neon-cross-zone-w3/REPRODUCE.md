# Reproducing experiment 8

Experiment 8 runs on experiment 5's cluster with experiment 5's procedure; read
`../5-neon-cross-zone-scaling/REPRODUCE.md` for the machines, software, deployment, parameters and pitfalls.
Only the differences and the commands actually run are listed here. `X=experiment/8-neon-cross-zone-w3`.

## Differences from experiment 5

| | Experiment 5 | Experiment 8 |
|---|---|---|
| Dataset | W = 4: stock 400,000 rows, heap 17,032 pages, stock_pkey 1,099 pages | W = 3: stock 300,000 rows, heap 12,774 pages, stock_pkey 825 pages (database about 0.27 GiB) |
| LFC prewarm | heap pages 0-3,399, then the key | heap pages 0-3,673 (`PREWARM_HEAP_BLOCKS` = 4,608 - 825 - 109), then the key |
| Heap share of the LFC (experiment 4's measure) | 20% | 28% |
| Variants | `nolfc`, `lfc25`, `lfc25_2ps` | `lfc`, `lfc_2ps` (no no-LFC sweep) |
| Statement timeout | set by hand before the scaling runs | set by `neon_ctl.py seed` (`ALTER ROLE bench SET statement_timeout = '600s'`) |
| Tenant | `490d45eece1d4d08aa89462e927b90a7` (W = 4), split into 2 shards | new tenant `625258ca0fcb429fabea4ca7adc237a5` (W = 3), split into 2 shards at the `split` step |

Everything else (shared_buffers 8 MB, LFC 36 MB in 8 kB chunks, op counts and caps, N lists, minimal client,
stripe 256 pages, compute placement, purge parallelism) is unchanged; the shared code is the same patch
(`code/shared_code.patch` on commit `f004d9f`).

## Commands, in order (25 Oct 2026, UTC)

```bash
# 0. cluster from experiment 5 still running (setup_neon_cluster.sh --status); free the coordinator's disk first
#    (about 8 GB of results). Pause pageserver 2 so the new tenant lands on node 1, and keep experiment 5's agent
#    archives apart:
curl -X PUT http://128.105.145.147:1234/control/v1/node/2/config -H 'Content-Type: application/json' \
     -d '{"node_id":2,"scheduling":"Pause"}'
source $X/scripts/cluster.env
for n in $AGENT_NODES; do h=HOST_$n; ssh $SSH_USER@${!h} 'mv ~/neon-archive ~/neon-archive-exp5 && mkdir -p ~/neon-archive'; done
$X/scripts/sync_client.sh b0 a4

# 1. seed on a4 (192 s): W = 3, pinned to node 1 with scheduling Pause
ssh a4 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --zone a && uv run python $X/scripts/neon_ctl.py seed --scale 3 --pageserver-node 1'
#   pass: stock 300,000 rows; heap 12,774 pages, stock_pkey 825 pages; Image layers > 0; SHOW statement_timeout = 10min as bench
scp a4:db-fork/$X/data/seed/seed.json $X/data/seed/ && ssh b0 "cd db-fork && cat > $X/data/seed/seed.json" < $X/data/seed/seed.json
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --from-seed-json $X/data/seed/seed.json && uv run python $X/scripts/neon_ctl.py clock-check && uv run python $X/scripts/neon_ctl.py smoke'

# 2. all measured runs, as one transient systemd unit on b0 (logs/remote_b0/run_all.log)
$X/scripts/launch.sh exp8-all $X/logs/run_all_driver.out $X/scripts/run_all.sh
#   low_load 74 min, cold_start 49 min, branch_create 43 min,
#   scaling_lfc 3.9 h + ceiling_lfc 22 min,
#   split 3 min (purge, shard-split --shards 2, env-init, sharded probe on b2),
#   scaling_lfc_2ps 2.2 h (N = 1, 64, 256, 512, 1024; no inserts) + ceiling_lfc_2ps 26 min
#   total 9.8 h; pass: every step DONE, no FAILED/RETRY line in any progress.log

# 3. collect, analyse, plot (coordinator, about 30 min)
$X/scripts/pull_data.sh                                            # 4.6 GB here
uv run python $X/scripts/breakdown.py --offsets $X/data            # figures/breakdown.*
uv run python $X/scripts/plot_low_load.py                          # low_load.png (means: experiments 3, 4, 8), breakdown, hit/miss
uv run python $X/scripts/plot_scaling.py                           # throughput_latency.png (experiment 5 dashed), data_ops.png
uv run python $X/scripts/resource_use.py                           # scaling_bottleneck.png, resource_use.csv
uv run python $X/scripts/plot_branch_create.py
uv run python $X/scripts/plot_cold_start.py
```

The plot scripts read experiment 5's results from `../5-neon-cross-zone-scaling/` for the reference lines and bars.

## Checking the shard split

Which shard a page goes to: `hash_combine(murmurhash32(relfilenode), murmurhash32(block / stripe)) % shards`
(`pgxn/neon/libpagestore.c`, `get_shard_number`). The stock relfilenode is 16464 (`SELECT relfilenode FROM
pg_class WHERE relname = 'stock'` on a compute of the seed timeline); with stripes of 256 pages, 61.6% of heap
blocks 3,674-12,773 map to shard 1, as the pageservers' GetPage counters show (`figures/resource_use.csv`).

## Cleanup

```bash
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py purge'   # computes and timelines except the seed
```
