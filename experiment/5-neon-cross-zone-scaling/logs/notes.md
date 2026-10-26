# Experiment 5 run notes (UTC)

Incidents during the runs and what was changed, in order. Feeds the README's deviations and the report's caveats.

- 2026-10-22 22:3x. Binary fan-out over ssh hung (a backgrounded HTTP server kept the session open); finished by
  hand, script switched to a transient systemd unit.
- 22:5x. Memory probe: 102 MB per compute with the data directory on the tmpfs (36 MB LFC, a fully allocated 16 MB
  WAL segment, about 28 MB shared memory, about 30 MB private incl. compute_ctl 17.5 MB). Agent option
  `pgdata_on_disk` (data directory on the root HDD, LFC on the tmpfs) and max_connections 25: 83 MB per compute.
  Used for scaling and branch-creation runs; low load and cold start keep everything on the tmpfs.
- 23:4x. LFC contents probe (logs/lfc_contents_probe.json): after 20,000 reads the LFC holds 3,415 stock heap
  pages (20.0% of the heap), 1,021 of 1,099 stock_pkey pages, about 170 catalog pages; client-side hit share 19.3%.
- 2026-10-23 05:1x. Compute placement by timeline-id hash was uneven (b2 had 51 of 257 computes at N=256, mean 37).
  Shared change: numbered branches (`..._<i>`) map to agent i mod 7 (`timeline_id_for`). Applies from
  scaling/nolfc RANGE_READ N=256 on; at N=256 the nodes then held 36-38 computes each.
- 05:27. scaling/nolfc INSERT N=256 failed: client processes could not rebuild a multiprocessing semaphore.
  Cause: b0 had logind's RemoveIPC=yes (it is not an agent node), so every ssh logout removed the runner's POSIX
  semaphores. Fixed (RemoveIPC=no on b0, also in setup_neon_cluster.sh for client nodes). The sequence was stopped
  during UPDATE N=256 and restarted (completed runs skipped); partial files in data/superseded/nolfc_interrupted_2026-10-23/.
  All earlier runs were checked: every one has all N client threads. run_scaling.sh got a retry pass.
- 07:26-08:58. scaling/nolfc READ N=1024 hung: a few workers' statements never returned; killed at the 5,400 s run
  timeout (exit 137), no results. No OOM, no backend crash. Compute logs at high N carry many walproposer warnings
  "terminating connection to safekeeper ... no messages received during the last 10000ms" (0 at READ N=512, 234 at
  INSERT N=512, 1,986 at UPDATE N=512, about 8,600 at READ N=1024; reconnects succeed within one round trip).
  Mitigation 09:20: `ALTER ROLE bench SET statement_timeout = '600s'` on the seed, so a hung statement fails (and
  is counted) instead of hanging the run. Applies to runs whose root was created after 09:20 (from scaling/nolfc
  UPDATE N=1024 and the READ N=1024 retry on).
- 09:1x. Write runs' extra wall time (about 40 min at N=512) is the teardown: deleting timelines that took writes
  takes seconds each on the pageserver. Deletions of one tenant run concurrently in the storage controller, so
  `NEON_LOCAL_PURGE_PARALLEL` (shared change) is 64 from scaling/lfc25 on. Measurements are not affected.
- 10:2x. The client node ran out of memory during scaling/nolfc INSERT N=1024 (15.6 of 15.9 GB used, swap full):
  the 16 client processes held 2.56 M operations' result rows (about 1 GB each) after the timed phase. The
  READ N=1024 "hang" earlier was most likely the same (3.07 M reads; workers starved by swapping, not stuck in
  Neon). Fix: run_scaling.sh caps a run at 1,000,000 point/range reads and 640,000 writes (per-client counts
  from N=512 on: READ 1953/976, INSERT/UPDATE 1250/625 at N=512/1024). Stopped at 10:30, partial files in
  data/superseded/nolfc_client_oom_2026-10-23/, sequence relaunched (exp5-all3). The nolfc N=512 runs done before
  the cap keep their larger counts (3,000 reads, 2,500 writes per client); throughput and latency are steady-state
  rates, so the count only changes the window length.
- 11:2x. The coordinator's root filesystem (shared machine, 787 GB) was full: 300 MB free; other users' and older
  data, not this experiment's (8.4 GB). Archive pulls had silently truncated b2-b7's samples. Agents' 1 s samples
  are now pulled and kept gzipped (samples.jsonl.gz; the analysis scripts read either), and result parquet files
  above 20 MB are rewritten with zstd (scripts/recompress.py, lossless, verified by comparing the tables): 6 GB free.
- 15:4x. The default-LFC scaling runs used a prewarm that loaded the primary key first and the heap prefix second.
  The LFC evicts the least recently used page, so the first heap misses evicted index leaves: over a scaling run's
  short window (976-3,000 reads per client) 16% of point reads took two round trips (1.0-1.1 GetPage per read,
  against 0.85 at low load). Probe over 3,000 reads (logs/lfc_probe_{key,heap}_first.json): key first 14.3% hits,
  18.5% two round trips, 1,032 of 1,099 key pages resident; heap first 18.3% hits, 0% two round trips, all key
  pages resident. The prewarm now loads the heap first and the key last (common.sh, ceiling_check.py). The
  scaling/lfc25 runs done until then (N=1..1024 except UPDATE N=1024) moved to
  data/superseded/lfc25_prewarm_key_first; the variant was rerun (exp5-all4). Low load and cold start are not
  affected (low load ran 40,000 reads and reached the steady state; cold start does not prewarm); branch creation's
  background clients used the old order, which only changes their load slightly.
- 15:5x. Low load's 40,000 point reads are in the steady state throughout: every quarter has 20-21% hits and 5%
  two-fetch reads. The 5% is steady: the hot index leaves are hit in shared_buffers, which does not refresh their
  LFC entries, so they age out of the LFC and miss once shared_buffers evicts them. With the heap-first prewarm a
  scaling run's window starts with every leaf resident (0% two-fetch reads over 3,000 reads), about 0.82 GetPage
  per point read against 0.85 in the long-run steady state.
- 18:3x. At the user's request the two-pageserver variant runs a shorter sweep to save about 1.75 h: N = 1, 64,
  256, 512, 1024 (not 4, 16, 128) and point reads, range reads and updates (no inserts: 0.01 GetPage per insert, so
  the second pageserver cannot change them); its minimal-client check drops inserts too. Defaults in
  run_scaling.sh and ceiling_check.py for --variant lfc25_2ps.
- 19:5x. Pre-check for the shard split: the storage controller's reconciliation of the tenant was stuck (542
  consecutive launches) because every compute notification to the hook stub on a0 timed out. The stub (single-
  threaded, from experiments 3-4) had hung on one connection, its listen queue full. Branch creation and deletion
  do not depend on it, so the measured runs were not affected, but a shard split needs a successful notification.
  config/compute_hook_stub.py is now threaded with a 10 s per-connection timeout; restarted 19:53, the pending
  notification succeeded at once and the tenant shows neither reconciling nor pending.
- 20:1x-20:4x. Shard split (run_all step "split"): `shard-split --shards 2 --stripe-pages 256` took 92 s; shard
  0002 stayed on node 1 (a2), shard 0102 moved to node 2 (a5); data/seed/shard_split.json. env-init then wrote
  NEON_LOCAL_PS_OPTS (JSON) inside double quotes, which neither python-dotenv nor bash parse; the step's smoke test
  would not have passed the shards anyway. Stopped by hand, neon_ctl.set_env now single-quotes values with double
  quotes, .env rewritten. Sharded smoke instead: lfc_contents_probe.py on b2 with the shards (2,000 reads,
  logs/lfc_probe_2ps_smoke.json): 17.8% hits, 0.1% two round trips, GetPage requests split 3,432 on a2 / 3,444 on
  a5. Marked DONE in run_all.log; the two-pageserver variant launched alone (exp5-all5, steps scaling_lfc25_2ps).
