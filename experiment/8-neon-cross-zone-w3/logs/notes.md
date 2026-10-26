# Experiment 8 run notes (UTC)

- 2026-10-25 05:2x. Experiment directory from experiment 5's scripts (variants renamed lfc / lfc_2ps); W=3;
  pageserver node 2 paused; agents' experiment-5 archives moved to ~/neon-archive-exp5 on every node.
- 05:3x. Seed on a4: tenant 625258ca0fcb429fabea4ca7adc237a5, seed timeline a51fdec5a4c14654a45f3607aaa912b3,
  300,000 stock rows, heap 12,774 pages, stock_pkey 825 pages, 192 s; the seed now sets the bench role's
  statement_timeout (checked: 10min). PREWARM_HEAP_BLOCKS = 4,608 - 825 - 109 = 3,674. Smoke test passed.
- 05:36. run_all launched on b0 (exp8-all): low_load, cold_start, branch_create, scaling_lfc (+ check), split,
  scaling_lfc_2ps (+ check).
- 06:50-08:22. Low load done (74 min), cold start (49 min), branch creation (43 min); no failures. One of the
  40,000 low-load point reads is missing from the compute's statement log (39,999 rows).
- 12:18. Default-LFC sweep done (3.9 h), 32 runs, 0 failed ops. Point reads peak at 17,650/s at N = 1,024
  (p50 65 ms; experiment 5: 16,300/s). Writes client-limited from N = 256 as in experiment 5. ceiling_lfc started.
- 12:41. ceiling_lfc done (22 min), 0 errors: point 18,238/s at N = 1,024 (p50 64.6 ms), insert 16,530/s,
  update 7,691/s, range 3,746/s. Split started.
- 12:44. Split done (190 s): shards 0002 on node 1 (a2), 0102 on node 2 (a5), stripe 256. Sharded probe on b2:
  2,000 reads, 29.0% hits, 0.05% two-round-trip reads. scaling_lfc_2ps started.
- 14:57. Two-pageserver sweep done (2.2 h), 15 runs, 0 failed ops; N = 1,024 point reads 22,401/s (client-limited).
- 15:23. ceiling_lfc_2ps done (26 min), 0 errors: point 30,894/s at N = 1,024 (p50 41.0 ms), range 6,342/s
  (p50 199.7 ms), update 7,549/s. run_all finished; no step failed or was retried.
- 15:25-15:45. pull_data (4.6 GB; gzip warned that the agents' samples files grew while being read: the agents keep
  sampling, the copies end a few seconds after the runs). Data quality: 59 runs complete, 0 failed ops, 20
  duplicate-key inserts. Two-pageserver GetPage split 39-44% / 56-61%: checked with Neon's get_shard_number on
  stock (relfilenode 16464, read from a seed-timeline compute started and stopped for it): 61.6% of the uncached
  heap maps to shard 1 (a5); experiment 5's heap gives 52%, as measured there.
- 15:55. Purged (no computes, only the seed timeline); services left running; node 2 Active.
