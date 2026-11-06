# Experiment 13 report: branching by copying Postgres data directories to other nodes (`pgcopy`)

5 Nov 2026, 16 xl170 nodes at CloudLab Utah on one 25 GbE LAN; n0 holds the seed and every run root, n1-n15 host
the branches with their clients. Setup, placement and code are in `README.md`, commands in `REPRODUCE.md`. One run
per point (the workloads' own windows); medians unless noted. Dataset: CH-benCHmark W = 3 (283 MiB `bench` database,
300,000 stock rows); each branch has 40 MB of shared_buffers, prewarmed like Neon's computes.

## Summary

- **A fork costs 1.6-1.8 s for this 283 MiB database, and nearly all of it is the copied data reaching the disk.**
  On an idle cluster `pg_basebackup` copies 322 MB in 0.62 s (520 MB/s: `--no-sync` leaves it in the page cache),
  then starting the copy takes 1.07-1.17 s. A copied directory starts as crash recovery, and PostgreSQL first fsyncs
  the whole data directory: 322 MB at the SSD's ~350 MB/s is about 0.95 s. The fork's cost therefore grows with the
  database size over the disk's write bandwidth, not with the network (25 GbE carried the copy in under 0.2 s).
- **Data operations at low load are local-Postgres fast:** point read 0.46 ms, 100-row range read 1.14 ms, insert
  0.74 ms, update 0.89 ms (p50, one branch, one client on the same node over loopback, fsync and synchronous commit
  on).
- **Throughput grows with N until the hosts are full, then stays flat.** With N busy branches spread over the 15
  hosts (throughput = sum of the nodes' own rates), it peaks at N = 256 to 512: 553k point reads/s, 208k range
  reads/s, 337k inserts/s, 268k updates/s, and stays within 9% of that up to N = 1,024 while latency rises. Reads stop
  at the hosts' CPUs (from N = 512, 17-19 of 20 hardware threads busy on the median node, 10 physical cores), writes at the SSDs
  (15-20k synchronous writes/s per node, the disk 84-88% busy).
- **The cap at N = 256 is the Python clients' CPU, at about one client per hardware thread.** Each busy branch is
  one client thread with one request in flight, served by one backend process of the branch's own instance. Per
  node, throughput grows until those pairs fill the 20 hardware threads (17 per node at N = 256) and is flat after.
  The client costs most of that CPU: ~270 us of hardware-thread time per point read, Postgres ~100 us. pgbench (a C
  client) with the same unprepared `SELECT *` on one node does 118k reads/s with 17 clients (131k at its peak)
  against the runner's 38k; with prepared statements 304k. The ceiling check's asyncio client is Python too
  (~430 us per read) and gets only 1.1-1.4x the runner (updates 0.85-0.89x). These curves therefore measure how many
  Python clients fit on a node, not what pgcopy's instances can serve.
- **One node (n4, hp091) has faulty memory**: about 1 GiB/s of memory copy bandwidth against 6.4-7.2 GiB/s on the
  other 15 (measured after the run). It falls behind the others whenever the hosts are full (at N = 1,024 its rate
  was 25-56% of the median node's). It is in the sums with its own rate, so the sums are up to 5% below what 15
  healthy hosts would give.
- **Busy branches elsewhere barely slow a fork; busy branches on the forking node do.** Spread over the 15 hosts,
  the create p50 grows from 1.63 s (N = 1) to 1.87 s (N = 128) and 2.42 s (N = 512, of which 35 busy branches are on
  the creating node). With all busy branches on the creating node it grows faster: 1.81 s at N = 1, 2.58 s at
  N = 64, 2.91 s at N = 128 (1.56x the spread value at the same N). In this design the fork competes only with the
  load on its own node (and with other copies from the seed node, which a timed create never had).
- **No cold start to speak of:** a fresh branch serves point reads at 0.20-0.26 ms from its first read (median of
  the first 100: 0.22 ms). Its shared_buffers are empty, but the copy has just put every file in the OS page cache.
  The fork itself (1.70 s) is the whole cold start.
- **Copying is the scarce resource at scale.** The run made 12,670 branch copies (4.56 TiB). With 16-63 other
  copies running the median fork took 7.3 s, with 64 or more 28.3 s (setup phases, all copies from one seed node:
  its NIC tops out at 23.9 Gbit/s, the hosts' disks at ~350 MB/s). Every branch is a full 322 MB copy: N = 1,024
  busy branches hold 330 GB, against shared pages for Neon or Doltgres.

For orientation only (other hardware and datasets, numbers quoted from experiment 12's `report.md`; its data is not
on this machine): Neon on the same type of cluster created a branch in 7-8 ms and started a compute in 269-356 ms,
and served low-load point reads in 1.5-2.8 ms. pgcopy's fork is about 5x Neon's create + compute start; its data
operations are 3-6x faster at low load, because client and data sit on one node.

## Low load: one busy branch, one client

![low load](figures/low_load.png)

| Operation | p50 | p95 | Statements |
|---|---|---|---|
| Point read | 0.46 ms | 0.56 ms | 40,000 |
| Range read (100 rows) | 1.14 ms | 1.18 ms | 10,000 |
| Insert | 0.74 ms | 0.80 ms | 6,000 |
| Update | 0.89 ms | 0.97 ms | 6,000 |
| Branch create (copy + start; 1 MIXED busy branch) | 1.60 s | 1.64 s | 100 |

The client is co-located with the branch (127.0.0.1), so no network is on the data path. Inserts and updates commit
with fsync on: the SATA SSD's flush costs a few hundred microseconds.

## Scaling with N busy branches spread over 15 hosts

![scaling](figures/scaling.png)

Each point: N branches split over n1-n15 (N < 15 uses N hosts), one client per branch on the branch's node,
min(20, branches) client processes per node, every node's timed window started together. Throughput = the sum of
each node's own rate (its operations / its own window). Experiment 12's rule, all operations / the longest node
window, is not used: one faulty node (n4, below) stretched the longest window up to 4x, which put that number up to
74% below the per-node sum (range reads at N = 1,024) and made it fall past N = 256 while every other node held its
rate. It is kept in `figures/scaling.csv` (`throughput_slowest_window`) for reference.

| N | Point read k/s | Range read k/s | Insert k/s | Update k/s | Point read p50 |
|---|---|---|---|---|---|
| 1 | 1.7 | 0.8 | 0.8 | 0.7 | 0.35 ms |
| 16 | 25.3 | 11.3 | 12.3 | 11.1 | 0.36 ms |
| 64 | 99.0 | 43.3 | 47.2 | 45.4 | 0.36 ms |
| 128 | 185.9 | 77.1 | 91.7 | 88.3 | 0.41 ms |
| 256 | 553.3 | 208.1 | 287.1 | 256.0 | 0.26 ms |
| 512 | 534.1 | 192.8 | 337.5 | 267.7 | 0.71 ms |
| 1,024 | 510.0 | 193.7 | 308.5 | 267.2 | 1.21 ms |

Per node (median of the 15) at N = 256 / 512 / 1,024, from the summaries' CPU and disk samples:

| | Point reads | Range reads | Inserts | Updates |
|---|---|---|---|---|
| Rate per node (k/s) | 37.9 / 36.2 / 35.0 | 14.2 / 13.5 / 13.5 | 18.8 / 23.0 / 21.0 | 17.3 / 18.3 / 18.7 |
| Busy hardware threads (of 20) | 14.4 / 17.2 / 17.9 | 11.8 / 17.3 / 18.7 | 12.0 / 16.4 / 16.7 | 12.3 / 13.6 / 15.9 |
| of which client / postgres (N = 256 ... 1,024) | 10.1 / 3.7 ... 10.2 / 4.1 | 9.6 / 1.7 ... 10.2 / 5.6 | 7.9 / 3.1 ... 10.1 / 3.6 | 7.5 / 4.1 ... 9.2 / 5.3 |
| SSD writes/s, MB/s | - | - | 15-20k, 134-172 MB/s | 15-17k, 213-259 MB/s |

- **Reads stop at the hosts' CPUs, and the client is most of it.** An xl170 has 10 physical cores (20 hardware
  threads). Each busy branch is one client thread (one request in flight, its own psycopg2 connection) and one
  backend process on the branch's own instance, so N = 256 puts 17 such pairs on each node, about one per hardware
  thread. From there the median node has 14-19 threads busy and its rate stays flat while p50 grows (point reads 0.26
  to 1.21 ms): more busy branches only queue. Per point read the node spends 380 us of hardware-thread time at
  N = 256 (Python client 266 us, postgres 98 us), 475-511 us at N = 512-1,024 (threads sharing cores). The runner's
  client processes use 1.8-5.6x the CPU of all the node's instances together. From N = 512 the 20 client processes
  per node each drive several branches.
- **What one node can serve (pgbench after the run, `logs/node_probes_after_run_2026-11-05.txt`).** pgbench on n0
  against the warm seed instance over loopback, one client = one connection = one backend as above, same point read
  as the runner (unprepared `SELECT *` on the stock key): 81k reads/s with 10 clients, 118k with 17, 131k with 20
  (152 us per read, client included), flat beyond (128k with 40). With prepared statements and one column: 304k at
  20 clients (66 us per read). Same knee at clients = hardware threads, at 3.1x the runner's rate for the same 17
  clients. Caveat: pgbench's clients shared one instance (one copy of the stock pages in 1 GB of shared_buffers);
  pgcopy's 17-68 instances each keep their own 40 MB copy, which costs CPU cache, but their measured postgres share
  (~100 us per read) is within pgbench's total.
- **Writes stop at the SSDs.** Every commit waits for its WAL flush (synchronous_commit on), and 17-69 separate
  instances cannot share a group commit: each node's disk is 84-91% busy from N = 128 on (15-20k writes/s from N = 256),
  updates write 213-259 MB/s (full-page images) against the SATA SSD's ~350 MB/s, and the CPUs are not full.
- **n4 (hp091) has faulty memory.** It was the slowest node at every N >= 256 (up to 4x the median node window, range
  reads at N = 1,024: 12.2 s against 3.0 s) with its CPUs as busy as the others', i.e. it got 1.6-4x less done per
  CPU-second. A check after the run (`dd` from /dev/zero to /dev/null with 64 MB blocks, the copy goes through DRAM):
  n4 1.0 GiB/s with one stream and 0.9 GiB/s with four, the other 15 nodes 6.4-7.2 and 17.3-18.8 GiB/s. Its CPU
  speed is normal (same clock under load, same `openssl speed` throughput, no thermal throttle counts), as are its
  DIMMs (4 x 16 GB at 2,133 MT/s like the others) and EDAC counters (0). Range reads move the most memory and suffer
  most (n4 at N = 1,024: 3.4k/s against the median 13.5k/s); point reads least. At N <= 128 n4 kept pace, so either
  the fault only shows when many processes compete for memory, or it started during the day; the data cannot tell.
  The sums above include n4 at its own rate (up to 5% below 15 healthy hosts).
- The timed windows are short (1-4 s at N >= 16): the per-client counts are experiment 5's, sized for slower
  systems; every point still holds 12,000-1,000,000 statements.
- Point-read p50 dips at N = 256 (0.26 ms against 0.41 ms at N = 128), and with it throughput grows 3x from
  N = 128 to 256 for 2x the branches (per node: 9 to 17 clients, 12.4k to 37.9k point reads/s); a likely cause is
  the CPUs staying at high clock when every core is busy (governor `schedutil`), not verified.

## Ceiling check: the asyncio client

![ceiling](figures/ceiling.png)

| N | Point read k/s | Range read k/s | Insert k/s | Update k/s |
|---|---|---|---|---|
| 128 | 410 | 152 | 113 | 151 |
| 256 | 612 | 236 | 329 | 222 |
| 512 | 693 | 242 | 387 | 237 |
| 1,024 | 692 | 234 | 427 | 228 |

Same load shape (one connection per branch, one request in flight) without the runner's bookkeeping, on the same
nodes, for 30 s per point; throughput is the sum of the nodes' rates, the squares are the runner's (scaling table).
At N = 256-1,024 the minimal client gets 1.11-1.36x the runner's point reads, 1.13-1.25x its range reads and
1.15-1.38x its inserts. It is not a light client: it is Python as well (psycopg, asyncio), at ~430 us of
hardware-thread time per point read (client ~260, postgres ~170) against the runner's 380 us, and its point reads
stop at about 46k/s per node with the nodes' threads 98% busy (295 of 300). pgbench's 131k reads/s per node with the
same query (Scaling, above) is the better reference for what the instances can serve. Updates are the exception: the minimal client gets 0.85-0.89x the runner, its 30 s windows
against the runner's 2-4 s; both are at the SSDs' write limit, and a longer window likely includes more checkpoint
and full-page-image writes (not verified). n4 is in these sums too (ceiling at N = 1,024, range reads: n4 3.2k/s, the others
16.3-16.8k/s).

## Branch creation under load

![branch create](figures/branch_create.png)

100 creates on n1 (every 5th connected), while N busy branches run MIXED (80% point reads, 20% updates).

| N | Spread over 15 hosts: p50 / p95 | (busy on n1) | One host: p50 / p95 |
|---|---|---|---|
| 1 | 1.63 / 1.77 s | 1 | 1.81 / 1.92 s |
| 4 | 1.65 / 1.76 s | 1 | 1.79 / 1.88 s |
| 16 | 1.70 / 1.80 s | 2 | 2.14 / 2.90 s |
| 64 | 1.79 / 1.91 s | 5 | 2.58 / 2.94 s |
| 128 | 1.87 / 2.06 s | 9 | 2.91 / 3.41 s |
| 256 | 2.25 / 2.79 s | 18 | |
| 512 | 2.42 / 2.88 s | 35 | |

The spread series' background ran 0.6k (N = 1) to 508k (N = 256) operations/s cluster-wide. The create latency
follows the busy branches on the creating node rather than N: 35 busy on n1 (spread N = 512) gives 2.42 s, 64 busy on
n1 (one host, N = 64) 2.58 s. At N = 1 the two series differ by 0.18 s with the same load; that is the run-to-run
spread of an idle fork on n1 here (1.60-1.81 s across low contention, the two branch series and cold start).

## Cold start

![cold start](figures/cold_start.png)

| Run | Create (copy + start) | Copy | Start | First connect | Reads 1-100 (median) | Last 5,000 (median) |
|---|---|---|---|---|---|---|
| 1 | 1,706 ms | 627 ms | 1,068 ms | 21 ms | 0.22 ms | 0.26 ms |
| 2 | 1,702 ms | 619 ms | 1,071 ms | 20 ms | 0.22 ms | 0.26 ms |
| 3 | 1,711 ms | 605 ms | 1,094 ms | 23 ms | 0.23 ms | 0.26 ms |

30,000 point reads on a fresh branch, on its own connection (no runner: 0.2-0.26 ms against the runner's 0.46 ms
at low load). There is no warm-up curve: the first reads are as fast as the last, because the copy has just written
every page into the OS page cache. Latency rises slightly over the first ~6,000 reads (0.22 to 0.26 ms), the opposite
of a cache warming up; not investigated. A branch whose files are no longer in the page cache would read from disk;
in this design a branch is used right after its copy.

## Where a fork's time goes

![fork phases](figures/fork_phases.png)

From the agents' record of every copy (`archive/<node>/forks.jsonl`; the superseded untrimmed attempt excluded), by
the number of other copies running at the same moment anywhere:

| Other copies | Forks | Copy p50 | Start p50 | Copy + start p50 |
|---|---|---|---|---|
| 0 | 1,684 | 0.62 s | 1.17 s | 1.79 s |
| 1-3 | 311 | 1.64 s | 2.50 s | 4.94 s |
| 4-15 | 740 | 3.46 s | 3.13 s | 7.16 s |
| 16-63 | 8,352 | 5.46 s | 1.72 s | 7.35 s |
| 64+ | 1,583 | 21.6 s | 3.17 s | 28.3 s |

(The figure stacks the two phases' medians; the last column is the median of each fork's sum.)

Concurrent copies share the seed node's NIC and disk reads and the target's disk writes; their start then waits for
its own fsync behind the others' writes. Run roots (seed to root, on n0, up to 15 at once) took 19.9 s each (p50):
15 x 417 MB of fsync on one disk. Only the setup phases ever ran copies concurrently; every timed create ran alone.

## Validity

- **Disk state matters more than load.** Without trims the SSDs slowed after a few hundred GB of copies (idle fork
  start 1.1 s to 3.3 s on n1). The agents trimmed before every point from 12:10 UTC, and the branch-creation results
  are from a rerun after that; the low-contention, scaling and ceiling runs predate the slowdown (n1's idle forks
  started in 1.08 s at 10:16-10:19). The untrimmed branch series is kept as superseded (README).
- **Fork cost scales with database size.** One size was measured (283 MiB). A fork's copy and fsync are both
  proportional to the data directory, so a 3 GB database would cost about 10x (not measured: plan's optional
  `seed --scale 30`).
- **Durability.** Instances run with fsync and synchronous_commit on, and a copied instance's start syncs its
  directory. With fsync off PostgreSQL skips that sync, which should remove most of the start time (not measured;
  such an instance is no longer crash-safe).
- **Client side.** Multi-node points and pgcopy's branch runs use experiment 12's `fast_client` patches (outside
  the latency timer). The low-contention, cold-start and single-node data runs use the plain runner. Background
  operations in the branch runs were recorded 1 in 20 (memory); their throughput is the sampled count x 20.
- **Placement.** Clients share their node with their branches' instances, so node CPU counts both (the summaries'
  `remote_cpu.computes` is the instances' share). The seed node only served copies and run roots (0.04-0.36 node cores in
  timed windows).
- **Copies use temporary replication slots** after the first ceiling attempt failed on removed WAL; the creates
  rerun with slots matched the earlier ones (1,576 against 1,582 ms).
- **Clocks** agreed within 9 ms (only the ceiling check's start uses them; the other multi-node points start on a
  GO message). Netem: none on any interface.
- **Not compared here:** Doltgres, Neon local and Neon TCP/RDMA (experiments 7, 11, 12), whose data is not on this
  machine; the result files keep experiment 12's layout for a later joint plot.
