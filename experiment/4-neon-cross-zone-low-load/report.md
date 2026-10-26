# Experiment 4: Neon at low load with compute and storage in different zones

## Verdict

At low load, Neon's latency is set by the network between the compute and the storage layer. Every operation
pays a whole number of round trips to the pageserver or the safekeepers, and little else. The experiment ran two
setups on the same storage layer, the same 60-warehouse dataset and the same compute cache:

- **Two zones.** The compute and client are in Utah and the storage layer in Wisconsin, 38.9 ms apart. Medians
  are 8 to 29 times experiment 3's.
- **One zone.** Everything is in Wisconsin, as in experiment 3, but the compute and client reach the storage layer
  over the public network instead of the LAN. That path has a 0.18 ms round trip and 1 Gbit/s, against 0.13 ms and
  9.4 Gbit/s on the LAN. Medians are at most 0.1 ms above experiment 3's, and below them where the cache helps.

| Operation (median ms) | Experiment 3: one zone, LAN, no cache | One zone, public network | Two zones, WAN | Round trips paid in two zones |
|---|---:|---:|---:|---|
| Point read | 2.54 | 1.86 | 40.7 | one for the 84% of reads that miss the compute's cache; none for the other 16% |
| Range read, 100 rows | 7.02 | 6.93 | 201 | about 4.5 page fetches, one after another |
| Insert | 1.82 | 1.91 | 40.8 | one, the commit waiting for the safekeeper quorum |
| Update | 5.11 | 2.27 | 41.6 | one for the commit, plus one page fetch for 42% of updates |
| Branch create | 6.68 | 6.77 | 84.9 | two: the client opens a new connection to the storage controller per call |
| Compute start | 477 | 434 | 5,207 | about 130: startup, then each first connection reads catalog pages one at a time |

The storage layer's own work is the same in both setups. A GetPage takes 0.45-0.58 ms on the pageserver, which
is one SSD read. A WAL flush takes 0.54-0.57 ms on a safekeeper. A timeline create takes 5.7-6.8 ms in the storage
controller. In one zone that work is about half of every operation. In two zones it is lost next to the WAN. The
compute's local file cache serves a read in under 1 ms. It cannot help operations that must reach the storage
layer: commits always cross the network, and range reads fetch their pages one round trip at a time.

## Setup

| | |
|---|---|
| Storage layer (Wisconsin, c220g2, all data on SSD), shared by both setups | a2 pageserver; a0 storage controller, broker and safekeeper 1; a1, a3 safekeepers 2 and 3; a4 used only to load the data |
| Two-zone setup | b0 client and b1 compute node in Utah (r320; computes on a tmpfs, Utah has no SSD); compute and client reach the storage layer over the WAN (38.9 ms round trip) |
| One-zone setup | a5 client and a6 compute node in Wisconsin (computes on the SSD, as in experiment 3); compute and client reach the storage layer over the public network (0.18 ms, 1 Gbit/s), the client reaches the compute over the LAN (0.16 ms) |
| Spare for later scaling | a7 (SSD node), b2-b7 (compute nodes) |
| Dataset | CH-benCHmark with 60 warehouses: 30 M rows, 5.1 GiB in Postgres, 14 GB on the pageserver's SSD (447 GB) |
| Compute (both setups) | shared_buffers 128 MB, local file cache (LFC) 448 MB, effective_io_concurrency 2 |
| Cache design | The LFC holds the whole stock primary key (128.5 MiB) plus 15% of the stock heap (1996 MiB). Before each data-op run the client fills it with `pg_prewarm` (untimed), so the run starts in the cache's steady state. |
| Workload | Experiment 3's N = 1 point: one branch, one client, synchronous. 40,000 point reads, 10,000 range reads, 6,000 inserts, 6,000 updates, each after a 10% untimed warm-up. 100 branch creates; every fifth new branch is connected to, which starts its compute. |

Each bar is the median client latency, defined as in experiment 3. Data operations use SQL statement time,
branch create uses the storage controller call, and compute start uses the first connection to a new branch.
Network probes: `logs/network_probe.out` and `logs/network_probe_onezone.out`, kept on disk but ignored by git.

## Latency per operation

![Median latency per operation in experiment 3 and in both setups of this experiment](figures/low_load.png)

In two zones the latencies sit on round-trip multiples, and the spread within each step is small:

| Operation | Share of operations by WAN round trips (two zones) |
|---|---|
| Point read | 0: 16.2% (mean 0.9 ms), 1: 83.8% |
| Range read | 0: 8%, 1-4: 15%, 5: 54%, 6: 20%, 7-8: 3% |
| Insert | 1: 99.5% |
| Update | 1: 58%, 2: 42% |
| Compute start | 5.1 to 5.5 s for all 20 starts |

In one zone the same cache split shows up at a smaller scale. 16.2% of point reads finish in 0.7 ms from the
cache, and the rest in about 1.9 ms. Range reads run from 6.9 ms (p50) to 9.2 ms (p99).

## Where the time goes

![Mean latency per operation split into components, two zones on the left, one zone on the right](figures/low_load_breakdown.png)

Mean ms per operation, from `figures/breakdown.csv` (two zones) and `figures/breakdown_onezone.csv` (one zone).
In every bar the components add up to the client's mean latency. "Network" is the WAN in two zones and the
public network in one zone.

| Component (two zones / one zone) | Point read | Range read | Insert | Update |
|---|---:|---:|---:|---:|
| Client to compute on the LAN, and the driver | 0.58 / 0.45 | 0.94 / 0.78 | 0.62 / 0.50 | 0.64 / 0.50 |
| Parse, plan, protocol (reads) | 0.22 / 0.20 | 0.19 / 0.22 | | |
| Commit: network to the safekeepers | | | 39.58 / 0.85 | 40.02 / 1.09 |
| Commit: safekeeper WAL flush on SSD | | | 0.57 / 0.54 | 0.69 / 0.62 |
| Executor CPU | 0.10 / 0.07 | 0.77 / 0.59 | 0.20 / 0.05 | 0.14 / 0.10 |
| LFC reads | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| GetPage: network and compute side | 32.87 / 0.52 | 174.74 / 2.45 | 0.07 / 0.00 | 16.48 / 0.23 |
| GetPage: pageserver | 0.48 / 0.45 | 2.58 / 2.32 | 0.00 / 0.00 | 0.26 / 0.23 |
| **Mean total** | **34.26 / 1.70** | **179.22 / 6.37** | **41.05 / 1.94** | **58.24 / 2.77** |
| GetPage requests per operation | 0.84 / 0.84 | 4.46 / 4.44 | 0.00 / 0.00 | 0.42 / 0.41 |

| Branch create (two zones / one zone) | ms |
|---|---:|
| Client to storage controller over the network | 78.92 / 1.33 |
| Storage controller | 2.62 / 2.06 |
| Pageserver timeline create (called by the storage controller) | 4.16 / 3.62 |
| **Mean total** | **85.70 / 7.02** |

| Compute start (two zones / one zone) | ms |
|---|---:|
| Client's first connection to `bench`: catalog reads over the network | 2,182 / 94 |
| Agent's readiness check, a first connection to `postgres`: catalog reads over the network | 2,325 / 95 |
| compute_ctl: other steps (the sync-safekeepers check and the rest) | 104 / 4 |
| compute_ctl: start Postgres | 53 / 60 |
| compute_ctl: basebackup (82 KB from the pageserver) | 306 / 147 |
| compute_ctl: sync safekeepers | 267 / 36 |
| **Mean total** | **5,236 / 436** |

What each bar shows:

- **Point read.** 84% of reads need one heap page from the pageserver; the other 16% find it in the compute's
  caches. The share matches the cache design: 15% of the heap in the LFC, plus a little in shared_buffers. In
  two zones a miss costs one round trip plus 0.58 ms of pageserver time. In one zone the pageserver's 0.45 ms is
  the largest part of the read: each GetPage is one read from its SSD, which stays 98% busy serving this single
  client one request at a time.
- **Range read.** A 100-row range looks up about 6 pages in the LFC; 4.4-4.5 of them come from the pageserver,
  and the waits do not overlap. Prefetch is limited to 2 requests in flight (experiment 3's setting), so the
  reads queue one round trip after another. In one zone that costs 4.8 ms per range and in two zones 177 ms.
  A deeper prefetch would cut it.
- **Insert.** The pages being written stay in the compute's cache, so the whole cost is the commit. The
  walproposer waits for 2 of 3 safekeepers to flush the WAL: one round trip plus a 0.55 ms SSD flush. That is
  1.4 ms in one zone, close to experiment 3's 1.82 ms median, and 40 ms in two zones.
- **Update.** Every update pays the commit. Half of the updates pick a fresh random row, and 85% of those miss
  the cache and fetch the page first: 0.42 fetches per update. In one zone that gives 2.3 ms (p50) against
  experiment 3's 5.1 ms, where every update fetched its page without a cache. In two zones it gives one round
  trip for 58% of updates and two for 42%.
- **Branch create.** The storage controller needs 5.7-6.8 ms, including the pageserver's timeline create. The
  rest is the client's call. The benchmark's Neon backend opens a new TCP connection for every
  storage-controller call: 1.3 ms in one zone, and two round trips (79 ms) in two zones. With a kept-alive
  connection it would be one round trip, about 46 ms in two zones.
- **Compute start.** In one zone a start takes 436 ms, close to experiment 3's 477 ms. compute_ctl takes 247 ms,
  of which the basebackup takes 147 ms. That is longer than experiment 3's 67 ms on the same kind of path, so
  it grows with the dataset, not the network. The two first connections take 95 ms each. In two zones the same
  steps take 5.2 s. Syncing the safekeepers takes 267 ms and the basebackup 306 ms, several round trips each.
  The first two connections take 2.2-2.3 s each: a new backend reads the catalog pages of its database from the
  pageserver, about 50 of them, one at a time. The agent's readiness check pays this for database `postgres`,
  and the client's first connection pays it again for `bench`. A probe on a fresh two-zone compute measured the
  client's part at 1.85 s to connect plus 0.2 s for the first query, with 108 GetPage waits
  (`logs/connect_probe.out`).

## Caveats

- **The public network adds little inside one site.** The public path's round trip is only 0.05 ms longer
  than the LAN's, and its 1 Gbit/s was not a limit for one client. The one-zone setup therefore shows the cost of
  the storage layer and the compute cache at near-LAN distance, rather than a measurable network penalty.
- **LFC chunk size.** The LFC uses 8 kB chunks instead of Neon's default 1 MB (128 pages). With uniform random
  reads over a table 4 times the cache, 1 MB chunks would each hold a few pages, and the hit ratio would fall to
  a few percent. Page-sized chunks give the 15% hit ratio the design asked for.
- **The cache is filled before each run.** Every run starts a fresh compute with an empty LFC, so the client
  fills it untimed (shared code change 2 in the README). Without that, a 40,000-read run would see a hit ratio
  rising from 0 to about 15%.
- **The LFC lives on different media.** It is on a tmpfs in two zones (Utah has no SSD) and on the SSD in one
  zone (as in experiment 3). LFC read waits round to 0.00 ms per operation in both, so this does not show.
- **Experiment 3 is not like for like.** Experiment 3 had no LFC, a 1-warehouse dataset and 1 MB of
  shared_buffers, so every read there was a GetPage at 0.6 ms. The one-zone setup isolates what the cache and
  the larger dataset change at near-LAN distance. The two-zone setup differs from it only in the network.
- **Measurement overhead.** The measured compute logs every statement's duration, and the agent queries its
  counters once a second. That is microseconds per statement and about 9 small queries per second.
- **Short windows in one zone.** One-zone runs are fast: 27 s for inserts and 9 s for branch creates. The
  1-second counter samples are interpolated at the window edges, so their per-operation means carry a few
  percent of error there. Client and log-based components are exact.
- **One run per operation.** Distributions are tight: the point-read p95 is 0.1-0.2 ms above its median in both
  setups, and all 20 two-zone compute starts fall between 5.1 and 5.5 s. Run-to-run variance is small next to the
  effects.
- **The WAN is Internet2, not a cloud backbone.** The 38.9 ms round trip is stable, with no loss in the probes.
  Its single-stream bandwidth (60-600 Mbit/s) did not limit these single-client runs.
- **Split points.** "GetPage: network and compute side" is the compute's wait minus the pageserver's processing
  time, so it includes the compute's own protocol handling. That is about 0.3 ms in one zone, with the network's
  0.18 ms round trip. The commit's network share is the commit wait minus the median safekeeper's flush time.

## Data quality

- **Statement counts match.** In both setups every client statement has a matching duration line in the
  compute's log: 40,000, 10,000, 6,000 and 6,000.
- **Breakdowns add up.** Components sum to the client mean for every bar. In each setup, 20 of 20 compute starts
  were matched to compute_ctl's startup metrics.
- **Clocks.** Agent clocks were within 6 ms of the two-zone client, measured over a 40 ms round trip, and within
  0.2 ms of the one-zone client. Windows last 9 s to 30 min, with 1 s counter samples.
- **No failures.** No operation failed in either setup, and the background clients during branch create had no
  failures.
