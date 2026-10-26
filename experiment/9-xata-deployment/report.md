# Xata: architecture and deployment on CloudLab

Experiment 9, 25 Oct 2026. Source: the `3rdparty/xata` submodule at commit `f197e7b` (Xata's open-source
mirror, synced from their internal repo at `0fd70b5`). Smoke test on node a6 (c220g2, Wisconsin).

## Summary

- **Architecture.** Xata is a control plane, a SQL gateway and operators on top of Kubernetes and a CNPG
  fork. Every branch is a separate, unmodified Postgres pod on its own volume. A child branch is a CSI
  VolumeSnapshot of the parent's volume, cloned into a new volume, on which a new Postgres does crash
  recovery. The copy-on-write layer is not Xata's code: in the open-source build it is whatever CSI
  driver you install. Xata's own NVMe-oF storage engine (Xatastor) and its pre-warmed cluster pools are
  closed source, so the open-source build always takes the "snapshot + new pod" path.
- **It deploys and works on CloudLab.** On a6 (one c220g2), the repo's own `local` overlay plus k3s
  and OpenEBS ZFS-LocalPV (real copy-on-write) came up from the published `ghcr.io` images without
  building anything. The REST API, branching, copy-on-write isolation, hibernate/wake, scale-to-zero,
  wake-on-connect through the gateway and a cross-zone client (b0) all worked. It needed one extra
  NetworkPolicy on k3s.
- **Branch creation takes seconds, not milliseconds.** The API returns in 40-250 ms, but a child of a
  479 MB parent is usable after **16-22 s without backups and 25-30 s with them** (Neon in experiment 5:
  85 ms). The time goes to Kubernetes, not storage: the snapshot and clone take about 5 s. With backups
  on, the backup plugin's sidecar adds about 10 s before Postgres even starts.
- **Copy-on-write space depends on the parent's last checkpoint.** A child taken right after the
  parent's checkpoint used 10 MB of new space. One taken after the parent's autovacuum but before its
  next checkpoint used 410 MB, because crash recovery replays and rewrites everything since the
  checkpoint.
- **Problems a benchmark would hit:**
  - About 1 in 4 first connections right after a branch became ready (or woke) hung for 127 s inside
    the gateway, which dials the backend without a timeout.
  - With backups on, every branch starts a full base backup. That backup keeps scale-to-zero from
    firing until it finishes; one ran past 14 min.
  - One c220g2 node fits about 15 running `xata.large` branches (8 GiB each).
- **Recommended setup:** one Wisconsin node as an all-in-one cell with ZFS-LocalPV on a whole SSD,
  backups off, clients on other nodes through the gateway. Then extend to a multi-node cell if branch
  count matters (section 6).

## 1. What Xata is

Xata (open source, Apache 2.0) is a Postgres-as-a-service platform on Kubernetes. It does not modify
Postgres and has no page server or WAL service of its own: each branch is an ordinary Postgres cluster
run by CloudNativePG (CNPG), on its own Kubernetes volume. Branching is a copy-on-write clone of the
parent's volume, made by whatever storage driver sits under Kubernetes. Xata adds the control plane, a SQL
gateway, scale-to-zero, and the glue that turns "create a child branch" into "snapshot the parent volume,
clone it, start a Postgres on the clone".

That makes it architecturally the opposite of Neon:

| | Neon (experiments 3-5, 8) | Xata |
|---|---|---|
| Postgres | modified compute, no local data files | stock Postgres 16/17/18 (`cnpg-postgres-plus` image) |
| Storage | pageserver + safekeepers, page-level LSN history | one block volume per branch (PVC), from a CSI driver |
| Branch = | new timeline at an LSN (metadata only) | CSI VolumeSnapshot of the parent's PVC, then a new PVC cloned from it |
| Child start | compute pod fetches pages on demand | new Postgres pod mounts the clone and does crash recovery |
| CoW granularity | 8 kB page versions in layer files | storage driver's block/record size (ZFS recordsize, NVMe-oF block, ...) |
| Where CoW lives | in Neon's code | outside Xata: ZFS, Ceph RBD, or Xata's closed Xatastor |

## 2. Components

```
 Control plane (namespace xata)
   CLI / dblib ──REST──► Envoy "eg" ──► projects ──gRPC──► clusters ──► k8s API: Branch CR + <id>-app Secret
                                          │  ▲
                          ValidateAccess  ▼  │ metadata in Postgres (postgres-0)
                                         auth ──► Keycloak (OIDC users/orgs), API keys, OPA policies

 Operators
   branch-operator: Branch CR ──► CNPG Cluster, Pooler, Services, NetworkPolicy,
                                  VolumeSnapshot of parent PVC (child branches), backups
   CNPG (Xata fork): Cluster ──► Postgres pod (+ scale-to-zero sidecar) on a PVC
   CSI driver: PVC / VolumeSnapshot / clone-from-snapshot  ◄── copy-on-write happens here

 Data path (namespace xata-clusters)
   psql ──TLS, SNI=<id>[-rw|-ro|-pooler].<domain>:7654──► gateway ──► branch-<id>-rw.xata-clusters.svc:5432
                                                            └─ hibernated? ask clusters to wake, hold conn
```

Services in this repo (all Go, one binary per service, images `ghcr.io/xataio/xata/<svc>:<commit>`):

- **projects** (`services/projects`): the public REST API (`openapi/projects.yaml`): organizations,
  projects, branches, regions, instance types, images, limits. Keeps its metadata (projects, branch tree,
  branch names, limits) in a Postgres (`postgres-0` in namespace `xata`). A *region* maps to a *cell*;
  each cell is reached through the cell's **clusters** gRPC service. In the open-source build there is
  one cell, `local`.
- **auth** (`services/auth`) + **Keycloak**: OIDC users and organizations, API keys with RBAC, policy
  checks via OPA. The REST services check every request with auth's `ValidateAccess` gRPC (Keycloak JWT
  or API key). Service-to-service gRPC calls carry short-lived Ed25519-signed tokens.
- **clusters** (`services/clusters`): the per-cell gRPC API (`proto/clusters`). `CreatePostgresCluster`
  turns a request into a `Branch` custom resource (`branchbuilder.go`): it copies size, image, resources
  and Postgres settings from the parent, picks the storage and snapshot class, forces child branches to
  one instance, and writes the `<id>-app` credentials secret. It also answers describe, credentials,
  metrics and logs calls (VictoriaMetrics/VictoriaLogs).
- **branch-operator** (`services/branch-operator`): a controller-runtime operator for the `Branch` CRD
  (`xata.io/v1alpha1`). For each Branch it reconciles a CNPG `Cluster`, a PgBouncer `Pooler`, services,
  a per-branch NetworkPolicy, backups (barman-cloud or pgBackRest to S3/GCS/Azure) and, for a child, the
  `VolumeSnapshot` of the parent's PVC (`pkg/reconciler/volumesnapshot.go`). It also runs the
  `WakeupRequest` controller, which serves branches from pre-created cluster pools (only with Xatastor,
  see below).
- **gateway** (`services/gateway`): a Postgres-protocol proxy (port 7654) plus SQL-over-HTTP/WebSocket
  (8443, the serverless driver). It terminates TLS, takes the branch from the first label of the SNI
  hostname (`<branch>[-rw|-ro|-r|-pooler].<domain>`), applies IP filtering, and forwards to
  `branch-<id>-<endpoint>.xata-clusters.svc:5432`. If the branch is hibernated with scale-to-zero on, it
  asks clusters to wake it and holds the client's connection until Postgres is reachable (default timeout
  50 s, status polled every 100 ms).
- **scale-to-zero** (`services/scale-to-zero-sidecar` + the CNPG-I plugin `cnpg-i-scale-to-zero`): the
  plugin injects a sidecar into every Postgres pod. On the primary it counts client connections in
  `pg_stat_activity` and, after `inactivityPeriodMinutes` (1-1440, default 30) with none, sets the
  Branch's hibernation, so CNPG deletes the pods and keeps the PVC.
- **CNPG fork** (`ghcr.io/xataio/xata-cnpg`, 1.28 based): CNPG with Xata's patches; does HA, failover,
  switchover, rolling upgrades, hibernation, recovery from a VolumeSnapshot or from object storage.

Infrastructure the local overlay also installs: Envoy Gateway (REST and Keycloak ingress), cert-manager,
the Keycloak operator, barman-cloud plugin, RustFS (S3 for backups), VictoriaMetrics/VictoriaLogs/
vmagent/Vector (per-cell observability), and a bootstrap Postgres for projects/auth/Keycloak metadata.

## 3. How a branch is created

Root branch (`POST /organizations/{org}/projects/{p}/branches`, `mode: custom`): projects validates
limits and calls clusters `CreatePostgresCluster`; clusters writes the `Branch` CR with
`bootstrap.initdb`; the operator creates the CNPG `Cluster`; CNPG runs an `initdb` job on a new PVC, then
starts the instance pod.

Child branch (`mode: inherit, parentID`):

1. clusters builds a Branch with `restore: {type: VolumeSnapshot, name: <parent>}` and the parent's
   storage size, storage class and snapshot class.
2. branch-operator checks the parent is `Cluster in healthy state` (or upgrade-delayed), then creates
   `VolumeSnapshot <parent>-<child>` of the parent's PVC. This is taken from the running parent: a
   crash-consistent snapshot, no checkpoint or backup mode.
3. It creates the child CNPG Cluster with `bootstrap.recovery.volumeSnapshots` pointing at that
   snapshot and `recoveryTarget: immediate`. The CSI driver provisions the child PVC as a clone of the
   snapshot. CNPG runs a `snapshot-recovery` job on it (prepares PGDATA), then starts the instance pod,
   whose Postgres logs "database system was not properly shut down; automatic recovery in progress",
   replays WAL from the parent's last checkpoint and comes up as a new primary.
4. Once the child is healthy the operator deletes the VolumeSnapshot object. On ZFS the underlying
   snapshot stays, because the clone depends on it.

So branch latency = snapshot + clone + pod scheduling + container start + Postgres crash recovery +
CNPG health checks, and the data copied is whatever the driver copies (nothing, for a CoW driver).

### What is not in the open-source repo

- **Xatastor**, Xata's storage engine (NVMe-over-Fabrics volumes with CoW snapshots and clones, `Xvol`
  CRD, `xatastor-csi`). The open code has the client side only: the `XVolClone` restore type, the
  generated `VolumeManager` gRPC mocks (`CreateVolume`, `CreateSnapshot`, `GetCloneTree`, ...), the
  `xatastor-*` VolumeAttributesClasses for storage QoS, and feature flags (`useXatastor`, 10x higher
  branch limits). It is off by default (`xatastor.enabled: false`).
- **Cluster pools / fast wake-up**: the `ClusterPool` CRD's operator. With Xatastor, a child branch is
  an XVol clone attached to an already-running pooled Postgres pod through a `WakeupRequest`, so it skips
  pod scheduling and start. Without Xatastor these paths are inactive.
- Multi-cell / multi-region and adversarial multi-tenancy hardening (per Xata's blog post).

The open-source branch path is therefore always "VolumeSnapshot + new pod", and its CoW behaviour depends
entirely on the CSI driver chosen.

## 4. Smoke test on a6

Setup (details and versions in `REPRODUCE.md`): single-node k3s on a6, ZFS pool on a file on the SSD,
OpenEBS ZFS-LocalPV as StorageClass `zfs-cow` (recordsize 8k) and VolumeSnapshotClass `zfs-cow-snap`,
then Xata's `kustomize/overlays/local` with images swapped to `ghcr.io/xataio/xata/*:f197e7b` and the
hostpath classes swapped to the ZFS ones. 34 pods came up within about 10 min of the apply. Everything
went through the public interfaces: REST API (Keycloak login as the dev user) and `psql` through the SQL
gateway. Branches were `xata.large` (2 vCPU request / 4 limit, 8 GiB, `shared_buffers` 2 GB),
Postgres 17.11, 20 GiB volume. The root was seeded with one 4 M-row table (479 MB database).

### What worked

| Check | Result |
|---|---|
| REST API: orgs, regions, instance types, images, projects, branches | works with a Keycloak token (`cli` client, password grant) |
| Root branch (`mode: custom`) | healthy after 13.5 s (no backups); the first one failed until the k3s NetworkPolicy fix |
| Child branch (`mode: inherit`) | see timings below; data identical to the parent at creation |
| Copy-on-write isolation | updated 1,000 rows + inserted 1 in a child: child changed, parent unchanged |
| ZFS clones | every child PVC is a `zfs clone` of a snapshot of the parent's dataset (`data/smoke/zfs_final.txt`) |
| Hibernate / wake via `PATCH {hibernate}` | pods gone after 63 s; wake: healthy 13.5 s, first query 15.2 s, data intact |
| Scale-to-zero (1 min inactivity) | idle root hibernated after 218 s (the sidecar checks every 60 s and needs two idle checks) |
| Wake-on-connect through the gateway | 15.6 s from `psql` start to result on a hibernated root |
| Cross-zone client (b0, Utah) via `<branch>.128.105.145.137.nip.io:7654` | connect 1.0 s (DNS + TLS + SCRAM over 39 ms RTT); `select 1` median 39.4 ms |
| Organization API key as bearer token | **not working** in the dev setup: the key validates but `default-org` does not exist in Keycloak (401) |

### Child-branch timings

Seconds from the POST. "Healthy" = API status `STATUS_TYPE_HEALTHY` with a connection string; "first
query" = first `select 1` answered through the gateway (5 s connect timeout per attempt, retried).

| Configuration | n | POST returns | Healthy | First query |
|---|---|---|---|---|
| Backups on, NetworkPolicy enforced | 4 | 0.04-0.10 | 24.3 / 28.5 / 30.3 / 28.3 | 24.8 / 34.2 (5 s stall) / 30.4 / 28.8 |
| Backups on, NetworkPolicy off | 2 | 0.05-0.26 | 24.8, 25.5 | 25.4, 25.7 |
| Backups off, NetworkPolicy off | 6 | 0.04-0.20 | 15.9-21.8 (median 20.0) | 16.0-22.0 (one 5 s stall) |

A fifth backups-on child (first run) became healthy but its first connection stalled for 127 s, which
stopped that run (`logs/smoke_test_attempt1.log`). A seventh no-backup child took 178 s: its pods could not be scheduled until another branch hibernated,
because the node's memory requests were at 98% (16 clusters at 8 GiB each). Not counted above.

Where the time goes (child `n847...`, backups on, all images cached), from Kubernetes events:

| Step | Time |
|---|---|
| Branch CR → VolumeSnapshot ready (ZFS snapshot) | 1-2 s |
| Clone PVC provisioned (first try failed: snapshot not yet ready; CSI retry) | about 4 s |
| CNPG `snapshot-recovery` job (prepares the restored PGDATA) | about 6 s |
| Instance pod: init containers, then the barman-cloud backup sidecar must start before Postgres | about 11 s (10 s of it the sidecar) |
| Postgres start, crash recovery, CNPG health check | about 2-3 s |

Without backups there is no barman sidecar, which saves roughly that 10 s. The storage work (snapshot
plus clone) is a small part either way: branch latency is dominated by Kubernetes pod lifecycle, as Xata's
closed cluster pools are designed to avoid.

### Copy-on-write space and crash recovery

Because the snapshot is taken from a running parent, each child starts with crash recovery from the
parent's last checkpoint redo point, and its end-of-recovery checkpoint writes every page that recovery
touched. On ZFS those writes un-share the blocks even when the bytes are identical. Observed child
`USED` (new space) on the ZFS clones:

| Parent state when the child was cloned | Child new space |
|---|---|
| Just seeded and `CHECKPOINT`ed | 10.5 MB |
| After the parent's autovacuum of the 4 M-row table, before its next checkpoint | 359-410 MB per child (the table is 479 MB) |

Child `damrhn...`'s log: "redo starts at 0/326B73E0 ... redo done at 0/3B53DC30" (149 MB of WAL in
2.6 s), then the end-of-recovery checkpoint "wrote 49403 buffers" (386 MB, 2.2 s). With
`checkpoint_timeout = 300 s` and spread checkpoints, the redo point can be up to about 10 min behind. So
both branch latency and branch space grow with the parent's write volume since its last checkpoint.
Neon children pay neither cost.

### Problems found

1. **k3s blocks CNPG from the API server.** The branch-operator's per-branch NetworkPolicy allows egress
   only to the branch's own pods and DNS. k3s enforces policies (kube-router); kind, which Xata uses for
   development, does not. Fix: `scripts/allow_apiserver_egress.yaml` (egress to the API server and the
   S3 store), or a cluster whose network plugin does not enforce policies.
2. **First connection can hang for 127 s.** In 5 of 18 first connections right after a branch became
   ready or woke, the gateway's backend dial hung until the kernel's SYN retries ran out
   (`tcp_syn_retries = 6`, about 127 s). In every case, a few dials first failed fast with
   `connection refused` (Service without endpoints yet), and the hung dial followed immediately. New
   connections made at the same time succeed in milliseconds. It happened both with NetworkPolicy
   enforced (3/7) and with the policy controller disabled (2/11), so kube-router is not the cause.

   The pattern fits a kube-proxy rule-update race: a SYN that passes after the "no endpoints" reject
   rule is gone but before the DNAT rule exists gets a conntrack entry without NAT, and its
   retransmissions keep following it. This is a hypothesis; a conntrack capture did not catch a stall
   in 4 tries. The gateway (`session/cluster_dialer.go`) dials with the session context, which has no
   deadline, so a lost SYN costs the full kernel timeout. Mitigations: a client connect timeout plus
   retry (what `smoke_test.py` does), a dial timeout in the gateway (a one-line change), or a
   kube-proxy-free network plugin (Cilium).
3. **Backups slow everything and block scale-to-zero.** With backups on (the default for region
   `local`), each new branch:
   - starts the barman-cloud sidecar (+10 s to create);
   - takes an immediate **full** base backup to S3. This is not copy-on-write: the 1.4 GB data
     directory is gzip-compressed on about 0.25 core.

   Five concurrent child backups were still running after 14 min. The backup's open connection counts as
   activity for the scale-to-zero sidecar, which counts any client connection, so those children never
   hibernated. For benchmarks, set `regions.backups_enabled = false` (done here in the `projects`
   database) or configure pgBackRest.
4. **Capacity.** Each running branch reserves its instance type's memory: `xata.large` is 8 GiB, so one
   c220g2 (125 GiB) fits about 15. Each branch is two pods (Postgres + PgBouncer), so Kubernetes'
   default 110-pod limit allows about 50 per node even with `xata.micro` (1 GiB). Hibernated branches
   free both.
5. **Defaults that assume one machine.** Connection strings use the region's `hostport`
   (`127.0.0.1.nip.io:7654`), and the REST route only matches `Host: localhost`. Remote clients need the
   region's `hostport` set to `<node IP>.nip.io:7654` (or a real wildcard DNS name) and the
   `httpRoute.hostname` of the charts changed, or must send `Host: localhost` and rewrite the
   connection host (as `remote_client_probe.py` does).

## 5. Comparison with the systems already benchmarked

| | Neon (exp. 5, cross-zone) | Xata OSS (this smoke test, one node) | Dolt / Doltgres |
|---|---|---|---|
| Branch create (API) | 85 ms, usable at once | 40-250 ms ack, usable after 16-30 s | ms (metadata) |
| Branch = | timeline at LSN | volume snapshot + clone + new Postgres pod | commit-graph ref |
| New space per branch | about 0 until written | 10 MB to most of the table, depending on parent WAL since checkpoint | about 0 until written |
| Running cost per branch | compute process (+LFC) | full Postgres pod with reserved CPU/RAM (+ PgBouncer pod) | none (one server) |
| Idle branches | compute stops | scale-to-zero hibernation (60 s checks, about 15 s wake) | n/a |
| Postgres compatibility | patched Postgres | stock Postgres + extensions | MySQL / Postgres wire, own engine |

## 6. Deployment options on the CloudLab cluster

Constraints from `.agent/cloudlab.md`: Wisconsin c220g2 nodes have 40 threads, 125-157 GiB RAM and one
447 GB SSD each, on a 10 GbE LAN. Utah r320 nodes have 16 GB RAM and only HDDs. The zones see each other
only over 1 GbE public links with 39 ms RTT. Neon's services currently occupy a0-a5 and b0-b7; a6 (now
running Xata) and a7 are free.

**A. Single-node cell (recommended first; what this experiment built).** k3s plus ZFS-LocalPV plus
Xata on one c220g2, with the benchmark client on another Wisconsin node or on b0. For real runs:
- give ZFS the whole SSD (`zpool create xpool /dev/sdc`) instead of a file on XFS;
- turn backups off;
- either keep NetworkPolicy enforcement with the egress fix or start k3s with
  `--disable-network-policy`;
- raise `max-pods` if many branches;
- set the region `hostport` to the node's LAN address (`10.10.1.16.nip.io:7654`) so connection strings
  work from other nodes.

Capacity: about 15 `xata.large` or about 50 small branches running at once; more with scale-to-zero.
Good for branch latency, copy-on-write space and scale-to-zero studies, and directly comparable to the
single-node Dolt runs.

**B. Multi-node cell in Wisconsin.** k3s server (control plane + Xata services) on one node, agents on
others; CNPG pods spread across agents. Two storage choices:
- *ZFS-LocalPV on every node.* Clones are node-local, so a child is scheduled on its parent's node and a
  whole branch tree lives on one node. Branch count scales with nodes only across independent root
  branches. Simple, and keeps SSD-local I/O.
- *Rook-Ceph RBD on the SSDs of 3+ nodes.* Networked block volumes with copy-on-write RBD clones, so any
  child can run on any node: the closest open analogue to Xata Cloud's NVMe-oF storage. Costs: Ceph's
  replication traffic on the 10 GbE LAN and write latency, plus ceph-csi flattening clone chains past a
  configurable depth (deep branch trees eventually get full copies). Much more setup work than ZFS.
- Avoid the Tilt default `csi-hostpath` driver for any measurement: its snapshots are full copies.

**C. Cross-zone.** Unlike Neon, compute and storage of a Xata branch are on the same node (node-local
volume), or on networked block storage that would have to cross the 1 GbE / 39 ms WAN, which is not
practical for Ceph. The open-source build also has a single cell (multi-cell is closed source). So the
meaningful cross-zone setup is the **client in Utah, the whole cell in Wisconsin**. Every client round
trip then costs 39 ms (measured), the same as Neon's compute-to-client distance in experiments 4/5 but
on the client link instead of the storage link. Running Postgres pods on the Utah r320s (16 GB RAM, HDD
only) is possible as extra k3s agents for `xata.micro/small` branches, but they would need their own
local CoW storage on HDDs.

### What the benchmark harness would need

`dblib/xata.py` already speaks this REST API (same paths as Xata Cloud: `projects/{id}/branches`,
`mode: inherit`, polling `status.statusType`). For the self-hosted cell it needs:
- the base URL from an env var instead of the hard-coded `https://api.xata.tech`;
- a token from Keycloak (password grant, refreshed) instead of an API key, unless a real organization
  is created so that org API keys work;
- a connect timeout and retry on the first connection to a new branch (problem 2);
- the region `hostport` fix above for clients on other nodes.

Branch timings should report both the API acknowledgement and readiness; the adapter already waits for
readiness inside BRANCH.

## 7. State left on the cluster

a6 still runs k3s with the full Xata stack, port-forwards on 8080/5081, and 16 branches in two projects
(`smoke`: root + 7 children; `nobackup`: root + 7 children), several hibernated. Changes made after the
smoke test, for the diagnostics:
- k3s network-policy enforcement is disabled (`/etc/rancher/k3s/config.yaml`);
- region `local` has `backups_enabled = false`;
- the smoke project's root still has scale-to-zero at 1 minute;
- a `bench` org API key exists (unusable, see above).

Cleanup is in `REPRODUCE.md`. Nothing on a0-a5, a7 or the b nodes was changed, except a read-only
`psql` probe from b0.
