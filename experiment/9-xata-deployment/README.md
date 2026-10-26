# Experiment 9: Xata architecture and deployment on CloudLab

**Question.** How is the open-source Xata platform (`3rdparty/xata`, commit `f197e7b`) built, and how
can it be deployed on the two-zone CloudLab cluster for branching benchmarks? Does it actually work
there (smoke test only, no long runs)?

**Verdict.** Xata runs on CloudLab from its published images, with no builds: one c220g2 node running
k3s, OpenEBS ZFS-LocalPV (real copy-on-write) and Xata's own `local` overlay, with one NetworkPolicy fix.
Branching, isolation, hibernate/wake, scale-to-zero and cross-zone clients all work.

Architecturally each branch is a separate stock Postgres pod on a cloned volume. A child is usable
16-22 s after the request without backups and 25-30 s with them, almost all of it Kubernetes pod
lifecycle. Its new space is small, unless the parent wrote a lot since its last checkpoint; crash
recovery then un-shares it.

Problems for benchmarking:
- an occasional 127 s first-connection stall in the gateway;
- per-branch full backups that block scale-to-zero;
- about 15 `xata.large` branches per node.

Recommended setup: a single-node cell (backups off, ZFS on the whole SSD), clients via the gateway.

Read `report.md` for the architecture, the smoke-test results and the deployment options;
`REPRODUCE.md` for the exact setup, versions and pitfalls.

## Layout

| Path | Contents |
|---|---|
| `plan.md` | the plan agreed in the session |
| `report.md` | architecture, smoke-test results, deployment options |
| `REPRODUCE.md` | machine, versions, commands in order, pitfalls, cleanup |
| `scripts/node_setup_k3s_zfs.sh` | on the node: k3s on the SSD, ZFS pool, CSI snapshot controller, OpenEBS ZFS-LocalPV, `zfs-cow` StorageClass and `zfs-cow-snap` VolumeSnapshotClass |
| `scripts/node_deploy_xata.sh` | on the node: renders Xata's `kustomize/overlays/local` with the published `ghcr.io` images and the ZFS classes, applies it |
| `scripts/allow_apiserver_egress.yaml` | NetworkPolicy needed on k3s so CNPG pods reach the API server and S3 |
| `scripts/node_port_forwards.sh` | on the node: port-forwards Keycloak (8080) and the REST API (5081) |
| `scripts/xata_api.py` | stdlib REST client (Keycloak password grant as the dev user) |
| `scripts/smoke_test.py` | on the node: seed, child branches, isolation, ZFS space, hibernate/wake, scale-to-zero |
| `scripts/remote_client_probe.py` | connect to a branch through the gateway from another node (nip.io SNI host) |
| `scripts/followups.py` | on the node: the follow-up steps (scale-to-zero on the root, children with policy off, backup-free project) |
| `scripts/conntrack_probe.py` | diagnostic: create children until a first connection stalls, dump conntrack (caught none in 4 tries) |
| `data/smoke/` | `summary.json` (main smoke run), `children_netpol_off.json`, `nobackup.json` (backup-free project: root + 3 children), `scale_to_zero_root.json`, `zfs_final.txt` (branch, dataset, used, refer, origin snapshot), k8s dumps (`k8s_*.json` after the main run, `*_final.json` at the end) |
| `data/remote_probe_b0.json` | cross-zone connection probe from b0 |
| `logs/` | setup, deploy and smoke-test output; `smoke_test_attempt1.log` is the first run, stopped by the 127 s connection stall; `scale_to_zero_root*.log`, `children_netpol_off.log`, `nobackup.log`, `conntrack_probe.log` are the follow-up diagnostics; `xata_images.txt` lists every image deployed |
| `figures/`, `code/` | empty: no plots; no shared code changed |

## File formats

`data/smoke/summary.json`: `root` (branch id, rows, database size), `children` (per child: `post_s` =
REST call latency, `healthy_s` = POST to API status `STATUS_TYPE_HEALTHY` with a connection string,
`first_query_s` = POST to first `select 1` answered through the gateway, `failed_connects` = 5 s-timeout
attempts that failed before it), `steps.isolation`, `steps.zfs_list` (`zfs list -p`: name, used bytes,
referenced bytes, origin), `steps.hibernate_wake_api`, `steps.scale_to_zero`. All times in seconds from
the start of the step. `k8s_*.json` are `kubectl get -o json` dumps taken at the end.

## Shared code

None changed. The existing `dblib/xata.py` (Xata Cloud REST API) was read but not modified; the report
lists what it would need for a self-hosted endpoint.
