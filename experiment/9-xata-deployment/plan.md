# Plan: Xata architecture and CloudLab deployment (experiment 9)

Request (25 Oct 2026): understand the architecture of the open-source Xata platform (submodule
`3rdparty/xata`, commit `f197e7b`), explore how it can be deployed on the CloudLab cluster, smoke-test it
on the cluster without long runs, and report on the architecture and possible deployment approaches.

Approved by the user during the session: install k3s, ZFS, the CSI snapshot controller, OpenEBS
ZFS-LocalPV and Xata's published charts and images on the idle node a6 (the Neon services on a0-a5 and the
b nodes are left alone).

Steps:

1. Read the source: services, CRDs, branch-create path, storage, gateway, scale-to-zero, what is closed.
2. On a6: single-node k3s on the SSD, ZFS pool on the SSD, ZFS-LocalPV as the copy-on-write CSI driver.
3. Deploy Xata's own `kustomize/overlays/local` (what `tilt up` deploys) with the published
   `ghcr.io/xataio/xata/*:f197e7b` images and the ZFS storage/snapshot classes.
4. Smoke test through the REST API and the SQL gateway: root branch, seed, three child branches (timed),
   copy-on-write isolation, hibernate/wake, scale-to-zero wake-on-connect.
5. Write `report.md`: architecture, what the smoke test showed, and deployment options for benchmarks.
