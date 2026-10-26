# Reproducing experiment 9 (Xata on one CloudLab node)

## Machine

a6 = `c220g2-011302.wisc.cloudlab.us` (public 128.105.145.137, LAN 10.10.1.16): 2x Xeon E5-2660 v3
(40 threads), 125 GiB RAM, Ubuntu 22.04, kernel 5.15. Root is on an HDD; everything else is on the SSD
`/dev/sdc`, XFS at `/mnt/ssd` (left over from experiment 4). See `.agent/cloudlab.md`. Any c220g2 node
works; avoid a0-a5 and the b nodes while the Neon services of experiments 5/8 are kept.

## Versions

| Component | Version |
|---|---|
| Xata | submodule `3rdparty/xata` at `f197e7b`; images `ghcr.io/xataio/xata/<svc>:f197e7b` (published by Xata's CI per commit) |
| Xata CNPG fork | chart `oci://ghcr.io/xataio/xata-cnpg/charts/cloudnative-pg` `0.0.0-g228b764`, image `ghcr.io/xataio/xata-cnpg/cloudnative-pg:g228b764` |
| scale-to-zero CNPG-I plugin | `v0.1.8` (sidecar `ghcr.io/xataio/xata/scale-to-zero-sidecar:f197e7b`) |
| Postgres image | `ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.11` (API image name `postgres:17.11`) |
| k3s | `v1.33.5+k3s1` (flannel + kube-router network policy), `--disable traefik`, data dir `/mnt/ssd/k3s` |
| Helm / kustomize / yq | `v3.18.6` / `v5.7.1` / `v4.47.1` |
| CSI snapshotter CRDs + controller | `v8.2.0` |
| OpenEBS ZFS-LocalPV | chart `2.8.0`; ZFS `2.1.5` (Ubuntu); pool `xpool` on a 200 GB sparse file `/mnt/ssd/xpool.img` |

Code: this repo's commit plus the uncommitted experiment directory; no shared code is used or changed
(the smoke test talks to the REST API directly). The Xata source is used unmodified; the deploy script
edits a scratch copy (`~/xata-src` on the node) as described below.

## Steps (from the repo root on the workstation)

```bash
A6=JiyuHu23@c220g2-011302.wisc.cloudlab.us; K="-i $HOME/.ssh/dassl_rsa"
E=experiment/9-xata-deployment

# 1. k3s + ZFS + snapshot controller + ZFS-LocalPV (about 2 min)
scp $K $E/scripts/node_setup_k3s_zfs.sh $A6:~/ && ssh $K $A6 'bash ~/node_setup_k3s_zfs.sh'
ssh $K $A6 'kubectl annotate sc local-path storageclass.kubernetes.io/is-default-class-'

# 2. Xata: render kustomize/overlays/local with published images and ZFS classes, apply (about 1 min,
#    then about 5-10 min for images to pull and Keycloak to settle)
rsync -a --delete --exclude .git -e "ssh $K" 3rdparty/xata/ $A6:~/xata-src/
scp $K $E/scripts/node_deploy_xata.sh $A6:~/ && ssh $K $A6 'bash ~/node_deploy_xata.sh'
#    pass check: every pod Running/Completed except svclb-envoy-xata-keycloak-eg-* (Pending is fine:
#    k3s ServiceLB cannot give two LoadBalancers port 80 on one node; we port-forward instead)

# 3. Network policy fix for k3s (required, see pitfalls)
sed 's#NODE_IP#128.105.145.137#' $E/scripts/allow_apiserver_egress.yaml | ssh $K $A6 'kubectl apply -f -'

# 4. Port-forwards to Keycloak (8080) and the REST API (5081), as the Tiltfile does
scp $K $E/scripts/node_port_forwards.sh $E/scripts/xata_api.py $E/scripts/smoke_test.py $A6:~/
ssh $K $A6 'bash ~/node_port_forwards.sh && python3 ~/xata_api.py get /organizations'
#    pass check: lists "default-org"

# 5. Project + root branch (root ready in about 30 s)
ssh $K $A6 'python3 ~/xata_api.py post /organizations/default-org/projects \
  "{\"name\": \"smoke\", \"configuration\": {\"scaleToZero\": {\"baseBranches\": {\"enabled\": false}, \"childBranches\": {\"enabled\": false}}}}"'
ssh $K $A6 'python3 ~/xata_api.py post /organizations/default-org/projects/<prj>/branches \
  "{\"name\": \"main\", \"mode\": \"custom\", \"configuration\": {\"region\": \"local\", \"instanceType\": \"xata.large\", \"image\": \"postgres:17.11\", \"replicas\": 0, \"storage\": 20}}"'

# 6. Smoke test (about 5-8 min, most of it waiting for scale-to-zero)
ssh $K $A6 'python3 ~/smoke_test.py --project <prj> --root <root branch id> --out ~/smoke-out' \
  > $E/logs/smoke_test.log 2>&1
rsync -a -e "ssh $K" $A6:~/smoke-out/ $E/data/smoke/
```

The dev login is `dev@xata.tech` / `Xata1234!` through Keycloak client `cli` (secret `devsecret`, password
grant), issuer `http://localhost:8080/realms/xata`; tokens must come from that exact issuer URL, hence
the port-forward. Connection strings point at `<branch>-deprecated.127.0.0.1.nip.io:7654`, so as
configured they only work on the node itself (see the report for remote clients).

## Follow-up diagnostics (run after the smoke test, in this order; about 20 min in all)

`scripts/followups.py` holds each step (it imports `smoke_test.py`); copy it next to it on the node.

```bash
scp $K $E/scripts/followups.py $E/scripts/conntrack_probe.py $A6:~/
# a. scale-to-zero + wake-on-connect on the smoke root (its backup had completed), about 5 min
ssh $K $A6 'cd ~ && python3 followups.py scale-to-zero <prj> <root>'          # logs/scale_to_zero_root.log
# b. disable k3s network-policy enforcement and flush kube-router's chains, about 1 min
ssh $K $A6 'echo "disable-network-policy: true" | sudo tee /etc/rancher/k3s/config.yaml; sudo systemctl restart k3s;
  sleep 15; sudo iptables-save | grep -v KUBE-ROUTER | sudo iptables-restore; bash ~/node_port_forwards.sh'
# c. wake-on-connect again: wait until the root hibernates again (scale-to-zero still at 1 min), then
#    time `psql "<root connection string>" -c "select count(*) from smoke_t"`  # logs/scale_to_zero_root_netpol_off.log
# d. two children with policy off, about 1 min
ssh $K $A6 'cd ~ && python3 followups.py children <prj> <root> 2 np-off'      # logs/children_netpol_off.log
# e. backups off for region local, then a backup-free project (root + 3 children), about 3 min
ssh $K $A6 "kubectl exec -i -n xata postgres-0 -- sh -c 'psql -U \$POSTGRES_USER -d projects'" <<< \
  "update regions set backups_enabled = false where id = 'local';"
ssh $K $A6 'cd ~ && python3 followups.py nobackup-project 3'                  # logs/nobackup.log
# f. conntrack probe on the backup-free root, about 2-4 min
ssh $K $A6 'sudo apt-get install -y conntrack && cd ~ && python3 conntrack_probe.py <nobackup prj> <nobackup root> 4'
# g. collect
ssh $K $A6 'kubectl get events -n xata-clusters -o json > ~/smoke-out/k8s_events_final.json'   # and branches/clusters
rsync -a -e "ssh $K" $A6:~/smoke-out/ $E/data/smoke/
```

## Pitfalls

- **k3s enforces NetworkPolicy, Xata's per-branch policy blocks the API server.** The branch-operator
  gives every branch a policy allowing egress only to its own pods and DNS. CNPG's `initdb` job,
  instance manager and PgBouncer need the API server (on k3s, `<node IP>:6443` behind `10.43.0.1:443`), so
  the first root branch looped on `connection refused` until `allow_apiserver_egress.yaml` was applied.
  Kind (Xata's dev setup) does not enforce these policies, so upstream does not hit this.
- **First connection to a new branch can hang for 127 s.** One of two children in the first run: the
  gateway's first dials got `connection refused` (Service without endpoints yet), the next SYN was
  dropped and never answered, and the gateway's backend dial has no deadline, so the client waited for
  the kernel's 6 SYN retries (`tcp_syn_retries=6`, ~127 s) and then got an error. A new connection right
  after works. Use a client connect timeout (`PGCONNECT_TIMEOUT=5`) and retry; `smoke_test.py` does and
  records the failed attempts. The first run's log is `logs/smoke_test_attempt1.log`.
- `pkill -f "kubectl port-forward"` inside an `ssh ... '...'` command kills the ssh session itself (the
  pattern matches its own command line); `node_port_forwards.sh` uses pid files.
- The ZFS-LocalPV chart ships the snapshot CRDs too; install it with
  `crds.csi.volumeSnapshots.enabled=false` when the CRDs are installed separately.
- The branch-create API's discriminator is `mode: custom` (root) / `mode: inherit` (child), not
  `configuration`.
- Events show `FailedToRetrieveImagePullSecret (ghcr-secret)`: harmless, the images are public.
- The stall in item 2 is not caused by kube-router: it also happened with policy enforcement disabled
  (2 of 11 first connections, vs 3 of 7 with it). Always connect with a timeout and retry.
- Region `local` has backups on by default: every branch then gets a barman-cloud sidecar (+10 s to
  create) and an immediate full base backup; the backup's connection keeps scale-to-zero from firing.
- With `xata.large` (8 GiB request) the node's memory requests fill at about 15 running branches; the
  next branch's pods stay Pending until another branch hibernates (one child took 178 s that way).
- Org API keys do not work for the dev user's `default-org` (it is not a Keycloak organization): use
  Keycloak tokens.
- The REST route matches `Host: localhost` only; from another machine send that Host header.
- Kubernetes' default 110 pods per node limits a node to about 50 running branches (instance + pooler
  pod each); raise `--kubelet-arg=max-pods=` or disable the pooler for scale runs.

## Cleanup

```bash
ssh $K $A6 'for f in ~/pf-kc.pid ~/pf-api.pid; do kill $(cat $f); done; \
  /usr/local/bin/k3s-uninstall.sh; sudo zpool destroy xpool; sudo rm -f /mnt/ssd/xpool.img; \
  sudo rm -rf /mnt/ssd/k3s ~/xata-src ~/xata-render'
```
