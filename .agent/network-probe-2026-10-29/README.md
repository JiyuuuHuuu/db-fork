# Network probe, 2026-10-29: 4 Wisconsin nodes on two subnets

Nodes (`nodes.txt`): g1a/g1b are c220g1 (E5-2630 v3) on VLAN `lanA` 10.10.1.0/24; g2a/g2b are c220g2 (E5-2660 v3)
on VLAN `lanB` 10.10.2.0/24. All four are at CloudLab Wisconsin; public IPs are all in 128.105.144.0/22.

Question: is cross-subnet RTT higher than within-subnet RTT? **No.** The two experimental VLANs have no router,
so cross-subnet traffic over 10.10.x fails (100% loss); it can only go over the public 1 GbE control network,
which is a single L2 segment for all four nodes (tracepath: 1 hop for every pair). Cross-subnet public RTT is within
~10-20 us of same-subnet RTT, less than the spread between directions of the same pair.

| Path | ICMP avg RTT (per-pair range) | TCP 64 B ping-pong p50 |
|---|---|---|
| same subnet, experimental LAN | 109-133 us | 278-328 us |
| same subnet, public | 123-148 us | 289-337 us |
| cross subnet, public | 133-161 us | 318-353 us |
| cross subnet, experimental LAN | unreachable | - |

Method (`scripts/probe.sh`, ~3 min, pairs probed one at a time): `sudo ping -c 1000 -i 0.005` per ordered pair and
path; `scripts/tcprtt.py` 2000 ping-pongs (100 warm-up, 2 ms gap, TCP_NODELAY; Python, so ~150 us is host
overhead); `tracepath` on the public path. `scripts/summarize.py` -> `summary.txt`. Raw output in `raw/`.

To make cross-subnet slower for real, the profile needs a router/delay node between the VLANs, or `tc netem`
delay on the nodes for traffic to the other subnet's addresses.

## Emulated availability zones (applied 2026-10-29, still active)

Zone 1 = g1a, g1b (c220g1); zone 2 = g2a, g2b (c220g2). `scripts/az_netem.sh apply 400` puts a `prio` root qdisc
on each node's public NIC `enp1s0f0` and sends only packets addressed to the other zone's public IPs through
`netem delay 400us`, so cross-zone RTT gains 0.8 ms (target ~1 ms, typical for AZs in one cloud region). Same-zone
traffic and ssh from outside are untouched. No jitter: netem jitter reorders packets, which real AZ links don't.
The setting does not survive a reboot; `az_netem.sh status` shows it, `az_netem.sh remove` restores the default `mq`.

Measured with the delay on (`raw_az400/`, `summary_az400.txt`):

| Path | ICMP avg RTT | TCP 64 B ping-pong p50 | iperf3 1 stream |
|---|---|---|---|
| same zone, experimental LAN | 110-132 us | 280-319 us | 9.37 Gbit/s |
| same zone, public | 128-152 us | 288-339 us | 937 Mbit/s |
| cross zone, public | 988-1041 us | 1174-1229 us | 936 Mbit/s each way |

Caveat: cross-zone traffic must use public IPs (10.10.1.x and 10.10.2.x are separate VLANs) and so is capped at
1 GbE, while same-zone LAN traffic gets 10 GbE. Real cloud AZ links do not have that 10x bandwidth gap; to remove it,
run same-zone traffic over the public IPs too.
