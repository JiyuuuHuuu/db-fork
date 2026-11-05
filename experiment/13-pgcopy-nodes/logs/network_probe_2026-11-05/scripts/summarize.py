#!/usr/bin/env python3
"""Summarise probe.sh output (RAW/{ping,tcp,rdma}/<src>_<dst>.txt): per-class aggregates, slowest pairs, and
switch groups inferred from RDMA latency (pairs above the gap in the latency distribution cross switches).
Usage: summarize.py RAWDIR"""
import re, statistics as st, sys
from collections import defaultdict
from pathlib import Path
R = Path(sys.argv[1])

def ping(f):
    t = f.read_text(); m = re.search(r"([\d.]+)% packet loss", t)
    a = re.search(r"= ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) ms", t)
    return (float(m[1]) if m else None), (float(a[2]) * 1000 if a else None)

def tcp(f):
    if not f.exists(): return None
    try:
        v = sorted(float(x) for x in f.read_text().split()); return st.median(v), v[int(0.99 * len(v))]
    except (ValueError, IndexError):
        return None

def rdma(f):
    if not f.exists(): return None
    m = re.search(r"^\s*2\s+1000\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", f.read_text(), re.M)
    return float(m[3]) if m else None

rows = {}
for f in sorted((R / "ping").glob("*.txt")):
    s, d = f.stem.split("_")
    loss, rtt = ping(f); t = tcp(R / "tcp" / f.name); r = rdma(R / "rdma" / f.name)
    rows[(s, d)] = (loss, rtt, t, r)
print(f"{len(rows)} ordered pairs")
def agg(name, vals):
    vals = [v for v in vals if v is not None]
    if vals:
        print(f"  {name:28} n={len(vals):3}  min {min(vals):8.2f}  median {st.median(vals):8.2f}  max {max(vals):8.2f}")
agg("ping avg RTT (us)", [r[1] for r in rows.values()])
agg("ping loss (%)", [r[0] for r in rows.values()])
agg("TCP 64 B RTT median (us)", [r[2][0] for r in rows.values() if r[2]])
agg("TCP 64 B RTT p99 (us)", [r[2][1] for r in rows.values() if r[2]])
agg("RDMA write lat typ (us)", [r[3] for r in rows.values()])
missing = [k for k, r in rows.items() if r[1] is None or r[2] is None or r[3] is None]
if missing:
    print("  incomplete pairs:", " ".join(f"{s}->{d}" for s, d in missing))
lat = sorted((r[3], k) for k, r in rows.items() if r[3] is not None)
if lat:
    gaps = [(lat[i + 1][0] - lat[i][0], i) for i in range(len(lat) - 1)]
    g, i = max(gaps); thr = (lat[i][0] + lat[i + 1][0]) / 2
    print(f"RDMA latency: largest gap {g:.2f} us at {thr:.2f} us; {len(lat) - i - 1} of {len(lat)} pairs above it")
    if g > 0.3:
        near = defaultdict(set)
        for v, (s, d) in lat:
            if v < thr: near[s].add(d); near[d].add(s)
        groups, seen = [], set()
        for n in sorted(near, key=lambda x: int(x[1:])):
            if n in seen: continue
            grp, todo = set(), [n]
            while todo:
                x = todo.pop()
                if x in grp: continue
                grp.add(x); todo += near[x] - grp
            seen |= grp; groups.append(sorted(grp, key=lambda x: int(x[1:])))
        print("  inferred switch groups (low-latency components):", " | ".join(" ".join(g) for g in groups))
    else:
        print("  no clear gap: one switch level for all pairs")
print("slowest 5 pairs by TCP median:")
for k, r in sorted(((k, r) for k, r in rows.items() if r[2]), key=lambda x: -x[1][2][0])[:5]:
    print(f"  {k[0]}->{k[1]}: TCP {r[2][0]:.1f} us, ping {r[1]:.1f} us, RDMA {r[3]} us")
