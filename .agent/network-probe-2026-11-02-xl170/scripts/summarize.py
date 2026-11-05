#!/usr/bin/env python3
"""Summarise a probe.sh output dir (raw/<run>/{ping,tcp,rdma,trace}) -> per-pair table + per-class aggregate.

Usage: summarize.py RAWDIR   (prints to stdout)
Classes: same/cross zone (first letter of node name) x path (lan = 10.10.x experimental, pub = control network).
"""
import re
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

R = Path(sys.argv[1])


def ping(f):
    t = f.read_text()
    m = re.search(r"([\d.]+)% packet loss", t)
    if not m:
        return None, None  # incomplete
    loss = float(m.group(1))
    m = re.search(r"= ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) ms", t)
    return loss, (tuple(float(x) * 1000 for x in m.groups()) if m else None)


def tcp(f):
    try:
        v = sorted(float(x) for x in f.read_text().split())
        return st.median(v), v[int(0.99 * len(v))]
    except (ValueError, IndexError, FileNotFoundError):
        return None


def rdma(f):
    if not f.exists():
        return None
    m = re.search(r"^\s*2\s+1000\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", f.read_text(), re.M)
    return float(m[3]) if m else None  # t_typical (half round trip)


rows, agg = [], defaultdict(lambda: defaultdict(list))
for f in sorted((R / "ping").glob("*.txt")):
    s, d, path = f.stem.split("_")
    kind = "same" if s[0] == d[0] else "cross"
    loss, pm = ping(f)
    if loss is None:
        continue
    tc = tcp(R / "tcp" / f.name) if not (kind == "cross" and path == "lan") else None
    rd = rdma(R / "rdma" / f.name)
    hops = "-"
    if path == "pub" and (R / "trace" / f"{s}_{d}_pub.txt").exists():
        tr = (R / "trace" / f"{s}_{d}_pub.txt").read_text()
        h = re.search(r"hops (\d+)", tr)
        hops = h[1] if h else "?"
    rows.append((s, d, path, kind, loss, pm, tc, rd, hops))
    a = agg[(kind, path)]
    a["loss"].append(loss)
    if pm:
        a["ping"].append(pm[1])
    if tc:
        a["tcp50"].append(tc[0]); a["tcp99"].append(tc[1])
    if rd:
        a["rdma"].append(rd)

fmt = lambda v, w=6: f"{v:{w}.0f}" if v is not None else f"{'-':>{w}}"
print("Per class (ranges are min-max of per-pair values; us)")
print(f"{'class':12} {'pairs':>5} {'loss%':>6} | {'ping avg':>15} | {'tcp p50':>15} | {'tcp p99':>15} | {'rdma write lat':>14}")
for (kind, path), a in sorted(agg.items()):
    rng = lambda k, p=0: (f"{min(a[k]):7.{p}f}-{max(a[k]):<7.{p}f}" if a[k] else f"{'-':>15}")
    print(f"{kind + ' ' + path:12} {len(a['loss']):5} {max(a['loss']):6.0f} | {rng('ping')} | {rng('tcp50')} | {rng('tcp99')} | {rng('rdma', 2):>14}")
print()
print(f"{'pair':8} {'path':4} {'kind':6} | {'loss':>5} {'min':>6} {'avg':>6} {'mdev':>5} | {'tcp p50':>7} {'p99':>6} | {'rdma':>5} | hops")
for s, d, path, kind, loss, pm, tc, rd, hops in rows:
    p = f"{pm[0]:6.0f} {pm[1]:6.0f} {pm[3]:5.0f}" if pm else f"{'-':>6} {'-':>6} {'-':>5}"
    t = f"{tc[0]:7.0f} {tc[1]:6.0f}" if tc else f"{'-':>7} {'-':>6}"
    r = f"{rd:5.2f}" if rd else f"{'-':>5}"
    print(f"{s + '->' + d:8} {path:4} {kind:6} | {loss:4.0f}% {p} | {t} | {r} | {hops}")
print("(us; ping = 500 ICMP probes at 5 ms (cross-zone lan: 3 probes, reachability only); tcp = 2000 64-byte "
      "ping-pongs, TCP_NODELAY, python; rdma = ib_write_lat t_typical, 2 B, half round trip, over lan for same zone "
      "and over the control network for cross zone)")
