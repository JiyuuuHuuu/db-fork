#!/usr/bin/env python3
"""Summarise raw/ping, raw/tcp and raw/trace into summary.txt (stdout)."""
import re, statistics as st
from pathlib import Path
D = Path(__file__).resolve().parent.parent
import sys
R = Path(sys.argv[1]) if len(sys.argv) > 1 else D / "raw"
grp = {n: n[:2] for n in ("g1a", "g1b", "g2a", "g2b")}
print(f"{'pair':10} {'path':4} {'kind':6} | {'loss':>5} {'ping min':>8} {'avg':>6} {'mdev':>6} | {'tcp p50':>7} {'p99':>6} | hops")
for f in sorted((R / "ping").glob("*.txt")):
    s, d, path = f.stem.split("_")
    kind = "same" if grp[s] == grp[d] else "cross"
    t = f.read_text()
    loss = re.search(r"([\d.]+)% packet loss", t).group(1) + "%"
    m = re.search(r"= ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+) ms", t)
    pm = f"{float(m[1])*1000:8.0f} {float(m[2])*1000:6.0f} {float(m[4])*1000:6.0f}" if m else f"{'-':>8} {'-':>6} {'-':>6}"
    try:
        v = sorted(float(x) for x in (R / "tcp" / f.name).read_text().split())
        tc = f"{st.median(v):7.0f} {v[int(.99*len(v))]:6.0f}"
    except (ValueError, IndexError):
        tc = f"{'fail':>7} {'-':>6}"
    hops = "-"
    if path == "pub":
        tr = (R / "trace" / f"{s}_{d}_pub.txt").read_text()
        h = re.search(r"hops (\d+)", tr); hops = h[1] if h else "?"
    print(f"{s+'->'+d:10} {path:4} {kind:6} | {loss:>5} {pm} | {tc} | {hops}")
print("(ping/tcp numbers in microseconds; ping = 1000 ICMP probes at 5 ms; tcp = 2000 64-byte ping-pongs, TCP_NODELAY, python)")
