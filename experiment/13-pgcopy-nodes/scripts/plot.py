#!/usr/bin/env python3
"""Figures and tables of experiment 13 (pgcopy) from data/<run>/ (scripts/pull.sh).

    uv run python experiment/13-pgcopy-nodes/scripts/plot.py [--run data/run] [--figures figures]

figures/low_load.{png,csv}       one busy branch, one client: p50 / p95 per data op; branch create (copy + start)
figures/scaling.{png,csv}        busy branches spread over the 15 hosts: throughput (sum of per-node rates) and p50
figures/ceiling.{png,csv}        the minimal asyncio client (ceiling_check) vs the runner, per op and N
figures/branch_create.{png,csv}  create latency (BRANCH rows) vs busy branches: spread over 15 hosts and on one host
figures/cold_start.{png,csv}     point-read latency on a fresh branch, rolling median per run
figures/fork_phases.{png,csv}    copy and start time per fork, by concurrent copies: p50/p95 per group (the per-fork
                                 table, from the agents' forks.jsonl, goes to data/fork_phases.csv)
Neon / Doltgres reference series: not plotted (experiments 11/12's data is not on this machine; add later).
Colours: the dataviz reference palette's categorical slots 1-4 in fixed order (blue, orange, aqua, yellow).
"""
import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

X = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"
OPS = ["READ", "RANGE_READ", "INSERT", "UPDATE"]
KIND = {"READ": "point", "RANGE_READ": "range", "INSERT": "insert", "UPDATE": "update"}
BRANCH, STATEMENTS = 1, {11, 12, 13}
W = 3


def style():
    plt.rcParams.update({
        "figure.facecolor": SURF, "axes.facecolor": SURF, "savefig.facecolor": SURF, "axes.edgecolor": GRID,
        "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "axes.grid": True,
        "grid.color": GRID, "grid.linewidth": 1, "grid.linestyle": "-", "axes.spines.top": False,
        "axes.spines.right": False, "lines.linewidth": 2, "lines.markersize": 7, "font.size": 10,
        "legend.frameon": False, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.axisbelow": True})


def line(ax, x, y, i, label):
    ax.plot(x, y, color=SERIES[i], marker="o", label=label, solid_capstyle="round",
            markeredgecolor=SURF, markeredgewidth=2)


def nticks(ax, ns):
    ns = sorted(set(int(n) for n in ns))
    ax.set_xscale("log", base=2)
    ax.set_xticks(ns, [str(n) for n in ns])
    ax.minorticks_off()


def save(fig, name, out: Path):
    fig.tight_layout()
    fig.savefig(out / f"{name}.png", dpi=150)
    plt.close(fig)
    print("wrote", out / f"{name}.png")


def summary(d: Path, glob: str):
    fs = sorted(d.glob(glob))
    return json.loads(fs[0].read_text()) if fs else None


def ms(x):
    return None if x is None else x * 1e3


# ---------------------------------------------------------------- low load
def low_load(run: Path, out: Path):
    base = run / "pgcopy" / "low_contention"
    rows = []
    for op in OPS:
        s = summary(base / "data_ops" / op, f"*_{op}_threads1_summary.json")
        if s:
            st = s["latency"]["statements"]
            rows.append({"op": op, "p50_ms": ms(st["p50"]), "p95_ms": ms(st["p95"]), "mean_ms": ms(st["mean"]),
                         "n": st["n"], "throughput": s["throughput"]})
    pqf = sorted((base / "branch_create").glob("*_b1.parquet"))
    if pqf:
        t = pq.read_table(pqf[0], columns=["op_type", "status", "latency"]).to_pandas()
        b = t[(t.op_type == BRANCH) & (t.status.astype(str).isin(["OK", "1"]))]["latency"]
        rows.append({"op": "BRANCH_CREATE", "p50_ms": b.median() * 1e3, "p95_ms": b.quantile(0.95) * 1e3,
                     "mean_ms": b.mean() * 1e3, "n": len(b), "throughput": None})
    df = pd.DataFrame(rows)
    df.to_csv(out / "low_load.csv", index=False)
    if df.empty:
        return df
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.2), gridspec_kw={"width_ratios": [4, 1.4]})
    d = df[df.op != "BRANCH_CREATE"]
    ax = axes[0]
    ax.barh(d.op, d.p50_ms, height=0.4, color=SERIES[0])
    for y, (p50, p95) in enumerate(zip(d.p50_ms, d.p95_ms)):
        ax.text(p50, y, f"  {p50:.2f} ms (p95 {p95:.2f})", va="center", color=INK, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("p50 latency per statement (ms)")
    ax.set_title("Data operations, one busy branch, one client")
    ax.set_xlim(0, d.p50_ms.max() * 2.2)
    ax = axes[1]
    c = df[df.op == "BRANCH_CREATE"]
    if not c.empty:
        ax.bar(["create"], c.p50_ms / 1e3, width=0.3, color=SERIES[1])
        ax.set_xlim(-0.6, 0.6)
        ax.text(0, c.p50_ms.iloc[0] / 1e3, f"{c.p50_ms.iloc[0] / 1e3:.2f} s", ha="center", va="bottom")
        ax.set_ylabel("p50 (s)")
        ax.set_title("Branch create")
    save(fig, "low_load", out)
    return df


# ---------------------------------------------------------------- scaling
def scaling(run: Path, out: Path):
    base = run / "pgcopy" / "scaling_data" / "data_ops"
    rows = []
    for op in OPS:
        for f in sorted((base / op).glob(f"*_{op}_threads*_summary.json")):
            s = json.loads(f.read_text())
            st = s["latency"]["statements"]
            rc = s.get("remote_cpu") or {}
            mc = (s.get("multi_client") or {}).get("nodes") or []
            slow = max(mc, key=lambda x: x["elapsed_time"]) if mc else None
            # throughput = sum of each node's own rate (all nodes start together; a slow node does not stretch
            # the others' windows). throughput_slowest_window = all ops / the longest node window, the shared
            # driver's merged number: one faulty node (n4, README) drags it down, so it is kept only for reference.
            rows.append({"op": op, "n": s["num_threads"],
                         "throughput": sum(x["throughput"] for x in mc) if mc else s["throughput"],
                         "throughput_slowest_window": s["throughput"],
                         "slowest_node": slow["node"] if slow else None,
                         "window_s": s.get("elapsed_time"),
                         "median_node_window_s": float(np.median([x["elapsed_time"] for x in mc])) if mc else None,
                         "p50_ms": ms(st.get("p50")),
                         "p95_ms": ms(st.get("p95")), "procs": s.get("num_processes"),
                         "client_cpu_cores": s.get("client_cpu_cores"),
                         "nodes": len((s.get("multi_client") or {}).get("nodes") or [1]),
                         "n1_node_cores": (rc.get("server") or {}).get("node_cores"),
                         "n1_postgres_cores": (rc.get("computes") or {}).get("proc_cores"),
                         "seed_node_cores": (rc.get("seed") or {}).get("node_cores")})
    df = pd.DataFrame(rows).sort_values(["op", "n"])
    df.to_csv(out / "scaling.csv", index=False)
    if df.empty:
        return df
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    for i, op in enumerate(OPS):
        d = df[df.op == op]
        line(axes[0], d.n, d.throughput / 1e3, i, op)
        line(axes[1], d.n, d.p50_ms, i, op)
    for ax in axes:
        nticks(ax, df.n)
        ax.set_xlabel("busy branches N (spread over the hosts)")
    axes[0].set_ylabel("throughput (k statements/s)")
    axes[0].set_title("Throughput (sum of per-node rates)")
    axes[1].set_ylabel("p50 latency (ms)")
    axes[1].set_title("Latency")
    axes[0].legend(loc="upper left")
    save(fig, "scaling", out)
    return df


def ceiling(run: Path, out: Path, sc: pd.DataFrame):
    f = run / "pgcopy" / "scaling_data" / "ceiling_check.csv"
    if not f.exists():
        return None
    c = pd.read_csv(f)
    c.to_csv(out / "ceiling.csv", index=False)
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    for i, op in enumerate(OPS):
        d = c[c.kind == KIND[op]]
        if d.empty:
            continue
        line(ax, d.busy_branches, d.ops_per_s / 1e3, i, op)
        r = sc[(sc.op == op) & sc.n.isin(d.busy_branches)] if sc is not None and not sc.empty else None
        if r is not None and not r.empty:
            ax.plot(r.n, r.throughput / 1e3, color=SERIES[i], marker="s", linestyle="none", markeredgecolor=SURF,
                    markeredgewidth=2)
    nticks(ax, c.busy_branches)
    ax.set_xlabel("busy branches N")
    ax.set_ylabel("throughput (k statements/s)")
    ax.set_title("Ceiling check: minimal client (lines) vs runner (squares)")
    ax.legend(fontsize=8, loc="center left", bbox_to_anchor=(1.01, 0.5))
    fig.set_size_inches(8.5, 3.8)
    save(fig, "ceiling", out)
    return c


# ---------------------------------------------------------------- branch create
def creates_of(d: Path):
    out = []
    for f in sorted(d.glob("*_b*.parquet")):
        if f.name.endswith("_setup.parquet"):
            continue
        n = int(f.stem.rsplit("_b", 1)[1])
        t = pq.read_table(f, columns=["op_type", "status", "latency"]).to_pandas()
        b = t[(t.op_type == BRANCH) & (t.status.astype(str).isin(["OK", "1"]))]["latency"]
        if len(b):
            out.append({"n": n, "p50_ms": b.median() * 1e3, "p95_ms": b.quantile(0.95) * 1e3,
                        "mean_ms": b.mean() * 1e3, "creates": len(b)})
    return pd.DataFrame(out)


def creates_multi(d: Path):
    """Multi-node points: one merged summary per total N (at d), the creator's rows in n1's parquet. n1's share is
    the same for several N (N = 1 and 4 both give it 1 branch), and node_run.sh moves an earlier point's files with
    the same name to n1/superseded_<t>/; so each merged point is matched to the parquet whose BRANCH rows fall in
    its timed window (GO .. GO + window)."""
    cands = [f for f in list((d / "n1").glob("*.parquet")) + list((d / "n1").glob("superseded_*/*.parquet"))
             if not f.name.endswith("_setup.parquet")]
    tables = []
    for f in cands:
        t = pq.read_table(f, columns=["op_type", "status", "latency", "start_time"]).to_pandas()
        tables.append(t[(t.op_type == BRANCH) & (t.status.astype(str).isin(["OK", "1"]))])
    out = []
    for f in sorted(d.glob("*_BRANCH_CREATE_threads1_summary.json")):
        s = json.loads(f.read_text())
        go, win = s["multi_client"]["go_time"], s["elapsed_time"]
        for b in tables:
            inw = b[b.start_time.between(go - 1, go + win + 1)]
            if len(inw) >= 10:
                lat = inw["latency"]
                out.append({"n": s["num_branches"], "p50_ms": lat.median() * 1e3, "p95_ms": lat.quantile(0.95) * 1e3,
                            "mean_ms": lat.mean() * 1e3, "creates": len(lat),
                            "background_ops_per_s": s["multi_client"]["background_ops_in_window"] / win})
                break
    return pd.DataFrame(out)


def branch_create(run: Path, out: Path):
    frames = []
    m = creates_multi(run / "pgcopy" / "scaling_branch" / "branch_create" / "mixed")
    if not m.empty:
        m["series"] = "spread over 15 hosts"
        frames.append(m)
    sgl = creates_of(run / "single" / "pgcopy" / "scaling_branch" / "branch_create" / "mixed")
    if not sgl.empty:
        sgl["series"] = "one host"
        frames.append(sgl)
    if not frames:
        return None
    df = pd.concat(frames).sort_values(["series", "n"])
    df.to_csv(out / "branch_create.csv", index=False)
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    for i, (label, d) in enumerate(df.groupby("series", sort=False)):
        line(ax, d.n, d.p50_ms / 1e3, i, f"{label}: p50")
        ax.fill_between(d.n, d.p50_ms / 1e3, d.p95_ms / 1e3, color=SERIES[i], alpha=0.15, linewidth=0)
    nticks(ax, df.n)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("busy branches N (background MIXED)")
    ax.set_ylabel("branch create (s)")
    ax.set_title("Branch create = copy + start (band: p50 to p95)")
    ax.legend()
    save(fig, "branch_create", out)
    return df


# ---------------------------------------------------------------- cold start
def cold_start(run: Path, out: Path):
    d = run / "pgcopy" / "cold_start"
    if not (d / "runs.json").exists():
        return None
    meta = json.loads((d / "runs.json").read_text())
    rows = []
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    for i, r in enumerate(meta["runs"]):
        t = pd.read_parquet(d / f"run{r['run']}.parquet")
        roll = t.latency_s.rolling(500, min_periods=50).median() * 1e3
        ax.plot(t.i, roll, color=SERIES[i % 4], label=f"run {r['run']}")
        rows.append({k: r.get(k) for k in ("run", "create_ms", "agent_copy_ms", "agent_start_ms", "first_connect_ms",
                                            "median_first_100_ms", "median_last_5000_ms", "median_last_10pct_ms",
                                            "copied_bytes")})
    ax.set_xlabel("read number")
    ax.set_ylabel("point read, rolling median of 500 (ms)")
    ax.set_title("Cold start: reads on a freshly copied branch")
    ax.set_ylim(bottom=0)
    ax.legend()
    save(fig, "cold_start", out)
    df = pd.DataFrame(rows)
    df.to_csv(out / "cold_start.csv", index=False)
    return df


# ---------------------------------------------------------------- fork phases
def fork_phases(out: Path, data: Path):
    from fork_phases import load
    df = load(data)
    if df.empty:
        return None
    df.to_csv(data / "fork_phases.csv", index=False)   # one row per fork: raw, stays out of git
    bins = [-1, 0, 3, 15, 63, 10 ** 6]
    labels = ["0", "1-3", "4-15", "16-63", "64+"]
    df["others"] = pd.cut(df.concurrent_copies, bins=bins, labels=labels)
    agg = df.groupby(["kind", "others"], observed=True).agg(
        forks=("name", "size"),
        copy_ms_p50=("copy_ms", "median"), copy_ms_p95=("copy_ms", lambda v: v.quantile(0.95)),
        start_ms_p50=("start_ms", "median"), start_ms_p95=("start_ms", lambda v: v.quantile(0.95)),
        total_ms_p50=("total_ms", "median"), total_ms_p95=("total_ms", lambda v: v.quantile(0.95)),
        mb_p50=("mb", "median"), mb_p95=("mb", lambda v: v.quantile(0.95)),
        gib_copied=("bytes", lambda v: v.sum() / 2 ** 30)).round(1)
    agg.reset_index().rename(columns={"others": "other_copies"}).to_csv(out / "fork_phases.csv", index=False)
    br = df[df.kind == "branch"].copy()
    g = br.groupby("others", observed=True)[["copy_ms", "start_ms"]].median() / 1e3
    cnt = br.groupby("others", observed=True).size()
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    x = np.arange(len(g))
    ax.bar(x, g.copy_ms, width=0.5, color=SERIES[0], label="copy (pg_basebackup)")
    ax.bar(x, g.start_ms, width=0.5, bottom=g.copy_ms, color=SERIES[1], label="start (recovery incl. data-dir fsync)",
           edgecolor=SURF, linewidth=2)
    for xi, (c, s, n) in enumerate(zip(g.copy_ms, g.start_ms, cnt)):
        ax.text(xi, c + s, f"{c + s:.1f} s\nn={n}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x, g.index.astype(str))
    ax.set_xlabel("other copies running at the same time (any node)")
    ax.set_ylabel("median seconds per fork")
    ax.set_title("Where a fork's time goes")
    ax.set_ylim(0, (g.copy_ms + g.start_ms).max() * 1.3)
    ax.legend(loc="upper left")
    save(fig, "fork_phases", out)
    return br


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", default=str(X / "data" / "run"))
    ap.add_argument("--data", default=str(X / "data"), help="holds archive/<node>/forks.jsonl")
    ap.add_argument("--figures", default=str(X / "figures"))
    a = ap.parse_args()
    run, out = Path(a.run), Path(a.figures)
    out.mkdir(parents=True, exist_ok=True)
    style()
    low_load(run, out)
    sc = scaling(run, out)
    ceiling(run, out, sc)
    branch_create(run, out)
    cold_start(run, out)
    fork_phases(out, Path(a.data))


if __name__ == "__main__":
    main()
