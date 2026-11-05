#!/usr/bin/env python3
"""Every fork of experiment 13 from the agents' archives: copy and start time, bytes copied, and how many other copies
read from the seed node at the same moment.

    uv run python experiment/13-pgcopy-nodes/scripts/fork_phases.py [--data DIR] [--out CSV]

Reads DIR/archive/<node>/forks.jsonl (scripts/pull.sh; one line per instance an agent created: t0 = copy start,
t_copied, t_ready, copy_ms, start_ms, bytes, parent) and writes one row per copy (seed initdb excluded) with
concurrent_copies = number of copies (any node) whose [t0, t_copied] overlaps this one's start. Prints the
distribution for idle forks (no other copy running: the timed creates and cold starts) and for setup copies.
"""
import argparse
import json
from pathlib import Path

import pandas as pd

X = Path(__file__).resolve().parents[1]


# The superseded scaling_branch attempt ran on SSDs that had never been trimmed (fork start on n1 grew from 1.1 s to
# 3.3 s); its forks are left out (README, "Superseded"). UTC epoch seconds.
EXCLUDE = [(1793875140.0, 1793880660.0)]   # 2026-11-05 10:39 - 12:11 UTC


def load(data: Path, exclude=EXCLUDE) -> pd.DataFrame:
    rows = []
    for f in sorted((data / "archive").glob("*/forks.jsonl")):
        node = f.parent.name
        for line in f.read_text().splitlines():
            r = json.loads(line)
            if r.get("init"):
                continue
            r["node"] = node
            rows.append(r)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("t0").reset_index(drop=True)
    t0, t1 = df["t0"].to_numpy(), df["t_copied"].to_numpy()
    df["concurrent_copies"] = [int(((t0 <= a) & (t1 > a)).sum()) - 1 for a in t0]
    for a, b in exclude:
        df = df[~df["t0"].between(a, b)]
    df = df.reset_index(drop=True)
    df["mb"] = df["bytes"] / 2**20
    df["kind"] = df["parent"].map(lambda p: "run_root" if p == "seed" else "branch")
    df["total_ms"] = df["copy_ms"] + df["start_ms"].fillna(0)
    return df


def describe(df: pd.DataFrame, label: str) -> None:
    if df.empty:
        print(f"{label}: none")
        return
    q = df[["copy_ms", "start_ms", "total_ms", "mb"]].quantile([0.5, 0.95]).round(0)
    print(f"{label}: n={len(df)}  copy p50 {q.copy_ms[0.5]:.0f} ms p95 {q.copy_ms[0.95]:.0f}  start p50 "
          f"{q.start_ms[0.5]:.0f} p95 {q.start_ms[0.95]:.0f}  total p50 {q.total_ms[0.5]:.0f}  size p50 "
          f"{q.mb[0.5]:.0f} MB p95 {q.mb[0.95]:.0f}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=str(X / "data"))
    ap.add_argument("--out", default=str(X / "data" / "fork_phases.csv"))
    a = ap.parse_args()
    df = load(Path(a.data))
    if df.empty:
        raise SystemExit("no forks.jsonl under data/archive (scripts/pull.sh)")
    br = df[df.kind == "branch"]
    describe(df[df.kind == "run_root"], "run roots (seed -> run root, on the seed node)")
    describe(br[br.concurrent_copies == 0], "branch copies with no other copy running")
    describe(br[br.concurrent_copies.between(1, 15)], "branch copies with 1-15 others")
    describe(br[br.concurrent_copies > 15], "branch copies with > 15 others")
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, index=False)
    print("wrote", a.out)


if __name__ == "__main__":
    main()
