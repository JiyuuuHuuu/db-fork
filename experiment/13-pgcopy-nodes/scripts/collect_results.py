#!/usr/bin/env python3
"""Copy experiment 13's aggregated results from data/ (raw, on /mydata, not in git) to results/ (in git).

    uv run python experiment/13-pgcopy-nodes/scripts/collect_results.py [--data DIR] [--out DIR]

Kept, same relative paths as under data/:
  run/**, smoke/**  *_summary.json  runner summaries: per point and op, count, throughput, latency percentiles,
                                    backend_observations, remote_cpu (one per node under nN/, the merged one above it)
                    ceiling/nN.csv, ceiling_check.csv   minimal-client ceiling, per node and merged
                    cold_start/runs.json                cold-start runs: create time, first connect, read percentiles
Written:
  archive/agents.csv  per node: copies the agent made (all of them, also the superseded attempts and run roots;
                      figures/fork_phases.csv leaves the untrimmed-SSD window out), GiB copied, failures, trims
Left out (raw data and execution logs): *.parquet (one row per operation), *.log / *.out / progress logs, the
agents' forks/failures/trims.jsonl (one line per event; figures/fork_phases.csv aggregates the forks),
nodes/<n>/ (each host's own copy of what run/ and smoke/ already hold), kept statement logs.
"""
import argparse
import json
import shutil
from pathlib import Path

X = Path(__file__).resolve().parents[1]
KEEP = ["*_summary.json", "ceiling/n*.csv", "ceiling_check.csv", "cold_start/runs.json"]


def jsonl(f: Path) -> list:
    return [json.loads(x) for x in f.read_text().splitlines() if x.strip()] if f.exists() else []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=str(X / "data"))
    ap.add_argument("--out", default=str(X / "results"))
    a = ap.parse_args()
    data, out = Path(a.data).resolve(), Path(a.out)
    n = 0
    for top in ("run", "smoke"):
        files = sorted({f for pat in KEEP for f in (data / top).rglob(pat) if f.is_file()})
        for f in files:
            dst = out / f.relative_to(data)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, dst)
            n += 1
    rows = []
    for d in sorted((data / "archive").iterdir(), key=lambda p: int(p.name[1:]) if p.name[1:].isdigit() else 999):
        if not d.is_dir():
            continue
        forks = [r for r in jsonl(d / "forks.jsonl") if not r.get("init")]
        rows.append(dict(node=d.name, forks=len(forks), gib_copied=round(sum(r.get("bytes") or 0 for r in forks) / 2**30, 1),
                         failures=len(jsonl(d / "failures.jsonl")), trims=len(jsonl(d / "trims.jsonl"))))
    (out / "archive").mkdir(parents=True, exist_ok=True)
    keys = list(rows[0])
    lines = [",".join(keys)] + [",".join(str(r[k]) for k in keys) for r in rows]
    tot = {k: sum(r[k] for r in rows) for k in keys[1:]}
    lines.append("total," + ",".join(str(round(tot[k], 1)) for k in keys[1:]))
    (out / "archive" / "agents.csv").write_text("\n".join(lines) + "\n")
    print(f"copied {n} files to {out}; wrote archive/agents.csv ({len(rows)} nodes, {tot['forks']} forks, "
          f"{tot['gib_copied']:.0f} GiB, {tot['failures']} failures)")


if __name__ == "__main__":
    main()
