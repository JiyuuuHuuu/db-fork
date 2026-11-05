"""Experiment 12's multi-client scaling (scripts/workloads/multi_client) and safekeeper groups (scripts/setup)."""
import importlib.util
import socket
import sys
import threading
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "setup"))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


ms = _load("multi_scaling_t", ROOT / "scripts/workloads/multi_client/multi_scaling.py")
patches = _load("fc_patches_t", ROOT / "microbench/fast_client/patches.py")


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_shares():
    assert ms.shares(1, 4) == [1]
    assert ms.shares(4, 4) == [1, 1, 1, 1]
    assert ms.shares(1024, 5) == [205, 205, 205, 205, 204]
    assert sum(ms.shares(513, 4)) == 513


def _go_all(port, key, n, abort=False):
    c = ms.Coordinator(port)
    c.expect(key, n + (1 if abort else 0))
    patches.SYNC_ADDR, patches.SYNC_KEY, patches.SYNC_TIMEOUT = f"127.0.0.1:{port}", key, 30
    res = []

    def one():
        try:
            patches.sync_go()
            res.append("go")
        except RuntimeError as e:
            res.append(str(e))

    ts = [threading.Thread(target=one) for _ in range(n)]
    for t in ts:
        t.start()
    if abort:
        import time
        while c.status(key)[2] < n:
            time.sleep(0.01)
        c.abort(key, "b2 exited 1 before the start")
    for t in ts:
        t.join(30)
    c.srv.shutdown()
    return res, c.status(key)


def test_coordinator_go():
    res, (state, go, arrived) = _go_all(_free_port(), "data_READ_n4.1.0", 4)
    assert res == ["go"] * 4 and state == "go" and go and arrived == 4


def test_coordinator_abort():
    res, (state, _, _) = _go_all(_free_port(), "k.1", 3, abort=True)
    assert state == "abort" and all("ABORT b2 exited 1" in r for r in res)


def test_gate_once_per_process():
    port = _free_port()
    c = ms.Coordinator(port)
    c.expect("g", 1)                      # one client process: one connection however many threads
    patches.SYNC_ADDR, patches.SYNC_KEY, patches.SYNC_TIMEOUT = f"127.0.0.1:{port}", "g", 30
    obj = types.SimpleNamespace()
    errs = []

    def w():
        try:
            patches._gate(obj)
        except Exception as e:  # noqa: BLE001
            errs.append(e)

    ts = [threading.Thread(target=w) for _ in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(30)
    c.srv.shutdown()
    assert not errs and c.status("g")[:1] == ("go",) and c.status("g")[2] == 1


def test_agent_safekeeper_groups():
    agent = _load("agent_t", ROOT / "scripts/setup/neon_node_agent.py")
    agent.ARGS = types.SimpleNamespace(safekeepers="a:1,b:1,c:1;d:1,e:1,f:1")
    tls = [f"{i:032x}" for i in range(2000)]
    picks = [agent.safekeepers_for(t) for t in tls]
    assert set(picks) == {"a:1,b:1,c:1", "d:1,e:1,f:1"}
    assert 900 < picks.count("a:1,b:1,c:1") < 1100                  # about half each
    assert picks == [agent.safekeepers_for(t) for t in tls]          # stable per timeline
    agent.ARGS = types.SimpleNamespace(safekeepers="a:1,b:1")
    assert agent.safekeepers_for(tls[0]) == "a:1,b:1"


def test_topo_safekeeper_groups(tmp_path):
    from topo import Topo, TopoError
    src = (ROOT / "scripts/setup/topologies/cloudlab_xl170_write2sk.toml").read_text()
    t = Topo(str(ROOT / "scripts/setup/topologies/cloudlab_xl170_write2sk.toml"))
    assert t.safekeeper_groups() == [["a1", "a2", "a3"], ["a5", "a6", "a7"]]
    bad = tmp_path / "bad.toml"
    bad.write_text(src.replace("safekeeper_groups = 2", "safekeeper_groups = 4"))
    with pytest.raises(TopoError):
        Topo(str(bad))
    one = Topo(str(ROOT / "scripts/setup/topologies/cloudlab_xl170.toml"))
    assert one.safekeeper_groups() == [["a1", "a2", "a3"]]


def test_neon_local_no_purge_in_multi_client():
    calls = []
    fake = types.SimpleNamespace(purge=lambda **k: calls.append("purge"), create_timeline=lambda r, s: calls.append(("create", s)),
                                 seed_timeline=lambda: "seed", MAIN_BRANCH="main")
    fake.setup_run = lambda db, log=print: (fake.purge(), "orig")
    fake.teardown_run = lambda db, log=print: fake.purge()
    p = _load("fc_patches_t2", ROOT / "microbench/fast_client/patches.py")
    p.SYNC_ADDR, p.SYNC_KEY = "127.0.0.1:1", "k"
    p.patch_neon_local(fake)
    root = fake.setup_run("throughput_bench", log=lambda m: None)
    fake.teardown_run("throughput_bench", log=lambda m: None)
    assert "purge" not in calls and calls == [("create", "seed")] and len(root) == 32
    q = _load("fc_patches_t3", ROOT / "microbench/fast_client/patches.py")
    q.SYNC_ADDR = q.SYNC_KEY = ""
    fake2 = types.SimpleNamespace(setup_run="orig")
    q.patch_neon_local(fake2)
    assert fake2.setup_run == "orig"                                   # single-client runs: untouched


def test_merge_pools_nodes(tmp_path):
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    nodes = []
    for i, (lat, el) in enumerate([(0.001, 10.0), (0.003, 12.0)]):
        d = tmp_path / f"b{i}"
        d.mkdir()
        rows = {"op_type": [11, 15] * 100, "status": ["OK"] * 199 + ["FAILED"], "latency": [lat] * 200}
        pq.write_table(pa.table(rows), d / "x.parquet")
        s = {"run_id": "neon_local_chw3_tp_t2_b2", "elapsed_time": el, "total_ops": 100, "intended_ops": 100,
             "failed_ops": 0, "num_processes": 2, "client_cpu_cores": 1.0, "throughput": 100 / el,
             "data_ops_count": 100, "loaded_branches": 2, "remote_cpu": {}}
        (d / "s.json").write_text(json.dumps(s))
        nodes.append({"node": f"b{i}", "n": 2, "procs": 2, "summary": str(d / "s.json"), "parquet": str(d / "x.parquet")})
    m = ms.merge("READ", 4, nodes, 1.0, tmp_path)
    assert m["throughput"] == 200 / 12.0 and m["num_branches"] == 4 and m["num_processes"] == 4
    assert m["latency"]["statements"]["n"] == 200 and m["latency"]["ops"]["n"] == 198   # one FAILED op row per node
    assert abs(m["latency"]["statements"]["mean"] - 0.002) < 1e-9
    assert abs(m["client_cpu_cores"] - (10 + 12) / 12.0) < 1e-9
