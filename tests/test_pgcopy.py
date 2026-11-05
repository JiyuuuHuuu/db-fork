"""pgcopy backend (experiment 13) without a cluster: naming, placement, the agent requests, the multi-client patches
and the topology's [pgcopy] section."""
import importlib.util
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

from dblib import pgcopy as pc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "setup"))
import topo as topo_mod  # noqa: E402

TOPO = ROOT / "scripts/setup/topologies/cloudlab_pgcopy.toml"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def env(monkeypatch):
    for k in ("PGCOPY_LOCAL_AGENT", "PGCOPY_SHARED_BUFFERS", "PGCOPY_INSTANCE_OPTS", "PGCOPY_START_ON_BRANCH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("PGCOPY_ROOT_AGENT", "http://root:9100")
    monkeypatch.setenv("PGCOPY_AGENTS", "http://h1:9100,http://h2:9100,http://h3:9100")
    monkeypatch.setenv("PGCOPY_RUN_NONCE", "n0nce")
    monkeypatch.setenv("PGCOPY_RUN_ROOT", "root_abc")
    return monkeypatch


@pytest.fixture
def sent(env):
    calls = []
    insts = {"root_abc": {"name": "root_abc", "host": "10.10.1.1", "port": 24001, "running": True}}

    def fake_http(method, url, body=None, timeout=30):
        calls.append((method, url, body))
        if method == "GET" and "/instances/" in url:
            name = url.rsplit("/", 1)[1]
            return (200, insts[name]) if name in insts else (404, {"error": "no"})
        if method == "POST" and url.endswith("/instances"):
            return 200, {"name": body["name"], "host": "10.10.1.3", "port": 24005, "copy_ms": 210.0,
                         "start_ms": 95.0, "bytes": 300 << 20, "started": True, "running": True}
        if method == "DELETE" and url.endswith("/instances"):
            return 200, {"deleted": 2}
        return 200, {}

    env.setattr(pc, "http", fake_http)
    return calls


def test_instance_ids_are_deterministic_and_main_is_the_run_root(env):
    assert pc.instance_id_for("main") == "root_abc"
    a, b = pc.instance_id_for("setup_branch_7"), pc.instance_id_for("setup_branch_7")
    assert a == b and len(a) == 32 and int(a, 16) >= 0
    env.setenv("PGCOPY_RUN_NONCE", "other")
    assert pc.instance_id_for("setup_branch_7") != a


def test_numbered_branches_spread_round_robin_without_a_local_agent(env):
    agents = pc.agents()
    got = [pc.agent_for(pc.instance_id_for(f"setup_branch_{i}")) for i in range(1, 7)]
    assert got == [agents[(i - 1) % 3] for i in range(1, 7)]


def test_roots_and_seed_live_on_the_root_agent_and_local_agent_wins(env):
    assert pc.agent_for("seed") == "http://root:9100"
    assert pc.agent_for("root_abc") == "http://root:9100"
    assert pc.agent_for("root_fffeee") == "http://root:9100"     # another node's run root
    env.setenv("PGCOPY_LOCAL_AGENT", "http://h2:9100")
    assert pc.agent_for(pc.instance_id_for("setup_branch_1")) == "http://h2:9100"
    assert pc.connect_host("http://h2:9100", "10.10.1.3") == "127.0.0.1"
    assert pc.connect_host("http://h3:9100", "10.10.1.250") == "10.10.1.250"


def test_create_asks_the_parent_address_then_posts_to_the_branch_agent(sent, env):
    env.setenv("PGCOPY_LOCAL_AGENT", "http://h2:9100")
    env.setenv("PGCOPY_SHARED_BUFFERS", "40MB")
    env.setenv("PGCOPY_INSTANCE_OPTS", '{"statement_log": false}')
    r = pc.create_instance("x1", "root_abc")
    assert sent[0][:2] == ("GET", "http://root:9100/instances/root_abc")
    method, url, body = sent[1]
    assert (method, url) == ("POST", "http://h2:9100/instances")
    assert body == {"name": "x1", "init": False, "parent_role": False, "start": True, "shared_buffers": "40MB",
                    "statement_log": False, "parent": {"host": "10.10.1.1", "port": 24001, "name": "root_abc"}}
    assert r["agent"] == "http://h2:9100"


def test_start_on_branch_off_copies_only(sent, env):
    env.setenv("PGCOPY_START_ON_BRANCH", "0")
    pc.create_instance("x2", "root_abc", agent="http://h1:9100")
    assert sent[-1][2]["start"] is False


def test_purge_deletes_on_every_agent_root_first(sent):
    assert pc.purge(log=lambda *_: None) == 8
    urls = [u for m, u, _ in sent if m == "DELETE"]
    assert urls[0] == "http://root:9100/instances" and len(urls) == 4


def test_branch_records_the_fork_phases(sent, env):
    pc.PgCopyToolSuite.OBSERVED.clear()
    env.setenv("PGCOPY_LOCAL_AGENT", "http://h2:9100")
    from dblib import result_collector as rc
    from dblib.db_api import Ref
    s = pc.PgCopyToolSuite(rc.ResultCollector())
    s._branch_impl("b1", Ref("main"))
    assert pc.PgCopyToolSuite.OBSERVED == {"forks": 1, "fork_copy_ms": 210.0, "fork_start_ms": 95.0,
                                           "fork_bytes": 300 << 20}
    assert pc.PgCopyToolSuite._INSTANCES[pc.instance_id_for("b1")] == ("127.0.0.1", 24005)


def test_main_cannot_be_deleted(env):
    from dblib import result_collector as rc
    from dblib.db_api import Ref
    with pytest.raises(ValueError):
        pc.PgCopyToolSuite(rc.ResultCollector())._delete_impl(Ref("main"))


# ---------------------------------------------------------------- multi-client patches
def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_multi_client_setup_run_does_not_purge(env, monkeypatch):
    patches = _load("fc_patches_pgc", ROOT / "microbench/fast_client/patches.py")
    patches.SYNC_ADDR, patches.SYNC_KEY = "127.0.0.1:1", "k"
    import types
    calls = []
    mod = types.SimpleNamespace(new_run=lambda db: calls.append(("new_run", db)) or "root_x",
                                create_run_root=lambda root, log=print: calls.append(("root", root)),
                                purge=lambda **k: calls.append(("purge",)))
    patches.patch_pgcopy(mod)
    assert mod.setup_run("bench", log=lambda *_: None) == "root_x"
    mod.teardown_run("bench", log=lambda *_: None)
    assert calls == [("new_run", "bench"), ("root", "root_x")]


def test_hold_patch_blocks_the_one_create_until_released():
    ms = _load("multi_scaling_pgc", ROOT / "scripts/workloads/multi_client/multi_scaling.py")
    patches = _load("fc_patches_hold", ROOT / "microbench/fast_client/patches.py")
    port = _free_port()
    coord = ms.Coordinator(port)
    coord.expect("h", 10 ** 9)
    patches.SYNC_ADDR, patches.HOLD_KEY, patches.SYNC_TIMEOUT = f"127.0.0.1:{port}", "h", 30
    import types

    class Op:
        def execute(self, context):
            raise AssertionError("the original create must not run")
    patches.patch_branch_ops(types.SimpleNamespace(BranchCreateOperation=Op))
    done = []
    t = threading.Thread(target=lambda: done.append(Op().execute(None)))
    t.start()
    time.sleep(0.3)
    assert not done and coord.status("h")[2] == 1
    coord.release("h")
    t.join(5)
    assert done == [None]


# ---------------------------------------------------------------- topology
def test_topology_views():
    t = topo_mod.Topo(TOPO)
    e3 = t.pgcopy_env("n3")
    assert e3["PGCOPY_LOCAL_AGENT"] == "http://10.10.1.4:9100"
    assert e3["PGCOPY_ROOT_AGENT"] == "http://10.10.1.1:9100"
    assert len(e3["PGCOPY_AGENTS"].split(",")) == 15 and "10.10.1.1:" not in e3["PGCOPY_AGENTS"]
    assert e3["PGCOPY_SHARED_BUFFERS"] == "40MB"
    assert e3["DBFORK_REMOTE_CPU"].split(";")[0] == "server=http://10.10.1.4:9010/cpu?set=server"
    assert "seed=http://10.10.1.1:9010/cpu?set=server" in e3["DBFORK_REMOTE_CPU"]
    e0 = t.pgcopy_env("n0", seeding=True)
    assert e0["PGCOPY_LOCAL_AGENT"] == "" and e0["PGCOPY_SHARED_BUFFERS"] == "1GB" and e0["DBFORK_REMOTE_CPU"] == ""
    v = t.shell_vars("pgcopy")
    assert (v["RUNNER_BACKEND"], v["SEED_DB"], v["CLIENT_NODE"], v["SERVER_SAME_HOST"]) == ("pgcopy", "bench", "n1", 0)
    assert v["ENV_FILE"].endswith("/pgcopy.env") and v["SB"] == "40MB"
    coord = topo_mod.Node("coordinator", {}, t)
    assert t.node("n5").addr_from(coord) == "128.110.218.55"   # the coordinator sees public addresses


def test_seed_cannot_be_a_host(tmp_path):
    p = tmp_path / "t.toml"
    p.write_text('state_dir = "/tmp/x"\n[pgcopy]\nseed = "a"\nhosts = ["a", "b"]\n'
                 '[nodes.a]\nhost = "ha"\nlan = "10.0.0.1"\n[nodes.b]\nhost = "hb"\nlan = "10.0.0.2"\n')
    with pytest.raises(topo_mod.TopoError):
        topo_mod.Topo(p)
