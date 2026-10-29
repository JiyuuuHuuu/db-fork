"""Tests for scripts/setup/topo.py: parsing, validation, addresses and the generated environment files."""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts" / "setup"))

import topo  # noqa: E402

TOPOS = REPO / "scripts" / "setup" / "topologies"


def write(tmp_path, text):
    p = tmp_path / "t.toml"
    p.write_text(text)
    return p


ONE_HOST = """
state_dir = "/tmp/bench"
[nodes.local]
host = "local"
server_cpus = "2-38:4"
client_cpus = "3-11:4"
[neon]
build_dir = "/x"
transport = "uds"
client = "local"
storcon = "local"
broker = "local"
seed_compute = "local"
pageservers = ["local"]
safekeepers = ["local"]
compute_agents = ["local"]
"""


@pytest.mark.parametrize("f", sorted(TOPOS.glob("*.toml")), ids=lambda p: p.stem)
def test_examples_load(f):
    t = topo.Topo(f)
    for b in ("neon", "doltgres"):
        if getattr(t, b):
            v = t.shell_vars(b)
            assert v["ENV_FILE"].endswith(f"/{b}.env")
            assert v["MAX_PROCS"] > 0


def test_expand_cpus():
    assert topo.expand_cpus("2-14:4,40,41-42") == [2, 6, 10, 14, 40, 41, 42]


def test_uds_env(tmp_path):
    t = topo.Topo(write(tmp_path, ONE_HOST))
    env = t.neon_env(seed={"tenant_id": "t1", "seed_timeline_id": "s1"})
    run = "/tmp/bench/neon/run"
    assert env["NEON_LOCAL_STORCON_URL"] == "http+unix://" + f"{run}/storcon.sock".encode().hex()
    assert env["NEON_LOCAL_AGENTS"] == "http+unix://" + f"{run}/agent.sock".encode().hex()
    assert env["NEON_LOCAL_TENANT_ID"] == "t1"
    assert t.listen("ps", "ps_pg", "pg") == f"unix:{run}/.s.PGSQL.64000"
    assert t.max_procs("neon") == 3


def test_uds_needs_one_host(tmp_path):
    text = ONE_HOST.replace('compute_agents = ["local"]', 'compute_agents = ["b"]') + \
        '[nodes.b]\nhost = "b.example"\nlan = "10.0.0.2"\n'
    with pytest.raises(topo.TopoError, match="uds"):
        topo.Topo(write(tmp_path, text))


def test_unknown_node(tmp_path):
    with pytest.raises(topo.TopoError, match="unknown node"):
        topo.Topo(write(tmp_path, ONE_HOST.replace('broker = "local"', 'broker = "nope"')))


def test_unknown_workload_key(tmp_path):
    with pytest.raises(topo.TopoError, match="unknown workload"):
        topo.Topo(write(tmp_path, ONE_HOST + "[workload]\nWW = 3\n"))


def test_backend_workload_override(tmp_path):
    t = topo.Topo(write(tmp_path, ONE_HOST + "[neon.workload]\nns_data = [1, 4]\n"))
    assert t.wl("neon")["ns_data"] == [1, 4]
    assert t.wl(None)["ns_data"] == topo.WORKLOAD_DEFAULTS["ns_data"]


def test_cross_zone_addresses():
    t = topo.Topo(TOPOS / "cloudlab_two_zone.toml")
    env = t.neon_env()  # viewer: the client b0 in Utah
    assert env["NEON_LOCAL_STORCON_URL"] == "http://128.105.145.147:1234"     # a0, other zone: public
    assert env["NEON_LOCAL_AGENTS"].split(",")[0] == "http://10.10.2.11:9000"  # b1, same zone: LAN
    seed_env = t.neon_env("a4", seeding=True)                                  # seeding from a4 in Wisconsin
    assert seed_env["NEON_LOCAL_STORCON_URL"] == "http://10.10.1.10:1234"
    assert seed_env["NEON_LOCAL_SHARED_BUFFERS"] == "8GB"
    assert seed_env["NEON_LOCAL_AGENTS"] == "http://127.0.0.1:9000"


def test_env_file_refuses_dotenv(tmp_path):
    t = topo.Topo(write(tmp_path, ONE_HOST))
    with pytest.raises(topo.TopoError):
        topo.write_env(t, "neon", out=str(tmp_path / ".env"))


def test_env_text_quotes_json():
    txt = topo.env_text({"A": "x", "B": '{"k": 1}'})
    assert "A=x\n" in txt and "B='{\"k\": 1}'" in txt
