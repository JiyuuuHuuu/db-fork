"""neon_local backend without a cluster: the compute-start request it sends to the node agent."""

import json

import pytest

from dblib import neon_local as nl


@pytest.fixture
def sent(monkeypatch):
    calls = []

    def fake_http(method, url, body=None, timeout=30):
        calls.append((method, url, body))
        return 200, {"host": "h", "port": 1, "started": True, "start_ms": 1.0}

    monkeypatch.setattr(nl, "http", fake_http)
    monkeypatch.setenv("NEON_LOCAL_TENANT_ID", "t0")
    monkeypatch.setenv("NEON_LOCAL_AGENTS", "http://agent:9000")
    monkeypatch.delenv("NEON_LOCAL_SHARED_BUFFERS", raising=False)
    monkeypatch.delenv("NEON_LOCAL_COMPUTE_OPTS", raising=False)
    return calls


def test_compute_start_body_has_only_the_basics_by_default(sent):
    nl.ensure_compute("ab" * 16)
    method, url, body = sent[-1]
    assert (method, url) == ("POST", "http://agent:9000/computes")
    assert body == {"timeline_id": "ab" * 16, "tenant_id": "t0"}


def test_compute_opts_are_merged_into_the_body(sent, monkeypatch):
    monkeypatch.setenv("NEON_LOCAL_SHARED_BUFFERS", "8MB")
    opts = {"lfc_mb": 36, "statement_log": False, "pageservers": ["a:1", "b:2"], "stripe_size": 256}
    monkeypatch.setenv("NEON_LOCAL_COMPUTE_OPTS", json.dumps(opts))
    nl.ensure_compute("cd" * 16)
    assert sent[-1][2] == {"timeline_id": "cd" * 16, "tenant_id": "t0", "shared_buffers": "8MB", **opts}


def test_numbered_branches_spread_evenly_over_agents(monkeypatch):
    agents = [f"http://a{i}:9000" for i in range(7)]
    monkeypatch.setenv("NEON_LOCAL_AGENTS", ",".join(agents))
    monkeypatch.setenv("NEON_LOCAL_RUN_NONCE", "abc123")
    counts = {}
    ids = set()
    for i in range(1, 1025):
        tl = nl.timeline_id_for(f"setup_branch_{i}")
        assert len(tl) == 32 and int(tl, 16) >= 0
        ids.add(tl)
        counts[nl.agent_for(tl)] = counts.get(nl.agent_for(tl), 0) + 1
    assert len(ids) == 1024
    assert max(counts.values()) - min(counts.values()) <= 1
    assert nl.agent_for(nl.timeline_id_for("setup_branch_1")) == agents[0]
    # unnumbered names keep the plain hash; one agent: ids unchanged
    import hashlib
    assert nl.timeline_id_for("main_copy") == hashlib.md5(b"abc123/main_copy").hexdigest()
    monkeypatch.setenv("NEON_LOCAL_AGENTS", agents[0])
    assert nl.timeline_id_for("setup_branch_5") == hashlib.md5(b"abc123/setup_branch_5").hexdigest()
