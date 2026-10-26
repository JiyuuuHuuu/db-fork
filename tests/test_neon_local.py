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


def test_unix_url_round_trip():
    u = nl.unix_url("/mydata/jiyu/neon-exp10/run/storcon.sock", "/v1/tenant")
    assert u == "http+unix://" + b"/mydata/jiyu/neon-exp10/run/storcon.sock".hex() + "/v1/tenant"
    assert nl.split_unix_url(u) == ("/mydata/jiyu/neon-exp10/run/storcon.sock", "/v1/tenant")
    # services' base URLs get paths appended by string concatenation
    base = nl.unix_url("/a/b.sock")
    assert nl.split_unix_url(base) == ("/a/b.sock", "/")
    assert nl.split_unix_url(base + "/x?y=1") == ("/a/b.sock", "/x?y=1")
    assert nl.split_unix_url("http://h:1/x") is None


def test_http_over_a_unix_socket(tmp_path):
    import http.server
    import socketserver
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def _reply(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n)) if n else None
            if self.path == "/missing":
                out, code = b"not here", 404
            else:
                out, code = json.dumps({"method": self.command, "path": self.path, "body": body}).encode(), 200
            self.send_response(code)
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        do_GET = do_POST = do_DELETE = _reply

        def log_message(self, *a):
            pass

        def address_string(self):
            return "unix"

    class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
        daemon_threads = True

    sock = str(tmp_path / "h.sock")
    srv = Server(sock, Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        base = nl.unix_url(sock)
        assert nl.http("POST", base + "/computes", {"a": 1}) == (
            200, {"method": "POST", "path": "/computes", "body": {"a": 1}})
        assert nl.http("GET", base + "/v1/x?y=2")[1]["path"] == "/v1/x?y=2"
        assert nl.http("GET", base + "/missing") == (404, "not here")
    finally:
        srv.shutdown()
        srv.server_close()


def test_compute_host_may_be_a_socket_directory(monkeypatch):
    seen = {}

    def fake_http(method, url, body=None, timeout=30):
        return 200, {"host": "/run/c0", "port": 20000, "started": True, "start_ms": 5.0}

    def fake_connect(**kw):
        seen.update(kw)

        class C:
            autocommit = False
        return C()

    monkeypatch.setattr(nl, "http", fake_http)
    monkeypatch.setattr(nl.psycopg2, "connect", fake_connect)
    monkeypatch.setenv("NEON_LOCAL_TENANT_ID", "t0")
    monkeypatch.setenv("NEON_LOCAL_AGENTS", nl.unix_url("/run/agent.sock"))
    monkeypatch.setenv("NEON_LOCAL_RUN_NONCE", "n")
    monkeypatch.setenv("NEON_LOCAL_RUN_ROOT", "ab" * 16)
    from dblib import result_collector as rc
    nl.NeonLocalToolSuite._COMPUTES.clear()
    s = nl.NeonLocalToolSuite(rc.ResultCollector(), "main")
    s._open("ab" * 16)
    assert (seen["host"], seen["port"]) == ("/run/c0", 20000)
    nl.NeonLocalToolSuite._COMPUTES.clear()
