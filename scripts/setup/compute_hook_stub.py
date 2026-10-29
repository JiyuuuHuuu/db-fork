#!/usr/bin/env python3
"""Compute-notification stub for the storage controller (`--control-plane-url`): accepts every notify-attach PUT
with 200 (stdlib only). The computes get their pageserver shards from the node agent at start
(scripts/setup/neon_node_agent.py), not from these notifications.

Threaded, with a per-connection timeout (experiment 5): experiments 3 and 4's single-threaded stub hung on one
connection that never completed its request; every later notification then timed out and the storage controller
kept retrying the tenant's reconciliation (a shard split needs a successful notification).

  python3 compute_hook_stub.py --listen 0.0.0.0:9999        TCP
  python3 compute_hook_stub.py --listen unix:/path/hook.sock Unix socket (experiments 10/11)
"""
import argparse
import http.server
import os
import socketserver


class Handler(http.server.BaseHTTPRequestHandler):
    timeout = 10          # seconds per connection read: a stalled client cannot block the others

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):
        pass

    def address_string(self):
        return "client"


class UnixHandler(Handler):
    disable_nagle_algorithm = False


class TcpServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    request_queue_size = 128


class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    request_queue_size = 128


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", default="0.0.0.0:9999", help="host:port or unix:/path")
    a = ap.parse_args()
    if a.listen.startswith("unix:"):
        path = a.listen[5:]
        if os.path.exists(path):
            os.unlink(path)
        UnixServer(path, UnixHandler).serve_forever()
    else:
        host, port = a.listen.rsplit(":", 1)
        TcpServer((host, int(port)), Handler).serve_forever()


if __name__ == "__main__":
    main()
