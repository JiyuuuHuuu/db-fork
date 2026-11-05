#!/usr/bin/env python3
"""TCP ping-pong RTT. server: tcprtt.py serve PORT ; client: tcprtt.py HOST PORT N SIZE -> prints per-RTT us."""
import socket, sys, time
if sys.argv[1] == "serve":
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", int(sys.argv[2]))); s.listen(8)
    while True:
        c, _ = s.accept(); c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            d = c.recv(65536)
            if not d: break
            c.sendall(d)
        c.close()
host, port, n, size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
c = socket.create_connection((host, port)); c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
msg = b"x" * size; out = []
for i in range(n + 100):
    t = time.perf_counter_ns(); c.sendall(msg); got = 0
    while got < size: got += len(c.recv(65536))
    if i >= 100: out.append((time.perf_counter_ns() - t) / 1000)
    time.sleep(0.002)
print("\n".join(f"{x:.1f}" for x in out))
