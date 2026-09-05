import argparse
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

"""Repeated batch load with connection churn.

Each round opens conns connections, sends batch requests on each, reads all
responses, then closes every connection. The next round reconnects again.

Usage:
    python3 pressure_cycle.py --conns 500 --batch 200 --duration 300
"""


def frame(payload):
    return ("%04d" % len(payload) + payload).encode()


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def build_wire(mode, batch):
    parts = []
    for i in range(batch):
        if mode == "mixed":
            parts.append("ping" if i % 2 == 0 else "hello")
        elif mode == "db":
            parts.append("hello")
        else:
            parts.append("ping")
    return b"".join(frame(p) for p in parts)


def round_worker(_):
    try:
        sock = socket.create_connection(("127.0.0.1", 9001), timeout=30)
        sock.settimeout(30)
        wire = build_wire(args.mode, args.batch)
        sock.sendall(wire)
        ok = 0
        bad = 0
        for _ in range(args.batch):
            header = recv_exact(sock, 4)
            if header is None:
                sock.close()
                return "close", 0, 0
            body = recv_exact(sock, int(header))
            if body is None:
                sock.close()
                return "close", 0, 0
            if body == b"echo:ping" or body == b"reply:hello|hello":
                ok += 1
            else:
                bad += 1
        sock.close()
        return "ok", ok, bad
    except socket.timeout:
        return "timeout", 0, 0
    except Exception:
        return "conn_fail", 0, 0


def main():
    global args
    ap = argparse.ArgumentParser()
    ap.add_argument("--conns", type=int, default=500)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--mode", choices=["mixed", "db", "ping"], default="mixed")
    ap.add_argument("--duration", type=float, default=300)
    args = ap.parse_args()

    start = time.monotonic()
    ok = bad = close = timeout = conn_fail = 0
    round_no = 0

    while time.monotonic() - start < args.duration:
        round_no += 1
        r0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.conns) as ex:
            results = list(ex.map(round_worker, range(args.conns)))
        for state, o, b in results:
            ok += o
            bad += b
            if state == "close":
                close += 1
            elif state == "timeout":
                timeout += 1
            elif state == "conn_fail":
                conn_fail += 1
        elapsed = time.perf_counter() - r0
        total = ok + bad
        qps = total / elapsed if elapsed > 0 else 0
        print(
            "round=%d elapsed=%.1fs ok=%d qps=%.0f close=%d timeout=%d "
            "conn_fail=%d" % (round_no, time.monotonic() - start, ok, qps,
                              close, timeout, conn_fail),
            flush=True,
        )

    total_elapsed = time.monotonic() - start
    print(
        "CYCLE_FINAL duration=%.1fs rounds=%d ok=%d bad=%d close=%d "
        "timeout=%d conn_fail=%d qps=%.0f"
        % (total_elapsed, round_no, ok, bad, close, timeout, conn_fail,
           ok / total_elapsed),
        flush=True,
    )


if __name__ == "__main__":
    main()
