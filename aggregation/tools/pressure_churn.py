import argparse
import socket
import time

"""Connection churn load: repeatedly open, exchange, close.

Simulates real deployments where clients connect/disconnect while a steady
traffic stream keeps running.

Usage:
    python3 pressure_churn.py --duration 300 --interval 0.1
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


def recv_frame(sock):
    header = recv_exact(sock, 4)
    if header is None:
        return None
    body = recv_exact(sock, int(header))
    return None if body is None else body


def one_churn(host, port):
    wire = frame("ping") + frame("hello")
    with socket.create_connection((host, port), timeout=10) as s:
        s.settimeout(10)
        s.sendall(wire)
        first = recv_frame(s)
        second = recv_frame(s)
        if first == b"echo:ping" and second == b"reply:hello|hello":
            return "ok"
        if first == b"reply:hello|hello" and second == b"echo:ping":
            return "ok"
        return "bad"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--interval", type=float, default=0.1)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9001)
    args = ap.parse_args()

    start = time.monotonic()
    end = start + args.duration
    ok = bad = conn_fail = timeout = 0
    last_report = start

    while True:
        now = time.monotonic()
        if now >= end:
            break
        t0 = time.perf_counter()
        try:
            result = one_churn(args.host, args.port)
            if result == "ok":
                ok += 1
            else:
                bad += 1
        except socket.timeout:
            timeout += 1
        except Exception:
            conn_fail += 1

        if now - last_report >= 30:
            print(
                "churn elapsed=%.0fs ok=%d bad=%d conn_fail=%d timeout=%d"
                % (now - start, ok, bad, conn_fail, timeout),
                flush=True,
            )
            last_report = now

        wait = args.interval - (time.perf_counter() - t0)
        if wait > 0:
            time.sleep(wait)

    elapsed = time.monotonic() - start
    print(
        "CHURN_FINAL duration=%.1fs ok=%d bad=%d conn_fail=%d timeout=%d"
        % (elapsed, ok, bad, conn_fail, timeout),
        flush=True,
    )


if __name__ == "__main__":
    main()
