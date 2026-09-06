import argparse
import socket
import statistics
import threading
import time

"""Pipelined load test with all connections established before timing.

All sockets are opened first. Measurement starts only after every worker has
been started, then each connection sends batch*rounds requests.

Usage:
    python3 pressure_preconnect.py --conns 500 --batch 100 --rounds 4
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


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ping_ok = 0
        self.db_ok = 0
        self.bad = 0
        self.timeout = 0
        self.broken = 0
        self.conn_fail = 0


def worker(sock, wire, batch, rounds, io_timeout, start_event, stop_event,
           stats, latency_us):
    sock.settimeout(io_timeout)
    start_event.wait()
    try:
        for _ in range(rounds):
            if stop_event.is_set():
                return
            t0 = time.perf_counter_ns()
            sock.sendall(wire)
            for _ in range(batch):
                header = recv_exact(sock, 4)
                if header is None:
                    with stats.lock:
                        stats.broken += 1
                    return
                length = int(header)
                body = recv_exact(sock, length)
                if body is None:
                    with stats.lock:
                        stats.broken += 1
                    return
                latency_us.append((time.perf_counter_ns() - t0) / 1000.0)
                with stats.lock:
                    if body == b"echo:ping":
                        stats.ping_ok += 1
                    elif body == b"reply:hello|hello":
                        stats.db_ok += 1
                    else:
                        stats.bad += 1
    except socket.timeout:
        with stats.lock:
            stats.timeout += 1
    except Exception:
        with stats.lock:
            stats.broken += 1
    finally:
        try:
            sock.close()
        except Exception:
            pass


def percentile(sorted_samples, p):
    if not sorted_samples:
        return 0.0
    pos = (len(sorted_samples) - 1) * p / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(sorted_samples) - 1)
    frac = pos - lo
    return sorted_samples[lo] * (1 - frac) + sorted_samples[hi] * frac


def build_wire(mode, batch):
    plan = []
    for i in range(batch):
        if mode == "mixed":
            payload = "ping" if i % 2 == 0 else "hello"
        else:
            payload = "hello" if mode == "db" else "ping"
        plan.append(payload)
    return b"".join(frame(p) for p in plan)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conns", type=int, default=500)
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--rounds", type=int, default=4)
    ap.add_argument("--mode", choices=["mixed", "db", "ping"], default="mixed")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--io-timeout", type=float, default=30.0)
    args = ap.parse_args()

    stats = Stats()
    sockets = []
    for i in range(args.conns):
        try:
            s = socket.create_connection((args.host, args.port), timeout=10)
            sockets.append(s)
        except Exception:
            with stats.lock:
                stats.conn_fail += 1
    if not sockets:
        print("no connections established")
        return

    start_event = threading.Event()
    stop_event = threading.Event()
    wire = build_wire(args.mode, args.batch)
    latencies = []
    threads = []
    for sock in sockets:
        local = []
        latencies.append(local)
        t = threading.Thread(
            target=worker,
            args=(sock, wire, args.batch, args.rounds, args.io_timeout,
                  start_event, stop_event, stats, local),
            daemon=True,
        )
        threads.append(t)
        t.start()

    t0 = time.perf_counter()
    start_event.set()
    for t in threads:
        t.join(timeout=max(10.0, args.io_timeout + 5.0))
    elapsed = time.perf_counter() - t0

    with stats.lock:
        ok = stats.ping_ok + stats.db_ok
        bad = stats.bad
        timeout = stats.timeout
        broken = stats.broken
        conn_fail = stats.conn_fail
        ping_ok = stats.ping_ok
        db_ok = stats.db_ok

    samples = []
    for local in latencies:
        samples.extend(local)
    samples.sort()
    qps = ok / elapsed if elapsed > 0 else 0
    avg_us = statistics.fmean(samples) if samples else 0.0
    print(
        "FINAL duration=%.2fs conns=%d batch=%d rounds=%d mode=%s "
        "total=%d ok=%d ping=%d db=%d bad=%d timeout=%d broken=%d "
        "conn_fail=%d qps=%.0f batch_latency_p50=%.2fms p95=%.2fms "
        "p99=%.2fms p999=%.2fms avg=%.2fms max=%.2fms"
        % (elapsed, len(sockets), args.batch, args.rounds, args.mode,
           len(samples), ok, ping_ok, db_ok, bad, timeout, broken,
           conn_fail, qps,
           percentile(samples, 50) / 1000.0,
           percentile(samples, 95) / 1000.0,
           percentile(samples, 99) / 1000.0,
           percentile(samples, 99.9) / 1000.0,
           avg_us / 1000.0,
           samples[-1] / 1000.0 if samples else 0.0),
        flush=True,
    )


if __name__ == "__main__":
    main()
