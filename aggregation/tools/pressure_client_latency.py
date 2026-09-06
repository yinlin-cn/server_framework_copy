import argparse
import socket
import statistics
import threading
import time

"""Client-side latency stress test.

Each worker keeps one connection open and sends one request at a time,
waiting for its reply before sending the next. Latency is measured per
request from the client side.

Usage:
    python3 pressure_client_latency.py --duration 60 --conns 500 --mode mixed
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

    def snapshot(self):
        with self.lock:
            return dict(
                ping_ok=self.ping_ok,
                db_ok=self.db_ok,
                bad=self.bad,
                timeout=self.timeout,
                broken=self.broken,
                conn_fail=self.conn_fail,
            )


def worker(index, mode, host, port, io_timeout, end_time, stop_event,
           stats, latency_us):
    while not stop_event.is_set():
        sock = None
        try:
            sock = socket.create_connection((host, port), timeout=io_timeout)
            sock.settimeout(io_timeout)
        except Exception:
            with stats.lock:
                stats.conn_fail += 1
            if stop_event.wait(0.5):
                return
            continue

        try:
            while not stop_event.is_set():
                if time.monotonic() >= end_time:
                    return
                if mode == "mixed":
                    payload = "ping" if index % 2 == 0 else "hello"
                else:
                    payload = "hello" if mode == "db" else "ping"
                want = (b"echo:ping" if payload == "ping"
                        else b"reply:hello|hello")
                t0 = time.perf_counter_ns()
                sock.sendall(frame(payload))
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
                    if body == want:
                        if payload == "ping":
                            stats.ping_ok += 1
                        else:
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
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
        if stop_event.wait(0.2):
            return


def percentile(sorted_samples, p):
    if not sorted_samples:
        return 0.0
    if p >= 100:
        return sorted_samples[-1]
    pos = (len(sorted_samples) - 1) * p / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(sorted_samples) - 1)
    frac = pos - lo
    return sorted_samples[lo] * (1 - frac) + sorted_samples[hi] * frac


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--conns", type=int, default=500)
    ap.add_argument("--mode", choices=["mixed", "db", "ping"], default="mixed")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--io-timeout", type=float, default=10.0)
    ap.add_argument("--report", type=float, default=20.0)
    args = ap.parse_args()

    stats = Stats()
    stop_event = threading.Event()
    start = time.monotonic()
    end_time = start + args.duration
    latencies = []
    lat_lock = threading.Lock()
    threads = []
    for i in range(args.conns):
        local = []
        latencies.append((local, lat_lock))
        t = threading.Thread(
            target=worker,
            args=(i, args.mode, args.host, args.port, args.io_timeout,
                  end_time, stop_event, stats, local),
            daemon=True,
        )
        threads.append(t)
        t.start()

    last = start
    last_ok = 0
    try:
        while True:
            now = time.monotonic()
            if now >= end_time:
                break
            s = stats.snapshot()
            ok = s["ping_ok"] + s["db_ok"]
            elapsed = now - start
            qps = (ok - last_ok) / (now - last) if now > last else 0
            print(
                "elapsed=%.0fs qps=%.0f ok=%d ping=%d db=%d bad=%d "
                "timeout=%d conn_fail=%d"
                % (elapsed, qps, ok, s["ping_ok"], s["db_ok"], s["bad"],
                   s["timeout"], s["conn_fail"]),
                flush=True,
            )
            last = now
            last_ok = ok
            if stop_event.wait(min(args.report, max(0.1, end_time - now))):
                break
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=max(10.0, args.io_timeout + 5.0))

    elapsed = time.monotonic() - start
    s = stats.snapshot()
    ok = s["ping_ok"] + s["db_ok"]
    qps = ok / elapsed if elapsed > 0 else 0
    samples = []
    for local, lock in latencies:
        samples.extend(local)
    samples.sort()
    avg_us = statistics.fmean(samples) if samples else 0.0
    print(
        "FINAL duration=%.1fs conns=%d mode=%s total=%d ok=%d ping=%d "
        "db=%d bad=%d timeout=%d broken=%d conn_fail=%d qps=%.0f "
        "latency_p50=%.2fms p95=%.2fms p99=%.2fms p999=%.2fms "
        "avg=%.2fms max=%.2fms"
        % (elapsed, args.conns, args.mode, len(samples), ok,
           s["ping_ok"], s["db_ok"], s["bad"], s["timeout"], s["broken"],
           s["conn_fail"], qps,
           percentile(samples, 50) / 1000.0,
           percentile(samples, 95) / 1000.0,
           percentile(samples, 99) / 1000.0,
           percentile(samples, 99.9) / 1000.0,
           avg_us / 1000.0,
           (samples[-1] / 1000.0) if samples else 0.0),
        flush=True,
    )


if __name__ == "__main__":
    main()
