import argparse
import os
import socket
import threading
import time

"""Continuous load test for the aggregation server.

Each worker keeps one TCP connection open and sends a fixed-size pipelined
batch in a loop until the configured wall-clock duration is reached.

Usage:
    python3 pressure_continuous.py --duration 1800 --conns 500 --pipeline 16
"""

VERBOSE = os.environ.get("PRESSURE_VERBOSE") == "1"


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


def build_plan(mode, pipeline):
    plan = []
    for i in range(pipeline):
        if mode == "mixed":
            payload = "ping" if i % 2 == 0 else "hello"
        else:
            payload = "hello" if mode == "db" else "ping"
        plan.append(payload)
    wire = b"".join(frame(p) for p in plan)
    return wire


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ping_ok = 0
        self.db_ok = 0
        self.bad = 0
        self.timeout = 0
        self.broken = 0
        self.conn_fail = 0
        self.rtt_sum_us = 0
        self.batches = 0

    def snapshot(self):
        with self.lock:
            return dict(
                ping_ok=self.ping_ok,
                db_ok=self.db_ok,
                bad=self.bad,
                timeout=self.timeout,
                broken=self.broken,
                conn_fail=self.conn_fail,
                rtt_sum_us=self.rtt_sum_us,
                batches=self.batches,
            )


def worker(index, mode, host, port, pipeline, io_timeout, end_time,
           stop_event, stats, backoff):
    wire = build_plan(mode, pipeline)
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
                    break
                t0 = time.perf_counter()
                sock.sendall(wire)
                for _ in range(pipeline):
                    header = recv_exact(sock, 4)
                    if header is None:
                        if VERBOSE:
                            print("eof worker=%d" % index, flush=True)
                        with stats.lock:
                            stats.broken += 1
                        return
                    length = int(header)
                    body = recv_exact(sock, length)
                    if body is None:
                        if VERBOSE:
                            print("eof-body worker=%d" % index, flush=True)
                        with stats.lock:
                            stats.broken += 1
                        return
                    with stats.lock:
                        if body == b"echo:ping":
                            stats.ping_ok += 1
                        elif body == b"reply:hello|hello":
                            stats.db_ok += 1
                        else:
                            stats.bad += 1
                dt_us = (time.perf_counter() - t0) * 1_000_000
                with stats.lock:
                    stats.rtt_sum_us += dt_us
                    stats.batches += 1
        except socket.timeout:
            with stats.lock:
                stats.timeout += 1
        except Exception as exc:
            if VERBOSE:
                print("err worker=%d %r" % (index, exc), flush=True)
            with stats.lock:
                stats.broken += 1
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
        if stop_event.wait(backoff):
            return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=1800)
    ap.add_argument("--conns", type=int, default=500)
    ap.add_argument("--pipeline", type=int, default=16)
    ap.add_argument("--mode", choices=["mixed", "db", "ping"], default="mixed")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--io-timeout", type=float, default=30.0)
    ap.add_argument("--report", type=float, default=60.0)
    ap.add_argument("--backoff", type=float, default=0.2)
    args = ap.parse_args()

    stats = Stats()
    stop_event = threading.Event()
    start = time.monotonic()
    end_time = start + args.duration
    threads = [
        threading.Thread(
            target=worker,
            args=(i, args.mode, args.host, args.port, args.pipeline,
                  args.io_timeout, end_time, stop_event, stats, args.backoff),
            daemon=True,
        )
        for i in range(args.conns)
    ]

    for t in threads:
        t.start()

    last = start
    last_ok = 0
    last_rtt = 0.0
    try:
        while True:
            now = time.monotonic()
            if now >= end_time:
                break
            elapsed = now - start
            s = stats.snapshot()
            ok = s["ping_ok"] + s["db_ok"]
            qps = (ok - last_ok) / (now - last) if now > last else 0
            rtt = (s["rtt_sum_us"] - last_rtt) / 1000.0 if s["batches"] > 0 else 0.0
            if s["batches"] > 0 and (now - last) > 0:
                rtt = ((s["rtt_sum_us"] - last_rtt) / 1000.0)
                last_rtt = s["rtt_sum_us"]
            print(
                "elapsed=%.0fs qps=%.0f ok=%d ping=%d db=%d bad=%d "
                "timeout=%d conn_fail=%d rtt_ms=%.1f"
                % (elapsed, qps, ok, s["ping_ok"], s["db_ok"], s["bad"],
                   s["timeout"], s["conn_fail"], rtt),
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

    total_elapsed = time.monotonic() - start
    s = stats.snapshot()
    ok = s["ping_ok"] + s["db_ok"]
    qps = ok / total_elapsed if total_elapsed > 0 else 0
    avg_rtt_ms = s["rtt_sum_us"] / 1000.0 / s["batches"] if s["batches"] else 0
    print(
        "FINAL duration=%.1fs conns=%d pipeline=%d mode=%s ok=%d "
        "ping_ok=%d db_ok=%d bad=%d timeout=%d broken=%d conn_fail=%d "
        "qps=%.0f avg_batch_rtt_ms=%.1f"
        % (total_elapsed, args.conns, args.pipeline, args.mode, ok,
           s["ping_ok"], s["db_ok"], s["bad"], s["timeout"], s["broken"],
           s["conn_fail"], qps, avg_rtt_ms),
        flush=True,
    )


if __name__ == "__main__":
    main()
