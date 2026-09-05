import argparse
import asyncio
import random
import time

"""Overlapping connection churn with random N*M batches.

Every launch interval picks a random number of messages M and a random
connection count N such that N*M >= 100000. Connections are spread over a
random window so batches overlap instead of connecting/disconnecting as one
monolithic block.

Usage:
    python3 pressure_pipeline_churn.py --duration 300 --interval 5
"""

HOST = "127.0.0.1"
PORT = 9001


def frame(payload):
    return ("%04d" % len(payload) + payload).encode()


def recv_exact(reader, n):
    return asyncio.wait_for(reader.readexactly(n), 60)


def build_wire(messages):
    parts = []
    for i in range(messages):
        parts.append("ping" if i % 2 == 0 else "hello")
    return b"".join(frame(p) for p in parts)


class Stats:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.ok = 0
        self.bad = 0
        self.conn_fail = 0
        self.close = 0
        self.timeout = 0


async def client_exchange(messages, spread, stats):
    await asyncio.sleep(random.uniform(0, spread))
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
    except Exception:
        async with stats.lock:
            stats.conn_fail += 1
        return

    wire = build_wire(messages)
    try:
        writer.write(wire)
        await writer.drain()

        ok = 0
        bad = 0
        for _ in range(messages):
            header = await recv_exact(reader, 4)
            body = await recv_exact(reader, int(header))
            if body in (b"echo:ping", b"reply:hello|hello"):
                ok += 1
            else:
                bad += 1

        async with stats.lock:
            stats.ok += ok
            stats.bad += bad
    except asyncio.TimeoutError:
        async with stats.lock:
            stats.timeout += 1
    except Exception:
        async with stats.lock:
            stats.close += 1
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def launcher(stop_event, args, stats, tasks):
    while not stop_event.is_set():
        # Random M and N with N*M >= 100000.
        messages = random.randint(args.msg_min, args.msg_max)
        base = (100000 + messages - 1) // messages
        conns = base + random.randint(0, args.extra_conns)

        for _ in range(conns):
            tasks.add(asyncio.create_task(
                client_exchange(messages, args.spread, stats)))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.interval)
        except asyncio.TimeoutError:
            pass


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--spread", type=float, default=3.0)
    ap.add_argument("--msg-min", type=int, default=200)
    ap.add_argument("--msg-max", type=int, default=500)
    ap.add_argument("--extra-conns", type=int, default=80)
    args = ap.parse_args()

    stats = Stats()
    stop_event = asyncio.Event()
    tasks = set()
    start = time.monotonic()
    launcher_task = asyncio.create_task(
        launcher(stop_event, args, stats, tasks))

    last_report = start
    while True:
        await asyncio.sleep(1)
        now = time.monotonic()
        if now - last_report >= 30:
            async with stats.lock:
                ok = stats.ok
                bad = stats.bad
                close = stats.close
                timeout = stats.timeout
            print(
                "elapsed=%.0fs ok=%d bad=%d close=%d timeout=%d qps=%.0f"
                % (now - start, ok, bad, close, timeout, ok / (now - start)),
                flush=True,
            )
            last_report = now
        if now - start >= args.duration:
            break

    stop_event.set()
    await launcher_task
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.monotonic() - start
    async with stats.lock:
        ok = stats.ok
        bad = stats.bad
        close = stats.close
        timeout = stats.timeout
        conn_fail = stats.conn_fail
    print(
        "PIPELINE_CHURN_FINAL duration=%.1fs ok=%d bad=%d close=%d "
        "timeout=%d conn_fail=%d qps=%.0f"
        % (elapsed, ok, bad, close, timeout, conn_fail, ok / elapsed),
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
