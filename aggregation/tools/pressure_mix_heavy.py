import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

"""
混合重业务压测：一半 ping（轻）+ 一半 heavy:N（重），看快慢混在一起的表现。
用法: python3 pressure_mix_heavy.py [连接数] [每连接消息数] [耗时毫秒]
"""

HOST = "127.0.0.1"
PORT = 9001
CONNS = int(sys.argv[1])
BATCH = int(sys.argv[2])
MS = int(sys.argv[3]) if len(sys.argv) > 3 else 5

PING_EXPECTED = "echo:ping"
HEAVY_EXPECTED = "done:heavy:%d" % MS
stats = {
    "ping_ok": 0,
    "heavy_ok": 0,
    "bad": 0,
    "timeout": 0,
    "conn_fail": 0,
    "conn_close": 0,
}
lock = threading.Lock()


def frame(payload):
    return ("%04d" % len(payload) + payload).encode()


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def worker(_):
    try:
        s = socket.create_connection((HOST, PORT), timeout=30)
    except Exception:
        with lock:
            stats["conn_fail"] += 1
        return
    s.settimeout(30)
    try:
        payloads = []
        for i in range(BATCH):
            payloads.append("ping" if i % 2 == 0 else "heavy:%d" % MS)
        s.sendall(b"".join(frame(p) for p in payloads))

        for _ in range(BATCH):
            header = recv_exact(s, 4)
            if header is None:
                with lock:
                    stats["conn_close"] += 1
                return
            length = int(header)
            body = recv_exact(s, length)
            if body is None:
                with lock:
                    stats["conn_close"] += 1
                return
            text = body.decode()
            with lock:
                if text == PING_EXPECTED:
                    stats["ping_ok"] += 1
                elif text == HEAVY_EXPECTED:
                    stats["heavy_ok"] += 1
                else:
                    stats["bad"] += 1
    except socket.timeout:
        with lock:
            stats["timeout"] += 1
    except Exception:
        with lock:
            stats["bad"] += 1
    finally:
        s.close()


def main():
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=CONNS) as ex:
        list(ex.map(worker, range(CONNS)))
    dt = time.time() - t0
    total = CONNS * BATCH
    qps = total / dt if dt > 0 else 0
    print(
        "ms=%d conns=%d batch=%d total=%d ping_ok=%d heavy_ok=%d bad=%d "
        "timeout=%d conn_fail=%d conn_close=%d elapsed=%.2fs qps=%.0f"
        % (MS, CONNS, BATCH, total, stats["ping_ok"], stats["heavy_ok"],
           stats["bad"], stats["timeout"], stats["conn_fail"],
           stats["conn_close"], dt, qps)
    )


if __name__ == "__main__":
    main()
