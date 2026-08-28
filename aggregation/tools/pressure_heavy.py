import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

"""
重业务压测：消息 heavy:N，服务器空转 N 毫秒模拟计算型任务。
用法: python3 pressure_heavy.py [连接数] [批大小] [耗时毫秒]
"""

HOST = "127.0.0.1"
PORT = 9001
CONNS = int(sys.argv[1])
BATCH = int(sys.argv[2])
MS = int(sys.argv[3]) if len(sys.argv) > 3 else 5

MSG_PAYLOAD = "heavy:%d" % MS
EXPECTED = "done:heavy:%d" % MS
stats = {"ok": 0, "bad": 0, "timeout": 0, "conn_fail": 0, "conn_close": 0}
lock = threading.Lock()


def frame(payload):
    return ("%04d" % len(payload) + payload).encode()


MSG = frame(MSG_PAYLOAD)


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
        s.sendall(MSG * BATCH)
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
            with lock:
                if body.decode() == EXPECTED:
                    stats["ok"] += 1
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
        "ms=%d conns=%d batch=%d total=%d ok=%d bad=%d timeout=%d "
        "conn_fail=%d conn_close=%d elapsed=%.2fs qps=%.0f"
        % (MS, CONNS, BATCH, total, stats["ok"], stats["bad"],
           stats["timeout"], stats["conn_fail"], stats["conn_close"], dt, qps)
    )


if __name__ == "__main__":
    main()
