import socket
import sys
import threading
import time

"""
串行压测：每个连接发一条等一条。
用法: python3 pressure_test.py [连接数] [每连接消息数]
"""

HOST = "127.0.0.1"
PORT = 9001
CONNS = int(sys.argv[1]) if len(sys.argv) > 1 else 20
MSGS = int(sys.argv[2]) if len(sys.argv) > 2 else 10

EXPECTED = "reply:hello|hello"
stats = {"ok": 0, "bad": 0, "timeout": 0, "conn_fail": 0, "conn_close": 0}
lock = threading.Lock()


def frame(payload):
    return ("%04d" % len(payload) + payload).encode()


def worker():
    try:
        s = socket.create_connection((HOST, PORT), timeout=5)
    except Exception:
        with lock:
            stats["conn_fail"] += 1
        return
    s.settimeout(5)
    try:
        for _ in range(MSGS):
            s.sendall(frame("hello"))
            header = b""
            while len(header) < 4:
                chunk = s.recv(4 - len(header))
                if not chunk:
                    with lock:
                        stats["conn_close"] += 1
                    return
                header += chunk
            length = int(header)
            body = b""
            while len(body) < length:
                chunk = s.recv(length - len(body))
                if not chunk:
                    with lock:
                        stats["conn_close"] += 1
                    return
                body += chunk
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
    threads = [threading.Thread(target=worker) for _ in range(CONNS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt = time.time() - t0
    total = CONNS * MSGS
    qps = total / dt if dt > 0 else 0
    print(
        "conns=%d msgs=%d total=%d ok=%d bad=%d timeout=%d conn_fail=%d "
        "conn_close=%d elapsed=%.3fs qps=%.0f"
        % (CONNS, MSGS, total, stats["ok"], stats["bad"], stats["timeout"],
           stats["conn_fail"], stats["conn_close"], dt, qps)
    )


if __name__ == "__main__":
    main()
