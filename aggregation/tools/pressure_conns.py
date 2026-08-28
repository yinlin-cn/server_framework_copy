import asyncio
import sys
import time

"""
连接承载压测：分批建立大量并发连接，全部保持打开，最后统一发 ping 收响应。
用法: python3 pressure_conns.py [连接数] [每连接消息数] [每批连接数]
运行前建议: ulimit -n 65535
"""

HOST = "127.0.0.1"
PORT = 9001
CONNS = int(sys.argv[1])
MSGS = int(sys.argv[2]) if len(sys.argv) > 2 else 2
BATCH = int(sys.argv[3]) if len(sys.argv) > 3 else 5000

EXPECTED = b"echo:ping"
MSG = b"0004ping"
stats = {"ok": 0, "bad": 0, "connect_fail": 0, "close": 0, "timeout": 0}
lock = asyncio.Lock()


async def exchange(reader, writer):
    try:
        writer.write(MSG * MSGS)
        await writer.drain()
        for _ in range(MSGS):
            header = await asyncio.wait_for(reader.readexactly(4), 30)
            length = int(header)
            body = await asyncio.wait_for(reader.readexactly(length), 30)
            async with lock:
                if body == EXPECTED:
                    stats["ok"] += 1
                else:
                    stats["bad"] += 1
    except asyncio.TimeoutError:
        async with lock:
            stats["timeout"] += 1
    except Exception:
        async with lock:
            stats["close"] += 1
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    readers, writers = [], []
    t_connect0 = time.time()

    # 分批建连，避免客户端瞬时创建 5 万 socket 撞上限
    for start in range(0, CONNS, BATCH):
        end = min(start + BATCH, CONNS)
        results = await asyncio.gather(
            *(asyncio.open_connection(HOST, PORT) for _ in range(end - start)),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                async with lock:
                    stats["connect_fail"] += 1
            else:
                readers.append(r[0])
                writers.append(r[1])
        print("created=%d/%d" % (len(writers), CONNS), flush=True)

    t_connect = time.time() - t_connect0
    t_exchange0 = time.time()
    await asyncio.gather(*(exchange(r, w) for r, w in zip(readers, writers)))
    t_exchange = time.time() - t_exchange0

    total = CONNS * MSGS
    qps = total / t_exchange if t_exchange > 0 else 0
    print(
        "conns=%d msgs=%d total=%d ok=%d bad=%d connect_fail=%d close=%d "
        "timeout=%d connect_time=%.2fs exchange_time=%.2fs qps=%.0f"
        % (CONNS, MSGS, total, stats["ok"], stats["bad"],
           stats["connect_fail"], stats["close"], stats["timeout"],
           t_connect, t_exchange, qps)
    )


if __name__ == "__main__":
    asyncio.run(main())
