# Concurrent Server Framework

> A short-task server framework for food delivery systems based on
> `epoll + thread pools + coroutines`.
>
> Version: v0.8

---

## Overview

The framework provides:

- Acceptor + N Reactor network model
- Thread-pool based task parsing and business execution
- Coroutine suspension / resumption for database waits
- Parameterized database queries through MariaDB prepared statements
- Bounded task queues with high/low watermarks
- Per-connection sliding-window flow control
- DB admission control and waiting queue
- Framework call layer for business/operations integrations
- Metrics and logging
- Graceful shutdown

It is designed for request-response style short tasks, not for
long-running business logic inside the worker pool.

---

## Quick Start

### Requirements

- Linux or WSL
- g++ with C++20 support
- MariaDB client library

### Build

```bash
sudo apt install libmariadb-dev

g++ -std=c++20 -fcoroutines \
    main.cpp Server.cpp ReactorControl.cpp epoll.cpp \
    divide_pool.cpp work_pool.cpp thread_pool.cpp blockingqueue.cpp \
    EventAwaiter.cpp context.cpp Handler_epoll_make.cpp \
    Handler_divide_make.cpp Handler_DB_make.cpp DB_pool.cpp \
    connect_pool.cpp Logger.cpp Metrics.cpp BatchSender.cpp \
    Handler_batch_make.cpp NetworkServer.cpp Reactor.cpp Acceptor.cpp \
    connect_book.cpp FrameworkCall.cpp ConnectionSession.cpp \
    $(mariadb_config --cflags --libs) -I . -o server -pthread
```

### Run

```bash
./server
```

### Protocol

Each message is length framed:

```text
[4 ASCII decimal digits][payload]
```

Example:

```text
0005hello
```

Expected response:

```text
0017reply:hello|hello
```

---

## Architecture

```text
Network IO layer (Acceptor + N Reactor)
                 |
                 v
Divide pool (protocol parsing / routing)
                 |
                 v
Work pool (business tasks / coroutines)
                 |
                 v
DB pool (parameterized SQL)
```

### Multi Reactor

- Acceptor accepts connections and distributes them round-robin to Reactors
- Every Reactor owns one epoll instance and one event-loop thread
- Connection table uses `unordered_map<int, shared_ptr<Internalconnection>>`
- BatchSender coalesces cross-thread wakeups
- Heartbeat removes connections idle for more than 60 seconds

### Framework Call Layer

The framework provides a framework-facing API:

- `connect_book`: connection registry with virtual identifiers and groups
- `FrameworkCall`: command dispatch for send/broadcast/group/close
- `ConnectionSession`: session handle for long-lived threads outside pools

Business code uses:

```cpp
framework_call("send_to_sb", virtual_fd, "message");
framework_call("send_to_gp", group, "message");
framework_call("close_conn", virtual_fd, "reason");
```

---

## Backpressure

### ConnectionFlow

Each connection has its own window:

- Default window: 8
- Window count, pause/resume state are protected by one mutex
- Reactor peeks one complete message before taking a window slot
- If a full message exists but no window slot is free, the message stays in
  `read_buffer`

### Bounded Task Queues

divide/work/DB queues use `bounded_task_queue<T>`:

- High water = queue capacity
- Low water = capacity / 2
- Producers can use blocking push
- Reactor-facing paths can use `try_push`
- Queue drains to low water before waking all blocked producers

### DB Admission

DB-related requests are classified before entering business:

- fast request: normal path
- db request: tries to acquire DB credit first
- no credit: message waits in `DbWaitingAdmission`, connection stops reading
- DB completion releases credit and resumes pending messages

### Handler PushResult

`Handler_epoll_make::on_message()` returns `PushResult`:

```cpp
enum class PushResult {
    Ok,
    Full,
    Closed,
};
```

On `Full`, Reactor returns the window slot and keeps the message in the
connection read buffer.

---

## Coroutine and DB Wakeup

Business coroutines call:

```cpp
DBResult res = co_await query_db(sql, params);
```

The suspension path is:

```text
business worker
  -> co_await
  -> register blockedtask
  -> submit DB task
  -> worker returns
```

DB worker:

```text
execute SQL
write result into Box
release DB connection
release DB credit
schedule coroutine resume
```

The DB worker waits until the initiating work function has returned before
resuming the coroutine. This prevents resuming a coroutine frame before it is
fully suspended.

---

## Database

SQL is parameterized:

```cpp
query_db(
    "SELECT name FROM users WHERE name = ? LIMIT 1",
    { name });
```

The DB layer returns complete rows:

```cpp
DBResult res = co_await query_db(sql, params);
res.rows;   // vector<vector<string>>
```

Business code decides how many rows/columns it needs.

---

## Metrics and Logging

Metrics sampler prints once per second:

```text
req_qps
req_avg60
req_avg / req_p99
inflight
conns
[divide/work/db] qps / avg / p99
queue(d/w/db)
bp(d/w/db)=size/high/full
db(queue/high/low/full wait credit active)
err(d/w/db)
cpu
rss
```

Example:

```text
bp(d/w/db)=3/512/0|10/640/0|2/1600/0
db(queue=1/1600/800/0 wait=0 credit=38/50 active=4)
```

---

## Graceful Shutdown

```text
stop accept
drain divide pool
drain work pool
settle pending coroutines
flush batch sender
stop Reactors
close DB pool
stop metrics sampler
flush logger
```

---

## Stability Bugfixes in v0.8

Three concurrency issues were found with ThreadSanitizer:

1. Metrics samplers were registered after the sampler thread started
   - fixed by registering all samplers before `start_sampler()`
2. DB could resume a coroutine before `await_suspend()` returned
   - fixed by adding a `wake_guard` barrier to `Box`
3. `on_connect` could run after a connection was visible to the Reactor
   event loop
   - fixed by invoking `on_connect` before inserting into `connections_`

---

## Directory Layout

```text
aggregation/
  Acceptor / Reactor / NetworkServer
  divide_pool / work_pool / thread_pool
  DB_pool / connect_pool
  EventTask / EventAwaiter / blockingqueue
  context / thread_context
  connect_book / FrameworkCall / ConnectionSession
  ConnectionFlow / bounded_task_queue / backpressure
  Handler_* / Metrics / Logger
  Server
  main
  tools/
```

---

## Performance Notes

Performance depends heavily on client behavior:

```text
Closed-loop request-response:
    about 8-10k QPS

Sustained 1-hour connection churn:
    about 9.5k QPS

2000 connections x 16 pipeline after v0.8 fixes:
    8.2k QPS, zero errors

High-burst pipelined flood:
    27k overall QPS / up to 71k during the first burst
    DB credit hit 0, DB wait appeared, p99 rose to 5ms
```

The stable operating window is around 10k QPS for request-response traffic.

---

## Limitations

- WSL numbers are useful for relative comparisons only
- Sustained latency needs native Linux validation
- Backpressure low-water external callbacks are not fully wired to Reactor
- Long-lived independent-thread sessions need real business validation
- Operations management dashboard is not implemented yet
