# 服务器并发框架

> 一个基于 `epoll + 线程池 + 协程` 的短任务服务器框架，目标是为外卖系统（顾客 / 商家 / 骑手三端）提供底层网络、异步业务与数据库支持。
>
> 版本：v0.8（v0.7 全功能 + 三处并发生命周期竞态修复）
>
> English version: [README_EN.md](README_EN.md)

---

## 〇、回到项目时，从这里开始

本项目开发主要发生在 WSL 主仓库，Windows 这份用于协作/备份/文档整理，两份要定期同步。

```text
WSL 主仓库     ~/server_framework_copy/aggregation/     # 实际编译、压测、跑 DB 的地方
Windows 副本   C:\Users\23966\Documents\GitHub\server_framework\aggregation/
```

同步方式（在 WSL 中执行；只覆盖代码与文档，不覆盖运行期产物）：

```bash
cd ~/server_framework_copy
cp /mnt/c/Users/23966/Documents/GitHub/server_framework/aggregation/*.cpp aggregation/
cp /mnt/c/Users/23966/Documents/GitHub/server_framework/aggregation/*.h   aggregation/
cp /mnt/c/Users/23966/Documents/GitHub/server_framework/aggregation/tools/*.py aggregation/tools/
cp /mnt/c/Users/23966/Documents/GitHub/server_framework/aggregation/tools/*.md aggregation/tools/
cp /mnt/c/Users/23966/Documents/GitHub/server_framework/README.md      .
cp /mnt/c/Users/23966/Documents/GitHub/server_framework/README_EN.md   .
```

依赖与数据库初始化（账号/库名与 `main.cpp` 的 `Server::DBConfig` 一致）：

```bash
sudo apt install libmariadb-dev

mariadb -uroot -p
CREATE DATABASE IF NOT EXISTS delivery;
CREATE USER IF NOT EXISTS 'delivery'@'localhost' IDENTIFIED BY 'delivery123';
GRANT ALL PRIVILEGES ON delivery.* TO 'delivery'@'localhost';
USE delivery;
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) UNIQUE NOT NULL
);
INSERT INTO users(name) VALUES ('hello'),('world')
ON DUPLICATE KEY UPDATE name = name;
```

编译（`aggregation/` 下，C++20 需要 `-fcoroutines`）：

```bash
cd ~/server_framework_copy/aggregation
g++ -std=c++20 -fcoroutines \
    main.cpp Server.cpp ReactorControl.cpp epoll.cpp divide_pool.cpp \
    work_pool.cpp thread_pool.cpp blockingqueue.cpp EventAwaiter.cpp \
    context.cpp Handler_epoll_make.cpp Handler_divide_make.cpp \
    Handler_DB_make.cpp DB_pool.cpp connect_pool.cpp Logger.cpp \
    Metrics.cpp BatchSender.cpp Handler_batch_make.cpp NetworkServer.cpp \
    Reactor.cpp Acceptor.cpp connect_book.cpp FrameworkCall.cpp \
    ConnectionSession.cpp \
    $(mariadb_config --cflags --libs) -I . -o server -pthread
```

启动与冒烟：

```bash
./server &                     # 正常打印：服务器启动，端口 9001
printf '0005hello' | nc -q2 127.0.0.1 9001
# 预期：0017reply:hello|hello
```

协议格式：`[4 位十进制 ASCII 长度][消息体]`。压测消息一览：

| 消息 | 预期 | 走的链路 |
|---|---|---|
| `ping` | `echo:ping` | 不查库（fast） |
| `hello` | `reply:hello|hello` | 参数化查库（db） |
| `broadcast:N` | `msg:0 ... msg:N-1` | 单任务多次 `send` |
| `heavy:N` | `done:heavy:N` | 业务线程内模拟 N ms 负载 |
| `fwbind:1001:7` | `fwbind-ok` | bind 当前连接到 virtual_fd=1001、组 7 |
| `fwsend:1001:xxx` | `fwsend-ok` | 按 virtual_fd 单发 |
| `fwgroup:7:xxx` | `fwgroup-ok` | 组播到组 7 |
| `fwdivide:7:1001,1002` | `fwdivide-ok` | 重建组 7 的连接集合 |
| `fwclose:1001` | 对方连接被框架关闭 | 主动关连接 |

快速回归：

```bash
python3 tools/pressure_pipe.py 200 100        # 轻量流水线（连接数 批次数）
python3 tools/framework_call_test.py          # 框架调用端到端
python3 tools/pressure_continuous.py          # 长稳，-h 查看参数
python3 tools/parse_metrics.py server.log     # 把指标日志汇总成表格
```

设计文档与图：

```text
design_text/服务器并发框架.drawio        # drawio 原图（.png 为同图导出）
design_text/README.md                   # 早期架构日志与设计决策
design_text/框架调用层与连接名册设计.md   # v0.6 框架调用设计
design_text/framework_call_design/      # 框架调用落地草案代码
aggregation/tools/PRESSURE_TEST.md      # 全部压测场景、脚本与数据
```

---

## 一、对外接口与使用规范（业务开发者对接指南）

### 1. 业务入口

框架只认识一个业务入口：**消息解析函数 `divide_work`**。收到一条完整消息后，框架在解析池里调用它，返回一个可执行的业务任务（`Work`）：

```cpp
using Work = std::function<void()>;
using DivideWork = std::function<Work(const std::string& msg)>;
```

`DivideWork` 通过 `Server` 构造函数传入。它只做“消息 → 闭包”的路由，不应在解析线程里执行业务：

```cpp
Server server(
    [biz](const std::string& msg) -> function<void()> {
        if (msg == "ping")   return [biz, msg]() { biz->echo(1, msg); };
        if (msg == "order")  return [biz, msg]() { biz->place_order(1, msg); };
        return [biz, msg]() { biz->flow(1, msg); };    // 查库示例
    },
    9001,   // 端口
    16,     // 解析线程数
    20);    // 业务线程数
```

同时用前缀声明路由分类，让协议层决定走 fast 还是 db 准入：

```cpp
server.mark_fast_prefix("ping");
server.mark_fast_prefix("broadcast");
server.mark_db_prefix("hello");
```

### 2. 业务唯一接口（context.h）

业务代码只 `include context.h`，不接触框架内部：

```cpp
bool send(const std::string& data);                    // 回复当前连接；入队失败返回 false
bool framework_call(const std::string& cmd,
                    const std::vector<std::string>& args); // bind/单发/组播/分组/关闭
EventAwaiter query_db(const std::string& sql,
                      std::vector<std::string> params = {}); // 参数化查询，co_await 使用
```

注意：早期文档中的 `get_user_data/set_user_data` 目前没有实现（`Internalconnection` 上也没有 user_data 字段）。当前“连接级业务状态”尚未真正落地；如果业务需要，下一步应重新加回 user_data 与 context 包装。

### 3. 必须遵守的原则

- ✅ 协程函数参数**按值传**（`string msg`，不要 `const string&`）——协程挂起后引用可能失效；
- ✅ 业务类**无状态**，请求数据放参数，连接状态挂 user_data（未来字段）；
- ✅ 需要等待（查库等）用 `co_await query_db(...)`，协程挂起不占线程；
- ✅ 长任务（订单状态机、定时清理）放**独立线程**，不要塞进业务池；
- ✅ SQL 用**参数化**：`query_db("... WHERE name = ?", { msg })`；表名/列名不能参数化，需业务层白名单校验；
- ✅ 查询结果从 `res.rows`（完整多行多列）取，**行数限制由业务层自己决定**；
- ✅ 客户端定期发轻量消息保持活跃，空闲超过 60 秒会被心跳清理；
- ✅ 主动推送/分组/关闭连接走 `framework_call`，由框架解析并安全执行；
- ❌ 业务池线程里 sleep/忙等/阻塞锁/直接操作网络；
- ❌ 长任务塞进线程池；
- ❌ 把用户输入拼进 SQL。

### 4. 协程业务写法

```cpp
class BusinessLogic {
public:
    EventTask flow(int id, string msg) {          // 参数按值
        DBResult res = co_await query_db(
            "SELECT name FROM users WHERE name = ? LIMIT 1",
            { msg });

        if (res.cancelled) { send("查询被取消"); co_return; }
        if (!res.ok)       { send("查询失败: " + res.err); co_return; }

        string name = res.rows.empty() || res.rows[0].empty()
            ? "" : res.rows[0][0];
        if (!send("reply:" + msg + "|" + name)) co_return;   // 连接已断开则收尾
    }
};
```

协程函数返回类型必须是 `EventTask`（`initial_suspend`/`final_suspend` 均为 `suspend_never`，函数被调用就立即开始，挂起点在 `co_await` 处）。业务代码不需要 `co_return value`，只用在分叉点 `co_return` 结束。

### 5. DBResult 的判定约定

```cpp
DBResult res = co_await query_db(sql, params);
// 判定顺序必须固定：先 cancelled，再 !ok，最后才读 rows
if (res.cancelled) { ... return; }   // 退出/超时兜底取消
if (!res.ok)       { ... return; }   // res.err 有描述
// res.ok == true 才读 res.rows / res.data
```

### 6. 对接步骤

1. 定义 `divide_work`：消息 → 业务任务；
2. 用 `Server` 组装：传 `divide_work`、端口、线程数，Reactor 数默认 4；
3. 用 `mark_fast_prefix` / `mark_db_prefix` 声明路由分类；
4. 可选：`set_db_config`（DB 连接池）、`set_error_handler`（业务错误回调）；
5. 编译、运行、用 `nc`/压测脚本验证。

---

## 二、框架定位

本框架处理**短任务**：一条消息从进入到回复，就是一个短任务的完整生命周期。

```text
网络层收包 → 解析 → 业务处理 → 回复
```

| 模式 | 主驱动 | 业务层形式 | 适用场景 |
|---|---|---|---|
| 模式 A：框架为主 | 消息请求 | 快速完成的函数库 | 外卖日常请求、CRUD 服务、网关 |
| 模式 B：短任务为主 | 持续流程 | 长任务跑主流程，框架提供 IO/数据/事件 | 订单状态机、定时批处理 |

判断标准：**谁是主驱动？** 消息请求 → 模式 A；持续流程 → 模式 B。两者可并存。

---

## 三、架构总览

```text
┌─ 网络IO层（Acceptor + N 个 Reactor）──────────────────────┐
│ Acceptor（主线程 accept）→ 轮询分给某个 Reactor            │
│ Reactor = 1 epoll + 1 eventfd + 1 事件线程                │
│   读 → 长度头拆包 → 每连接窗口 try_take                   │
│   Handler_epoll_make：路由分类 / DB 准入 / 投 divide      │
│   发送：业务线程只入队，Reactor 统一 try_send             │
│   BatchSender 攒批唤醒 + 心跳扫描（60s 空闲清理）         │
└───────────────────┬───────────────────────────────────────┘
                    ▼
┌─ 任务解析线程池（divide_pool）────────────────────────────┐
│ worker 执行 divide_work(msg) → 得到业务闭包               │
│ 经 Handler_divide_make 封装 work_task 投业务池            │
└───────────────────┬───────────────────────────────────────┘
                    ▼
┌─ 业务工作线程池（work_pool）──────────────────────────────┐
│ 取任务 → 设 thread_local 白板 → 执行 fn → 清空           │
│ 执行中 co_await query_db：EventAwaiter 挂起协程           │
│   挂起任务登记进 blockingqueue（按 wait_key）             │
│   线程归还；DB 完成后再投 resume 任务回业务池             │
└───────────────────┬───────────────────────────────────────┘
                    ▼
┌─ 数据库层（DB_pool + connect_pool）───────────────────────┐
│ prepared statement 参数化执行 → 结果写 Box(rows/err)      │
│ → release DB credit → 等 wake_guard=false 再投唤醒任务    │
└───────────────────────────────────────────────────────────┘
```

### 3.1 分层职责

| 层 | 职责 | 关键组件 |
|---|---|---|
| 网络 IO 层 | 收发、长度头拆包、连接表、心跳、发送桶 | `Acceptor` / `Reactor` / `Internalconnection` / `Handler_epoll_make` |
| 批处理层 | 攒一批“待发 Reactor”，定时唤醒一次 | `BatchSender` / `Handler_batch_make` |
| 解析层 | 路由分类、消息 → 业务闭包 | `divide_pool` / `Handler_divide_make` |
| 业务层 | 执行业务闭包、协程挂起/恢复 | `work_pool` / `thread_pool` / `blockingqueue` / `EventAwaiter` |
| 数据库层 | 参数化执行、结果写 Box、唤醒协程 | `DB_pool` / `connect_pool` / `Handler_DB_make` |
| 框架调用层 | virtual_fd 映射、分组、单发/组播/关闭 | `connect_book` / `FrameworkCall` / `ConnectionSession` |
| 运维层 | 指标采样、日志写入、错误回调 | `Metrics` / `Logger` / `ErrorHandler` |
| 集成层 | 组装各层、控制启动/优雅退出 | `Server` / `NetworkServer` / `ReactorControl` |

### 3.2 线程模型（回来看代码前先记住这张表）

| 线程/角色 | 数量 | 在跑什么 | 关键约束 |
|---|---|---|---|
| Acceptor 线程 | 1 | accept + 轮询分配连接 | 只碰 listen_fd，不处理业务 |
| Reactor 事件线程 | N（默认 4） | 本组连接读/写/心跳/关闭 | 每 Reactor 单线程；跨线程只经 pending 桶 + eventfd |
| divide worker | parse_threads（默认 16） | `divide_work(msg)` | 不碰连接、不查库 |
| work worker | work_threads（默认 20） | 业务闭包 / 协程 resume | 白板是 TLS；查库会挂起归还线程 |
| DB worker | db_workers（默认 50） | prepared statement 执行 | 可阻塞；完成后经业务池 resume |
| BatchSender 线程 | 1 | 每 2ms 攒批唤醒待发 Reactor | 不做业务 |
| Metrics 采样线程 | 1 | 每秒读计数器出快照 | 必须在 sampler 注册完成后才 start |
| Logger 写线程 | 1 | 消费队列写 server.log | 调用方只 push，不碰磁盘 |
| main 线程 | 1 | 启动、信号等待、`Server::stop()` | 优雅退出全程在这里编排 |

### 3.3 跨线程协作为什么这样走

- 业务线程要发数据：只把消息推入 `conn->send_queue`（send_mutex 保护），登记进 Reactor 的 `pending_send_`；BatchSender 统一唤醒 Reactor 后由事件线程 `try_send()`。业务线程绝不直接 `write(fd)`。
- Reactor 要通知业务：把任务推进各池的任务队列，不直接调用业务函数。
- DB worker 要恢复协程：把 resume 任务投回 work_pool，由业务 worker 执行 `handle.resume()`，不在 DB 线程直接 resume。
- 谁要唤醒某个 Reactor：写它的 eventfd（`Reactor::wakeup()`），事件循环醒来后处理 pending 桶。

### 3.4 模块地图（文件归哪层、管什么）

| 文件 | 层级 | 说明 |
|---|---|---|
| `Internalconnection.h` | 网络 | sock、read_buffer、send_queue、send_function、owner_reactor、last_active_us、reading_paused、ConnectionFlow |
| `Reactor.h/.cpp` | 网络 | epoll 循环、拆包、读写、心跳、pending_send/close/resume 桶 |
| `Acceptor.h/.cpp` | 网络 | accept + 轮询分配，不持有连接 |
| `NetworkServer.h/.cpp` | 集成 | Acceptor + N Reactor 组装 |
| `ReactorControl.h/.cpp` | 背压 | 实现 IReactorControl：pause/schedule_resume 适配到 Reactor |
| `Handler_epoll.h` | 接口 | on_message/on_connect/on_disconnect |
| `Handler_epoll_make.h/.cpp` | 接线 | 路由分类 + DB 准入 + 投 divide + 名册生命周期 |
| `BatchSender.*` / `Handler_batch*` | 网络 | 攒 Reactor 待发桶，2ms 统一 wakeup，退出前 flush |
| `divide_pool.h/.cpp` | 解析 | 有界队列 + worker |
| `Handler_divide.h` | 接口 | on_work(conn, work) |
| `Handler_divide_make.h/.cpp` | 接线 | work_task → work_pool |
| `work_pool.h/.cpp` | 业务 | 线程池 + blockingqueue + 名册持有 + 透传 shutdown/wait_idle |
| `thread_pool.h/.cpp` | 业务 | 真正 worker：白板、try/catch、请求级指标、窗口归还、active_ 跟踪 |
| `work_task.h` | 业务 | fn + conn + is_business |
| `blockedtask.h` / `blockingqueue.*` | 业务 | 挂起协程登记表：wait_key → blockedtask（resume 闭包 + Box） |
| `EventTask.h` / `EventAwaiter.*` | 协程 | TLS 挂起标志；await_suspend 登记 + submit DB；await_resume 转 DBResult |
| `context.h/.cpp` | 业务接口 | send / framework_call / query_db；TLS 白板与全局入口 |
| `thread_context.h` | 共享 | tls_current_conn、g_work_pool、g_db_handler、g_framework_call |
| `DB_pool.h/.cpp` | DB | worker 循环、连接池借还、参数化执行、写 Box、credit release |
| `connect_pool.h/.cpp` | DB | N 条 MariaDB 连接借/还/显式 shutdown |
| `Handler_DB.h` | 接口 | submit(wait_key, box, sql, params) |
| `Handler_DB_make.h/.cpp` | 接线 | EventAwaiter → DBTask → DB_pool |
| `DB_task.h` / `Box.h` / `DBResult.h` | DB | 任务/信箱；Box 含 wake_guard 防提前 resume |
| `backpressure.h` | 背压 | RouteClassifier / DbCreditGate / DbWaitingAdmission |
| `bounded_task_queue.h` | 背压 | 有界队列：push/try_push、高低水位、Full 计数、close |
| `ConnectionFlow.h` | 背压 | 每连接窗口状态机 |
| `connect_book.h/.cpp` | 框架调用 | virtual_fd/组/版本号/变更缓存/条件变量等待 |
| `FrameworkCall.h/.cpp` | 框架调用 | 命令分发：bind/send_to_sb/send_to_gp/divide_gp/close_conn |
| `ConnectionSession.h/.cpp` | 框架调用 | 框架外独立线程的会话句柄 |
| `Metrics.*` / `MetricsConfig.h` | 运维 | 原子埋点 + 每秒快照（公式见第九章） |
| `Logger.*` | 运维 | 队列日志 + 后台写文件 + 级别过滤 |
| `Handler_metrics.h` / `Handler_log.h` | 接口 | 埋点/日志抽象 |
| `Server.h/.cpp` | 集成 | 组装顺序 + 优雅退出顺序 + 全局入口 |
| `main.cpp` | 示例 | BusinessLogic 演示 + DBConfig + 信号退出 |

---

## 四、网络层架构

### 4.1 多 Reactor（当前默认，v0.3 起）

```text
                 ┌─ Reactor 0（epoll + eventfd + 事件线程）→ 连接组 0
Acceptor（主线程）┼─ Reactor 1（epoll + eventfd + 事件线程）→ 连接组 1
  accept 轮询分配 └─ Reactor N（epoll + eventfd + 事件线程）→ 连接组 N
```

- `Acceptor` 只 accept，新连接按 `next_.fetch_add(1) % reactor_count` 轮询分配；
- 每个 `Reactor` 一个 epoll + 一个事件线程，连接表 `unordered_map<int, shared_ptr<Internalconnection>>`（O(1) 查找）；
- 跨线程发送：业务线程入队 + 加入待发送桶 + 标记 BatchSender，Reactor 统一写出；
- **心跳**：连接记录最后活跃时间，事件循环每 5 秒节流扫描，空闲超过 60 秒自动关闭；
- 连接注册到 Reactor 时立即初始化 `last_active_us`，避免冷启动第一批连接被误判超时。

### 4.2 单 Reactor（旧实现，保留参考）

`epoll_make` / `epoll.cpp`：单 epoll + 单线程事件循环。保留用于理解基础链路和对照性能，不在默认启动路径里。

### 4.3 BatchSender 批处理

```text
业务线程 send → 入队 + 加待发送桶 + Handler_batch::on_need_send(reactor)
  → BatchSender 攒 reactor 集合，每 2ms 统一 wakeup 一次
  → Reactor try_send 批量写出
```

动机：请求-响应下“每条消息一次 eventfd 唤醒”成本高；批处理后同一 Reactor 的多条待发数据一次唤醒、一次写完。退出时 `flush_and_stop()` 会把剩余待发 Reactor 全部唤醒，避免关 socket 前丢消息。

### 4.4 Reactor 内部三个 pending 桶

| 桶 | 谁写入 | 事件线程处理 |
|---|---|---|
| `pending_send_` | 业务线程（send_function） | `try_send()` 写 socket |
| `pending_resume_` | 业务/DB 侧（schedule_resume） | 去重后恢复 EPOLLIN |
| `pending_close_` | 任何线程（request_close） | `close_client()` 统一关闭 |

桶由 mutex 保护；跨线程只往桶里放指针，再写 eventfd；具体 epoll 操作都在 Reactor 线程内完成。`process_pending_resume` 会对同一连接按指针排序 + unique 去重，防止一次恢复被重复处理。

---

## 五、核心机制

### 5.1 任务携带连接（上下文随任务走）

所有任务（`work_task`/`divide_task`/`blockedtask`）携带 `shared_ptr<Internalconnection>`，连接生命周期由引用计数保证。连接断开不影响正在跑的任务；断线后 `send` 入队失败返回 false，业务据此收尾。

### 5.2 线程白板（TLS）

worker 执行任务前设置 `tls_current_conn`，业务通过 `send()` / `framework_call("bind", ...)` 等访问当前连接，执行完清空。白板是 `thread_local`（每个 worker 线程一份），不是全局共享变量；协程跨线程恢复时，resume 任务也携带 conn，白板会重新指向正确连接。

### 5.3 协程挂起 + blockingqueue 唤醒（一条 db 消息的完整生命周期）

```text
work worker 执行业务协程（is_business = true）
  → co_await query_db(...)
  → EventAwaiter::await_suspend(h)：
      1. 置 g_coroutine_suspended = true（本业务任务真正挂起，窗口暂不归还）
      2. box->wake_guard = g_current_task_active（DB 等它清掉才允许 resume）
      3. blockingqueue.insert(blockedtask{ wait_key, resume 闭包, box })
      4. db_handler->submit(wait_key, box, sql, params)
  → worker 返回（线程归还）

DB worker 执行 SQL → 写 box.rows/err → 还连接 → release credit
  → 等 wake_guard 变成 false（防止协程帧还在被业务 worker 使用）
  → work_pool::on_event(wait_key)：从 blockingqueue 取回 blockedtask
  → resume 任务重新进入 work 队列

work worker 再取到该任务 → 白板恢复 → handle.resume()
  → await_resume() 返回 DBResult → 业务继续 → send 回包
```

关键规则：**完成方绝不直接 resume，恢复必须重新入队**，避免协程池污染 / 业务池饥饿。

### 5.4 窗口位归还策略（ConnectionFlow）

- `Reactor::handle_read` 取到一条完整消息后先 `flow.try_take()`，成功才调用 handler；
- handler 返回 `Full` 时 `flow.release_slot()`（消息留在 read_buffer，等空位）；
- 业务 worker 跑完任务后：若 `g_coroutine_suspended == false`，说明任务已彻底结束，调 `flow.finish_one()`；
- 若任务挂起，worker 先归还线程；窗口位由后续 resume 任务执行者按同样规则归还；
- `finish_one()` 在窗口曾满时会置 `resume_pending_` 并通知 Reactor 恢复读取。

### 5.5 结构化查询结果

`DB_pool` 用 prepared statement 执行，完整结果写入 `box.rows`（vector<vector<string>>），错误写 `box.err`，取消写 `box.cancelled`。业务侧只读 `DBResult`；Box 生命周期由 shared_ptr 与 resume 闭包共同保证。

### 5.6 错误回调（ErrorHandler）

```cpp
using ErrorHandler = std::function<void(
    std::shared_ptr<Internalconnection> conn,
    const std::string& stage,     // "divide" / "work" / "db"
    const std::string& err)>;
```

worker 对任务执行包 try/catch，异常不会导致进程崩溃；catch 后依次走 ErrorHandler、日志、指标三路。main.cpp 示例会把 `ERROR: ...` 回给客户端，真实业务可自行决定打日志/落库/忽略。

### 5.7 优雅退出（Server::stop 的执行顺序）

```text
1. network_->stop_accept()             关闸：停 accept，不再收新连接
2. parse_pool_->shutdown()             解析关闸（任务队列 close）
   parse_pool_->wait_idle(5s)          等解析 worker 排干
3. work_pool_->shutdown()              业务关闸
   work_pool_->wait_idle(5s)           等业务排干（DB 此时仍可用，让在途查询能完成）
4. settle_pending()                    兜底：blockingqueue 里仍挂起的任务标记取消并重新投递
   work_pool_->wait_idle(5s)           等取消任务跑完
5. batch_sender_->flush_and_stop()     排空批处理模块，保证剩余发送被 Reactor 写出
6. network_->stop()                    停全部 Reactor：断连接、清理连接表
7. connect_book_->shutdown()           唤醒可能的版本等待者（ConnectionSession 会退出）
8. db_pool_->shutdown()                DB 最后关：先关连接池，再停 worker、join
9. metrics_->stop_sampler()            停指标采样线程
10. logger_->flush_and_stop()          排空日志队列再 join
11. clear_globals()                    清空 g_work_pool / g_db_handler / g_framework_call
```

顺序要点：**先关闸（不让新任务进来），再逐层排干（让在途任务按流程跑完），DB 必须留到业务排干之后才关**。main 线程收到 SIGINT/SIGTERM 只置 `g_exit_flag`，信号处理里不做复杂操作，退出编排全在主线程。

### 5.8 退出机制已知边界（当前实现不完善处，回来改这里）

- `thread_pool::shutdown()` 只是 `tasks_.close()`；close 后队列里剩余任务不会被继续执行，阻塞生产的线程直接返回；
- 因此 stop 依赖“排干后再 settle”：若排干超时，settle 阶段把取消任务重新投递到已关闭队列，这类任务实际不会执行；
- 挂起协程没有全局登记表，`blockingqueue::take_all()` 只能取到还没被 DB 唤醒的挂起任务；DB 已完成、resume 任务还在 work 队列里的协程走的是排干阶段；
- 结论：正常负载下退出路径干净；极端“DB 还在跑 + 队列积压 + 马上退出”时可能有个别协程帧不回收（进程退出时由 OS 释放）。要保证“任何时刻退出都不漏任务”，需要给 Box/EventAwaiter 加全局登记表与显式取消协议（见第十三章）。

---

## 六、框架调用层与连接名册（v0.6 新增）

框架提供业务/运维可用的内部调用通道：

- **connect_book**：连接名册，只收录框架管理的连接；保存 virtual_fd → 连接、连接组、版本号与变更缓存；
- **FrameworkCall**：命令分发入口，内置 bind/send_to_sb/send_to_gp/divide_gp/close_conn；
- **ConnectionSession**：给框架外独立长连接线程使用的会话句柄，可通过版本号条件变量等待名册变化。

```text
业务代码 / 独立长连接线程 / 未来运维管理
        │
        ▼
context::framework_call(cmd, args)
        │
        ▼
FrameworkCall（命令分发 + 内置 handler）
        │
        ▼
connect_book（virtual_fd / 分组 / 版本 / 变更缓存）
        │
        ▼
conn->send_function(...) 或 network_->request_close(...)
```

生命周期接入点：

```text
Handler::on_connect     → connect_book::on_connection(conn)    // 先以 sock 为临时 virtual_fd
Handler::on_disconnect  → connect_book::dis_connection(conn)   // 反查并清理
业务主动关闭            → FrameworkCall::close_conn → Reactor::request_close
```

名册关键设计：

- 存 weak_ptr，不拖长连接生命周期；
- 登录/鉴权成功后业务用 `framework_call("bind", virtual_fd, group)` 换成本地业务标识；业务标识冲突会 bind 失败；
- 名册每次变化自增版本号，并记录 net_changer 到变更缓存（上限 MAX_CHANGES=4096，超出丢最旧）；
- 独立长连接线程可用 `ConnectionSession::wait_version_change()` 条件变量等待，不空转轮询；connect_book::shutdown 唤醒所有等待者；
- 关闭动作永远交还 Reactor（request_close），业务线程不直接 close(fd)；
- 心跳活跃时间在连接注册到 Reactor 时立即初始化，修复冷启动第一批连接被误判断开。

当前内置命令：

| 命令 | 作用 |
|---|---|
| `bind` | 把当前连接绑定为业务 virtual_fd，可同时入组 |
| `send_to_sb` | 按 virtual_fd 单发 |
| `send_to_gp` | 按 group_name 组播 |
| `divide_gp` | 重建连接组（assign_group） |
| `set_group` | 把单个连接改到另一组 |
| `close_conn` | 主动关闭指定连接 |

---

## 七、背压与滑动窗口（v0.7 新增）

框架 v0.7 从“无界队列 + 纯请求响应”升级为带背压的流控结构：

- **每连接窗口状态机 ConnectionFlow**：同一连接最多 8 条在途消息；窗口计数、暂停/恢复标志在同一把锁内完成；
- **三层有界任务队列 bounded_task_queue**：divide / work / DB 各自队列有容量与高低水位；
- **Handler PushResult**：网络入口能知道 divide 队列是否可投递，Full 时消息留在 read_buffer，不丢；
- **DB 准入/等待队列**：DB 额度不足时消息进入等待区并暂停该连接读取；SQL 完成后释放额度并补投；
- **业务池结算窗口位**：业务任务没挂起则 worker 返回后归还窗口；协程挂起则等续体真正结束后归还（当前通过 resume 任务再走一遍 worker 归还逻辑）。

背压层级：

```text
连接窗口（限制每连接在途消息）
    → Handler 准入（fast / db 分类）
    → divide 有界队列
    → work 有界队列
    → DB 额度 + DB 有界队列
```

核心语义：

- fast 任务走主任务队列；db 任务先进 DB 准入，额度不足进 DbWaitingAdmission 且暂停读；
- 队列到达高水位时上游生产者阻塞/暂停；排到低水位时批量唤醒等待生产者；
- Reactor 只读“有完整消息且窗口有空位”的连接；投递失败消息不取走，窗口位归还；
- 指标 `bp(d/w/db)=size/high/full` 就是三个队列的当前大小/高水位/累计 Full 次数；DB 另有 `db(queue/wait/credit/active)` 采样。

### 7.1 Handler_epoll_make 的接线

`Handler_epoll_make` 是协议解析层与背压之间的接线器，持有：

```text
RouteClassifier         判断 fast / db
DbCreditGate            DB 是否还有准入额度
DbWaitingAdmission      DB 额度不足时的等待区
IReactorControl         pause_reading / schedule_resume
divide_pool / Handler_divide   正常任务投递链
connect_book            连接名册生命周期回调
```

`on_message` 不再无脑投递，而是返回 PushResult：

```cpp
PushResult Handler_epoll_make::on_message(conn, msg) {
    WorkClass cls = route_->classify(msg);   // 前缀匹配，默认 Fast
    if (cls == WorkClass::Db && !db_gate_->try_acquire()) {
        waiting_->add(conn, msg);              // 进 DB 等待区
        reactor_control_->pause_reading(conn); // 暂停读该连接
        return PushResult::Ok;                 // 消息已被 waiting 收下
    }
    PushResult r = divide_pool_->try_add_task(divide_task{...});
    if (r == PushResult::Full)
        divide_pool_->add_task(divide_task{...});   // 阻塞投递兜底，不丢消息
    return r;   // Ok / Full / Closed
}
```

Reactor 拿到 `Full` 时：归还窗口位、消息留在 read_buffer、不再继续读这条连接，等窗口/队列恢复后重新处理。

### 7.2 队列容量与水位来源（当前取值）

- divide 队列容量 = 解析线程数 × 32；work 队列容量 = 业务线程数 × 32；DB 队列容量 = DB worker 数 × 32（见 divide_pool / thread_pool / DB_pool 构造）；
- DB 额度 limit = max(4, DB 连接池大小)，由 Server::start 创建 DbCreditGate 时收紧；
- ConnectionFlow 默认 window = 8；
- 这些是设计参数，正式接入业务后应按“排队深度 × 单请求内存”重新计算；压测目标是在高水位以下运行。

### 7.3 已发现并修复的背压竞态（v0.8）

1. **Metrics 采样器注册竞态**：必须所有 register_queue_sampler / register_db_sampler 完成后再 start_sampler；
2. **DB 提前 resume**：Box 加 `wake_guard`（shared_ptr<atomic<bool>>），DB worker 完成 SQL 后循环等待 wake_guard 为 false 再投递 resume，防止协程帧还没真正挂起就被并发 resume；
3. **on_connect 可见性竞态**：Reactor::add_connection 先执行 conn->handler->on_connect()，再插入 connections_ 和 epoll，防止客户端立即断开时事件线程与登记线程并发。

---

## 八、数据库层

| 接口 | 交付方式 | 现状 |
|---|---|---|
| `query_db(sql, params)` | `co_await`，返回 DBResult | 已实现：prepared statement + 多行多列 rows |
| `query_db_sync(sql)` | promise/future | 未实现（早期 README 描述过，当前代码没有该接口；长任务线程未来需要时再加） |
| `query_db_cb(sql, cb)` | 回调 | 未实现 |

要点：

- `connect_pool`：N 条 MariaDB 连接，借/还阻塞队列实现；`shutdown()` 先关连接、再唤醒等待者（先关连接池后 join 的顺序是优雅退出正确的关键）；
- `DB_pool`：独立 worker 线程池 + 有界队列；借连接 → prepared statement 参数化执行 → 完整结果写 rows/err → 还连接 → release credit → 等 wake_guard → 投 resume；
- 连接建立时设置 connect/read/write 超时；
- DB 结果不做行数限制，限制归业务层；连接池重连、慢查询统计属后续细化项；
- 参数化只针对“值”。表名/列名/排序方向等结构不能参数化，业务层必须用白名单校验。

---

## 九、指标与日志

### 9.1 Metrics：原子计数 + 独立采样线程

埋点入口是 `Handler_metrics` 接口（请求开始/完成、模块任务完成、错误、连接、入队/出队），实现是 `Metrics`：业务线程只做原子累加，采样线程每秒 snapshot 一次。默认 MetricsConfig 全开。每条日志 = 一秒窗口的统计。

| 输出字段 | 含义与公式 |
|---|---|
| `req_qps` | 业务请求级瞬时 QPS =（本期 total_requests − 上期 total_requests）÷ interval(1s)。只有 is_business=true 的任务计入 |
| `req_avg60` | 最近 60 个 req_qps 样本的算术平均（滚动窗口），用于看长时间平均吞吐 |
| `req_avg` | 业务请求端到端平均延迟 = 本期延迟增量 ÷ 本期请求增量，单位 ms |
| `req_p99` | 延迟直方图近似 P99：8 个桶（<1ms / <5ms / <10ms / <50ms / <100ms / <500ms / <1s / ≥1s），统计本期差值，累加到 ≥99% 时返回该桶上界 |
| `inflight` | on_request_started +1、on_request_done −1（业务请求在途数，瞬时值） |
| `conns` | on_conn_open − on_conn_close（框架当前管理的连接数） |
| `[divide/work/db] qps` | 模块任务瞬时 QPS，公式同 req_qps，计数来自 on_module_task_done(PoolId) |
| `[divide/work/db] avg/p99` | 模块任务平均延迟 / P99；work 约为业务 fn 执行耗时，divide 为解析耗时，db 为 SQL 执行耗时 |
| `queue(d/w/db)` | 入队/出队计数差值（queue_depth_ 原子加减）。注意 resume/on_event 任务也走 work 队列，work 的 queue 含恢复任务 |
| `bp(d/w/db)=size/high/full` | 采样器读真实有界队列：当前长度 / 高水位 / 累计 Full 次数 |
| `db(queue=size/high/low/full wait=.. credit=.. active=..)` | DB 队列真实水位 + DbWaitingAdmission 长度 + DbCreditGate 可用/上限 + DB worker 正在执行的 SQL 数 |
| `err(d/w/db)` | 分阶段累计错误（ErrorStage::Divide/Work/DB） |
| `cpu` | 进程 CPU 时间差值 ÷ 墙钟窗口 × 100；>100% 表示多核合计 |
| `rss` | /proc/self/statm 第 2 字段 × 4KB |

真实日志示例：

```text
[INFO] req_qps=8182 req_avg60=8182 req_avg=0.105ms req_p99=1ms inflight=7 conns=2000
[divide] qps=8182 avg=0.010ms p99=1ms [work] qps=8182 avg=0.043ms p99=1ms
[db] qps=4091 avg=0.150ms p99=1ms queue(d/w/db)=3/10/2
bp(d/w/db)=3/512/0|10/640/0|2/1600/0
db(queue=2/1600/800/0 wait=0 credit=48/50 active=2) err(d/w/db)=0/0/0
cpu=568% rss=20500KB
```

用 `tools/parse_metrics.py` 可把 server.log 里的指标行汇总成平均/峰值表格。

### 9.2 Logger：队列 + 后台写文件

- 级别：Debug(0) < Info(1) < Warn(2) < Error(3)，构造函数 min_level 过滤，默认 Info；
- 调用方只 push（mutex + deque），后台线程每 50ms 或唤醒时批量写文件；默认输出当前目录 `server.log`，file 为空则写 stderr；
- 行格式：`[YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] 消息`；
- 优雅退出 `flush_and_stop()`：先让写线程退出并 join，再最后 flush 剩余队列，保证日志不丢。

---

## 十、设计要点与注意事项（维护者速读）

- 长度头协议解决粘包半包；非法头部逐字节跳过重新同步；
- epoll 使用 ET 模式，读到 EAGAIN/EWOULDBLOCK 才停；发送也走非阻塞；
- 唤醒必须通过任务重新入队，不在完成线程直接 resume；DB worker 完成 SQL 后还必须等 wake_guard=false；
- 白板是 thread_local；协程恢复时 resume 任务带 conn，白板会重设；
- `DB_pool::shutdown()` 幂等 + joinable 防护；connect_pool 必须先 shutdown 再让 DB worker join；
- SQL 已参数化（prepared statement），表名/列名需业务层白名单校验；
- 断线后 send 返回 false；业务层据此收尾，不直接操作 socket；
- on_connect 必须在连接进入 Reactor 可见表之前执行；
- 共享回调/采样器必须先注册完成再启动读取线程（Metrics 教训）；
- 增加新池/新指标时：PoolId / ErrorStage 枚举加项要插在 Count 前面；Handler_metrics 加埋点方法时所有实现同步更新；
- 增加新 Handler 时保持“接口 + make 接线 + 工厂”模式，业务/网络层不要反向依赖实现；
- 现在有三个全局单例：g_work_pool / g_db_handler / g_framework_call（context.h 靠它们工作）。单进程只应创建一个 Server；
- EventTask 的 unhandled_exception 直接 terminate；协程业务里能处理的错误要自己 try/catch 或依赖框架 worker 层兜底，不要抛到协程外面。

---

## 十一、文件结构

```text
aggregation/
├─ Internalconnection.h        // 连接结构
├─ Handler_epoll.h             // 网络层 Handler 接口
├─ Handler_epoll_make.h/.cpp   // 网络层 Handler 实现（背压接线 + connect_book）
├─ Handler_divide.h            // 解析→业务 Handler 接口
├─ Handler_divide_make.h/.cpp  // 解析→业务 Handler 实现
├─ Handler_DB.h                // 业务→数据库 Handler 接口
├─ Handler_DB_make.h/.cpp      // 业务→数据库 Handler 实现
├─ Handler_batch.h             // 批处理接线接口
├─ Handler_batch_make.h/.cpp   // 批处理实现
├─ Handler_metrics.h           // 指标埋点接口
├─ Handler_log.h               // 日志接口
├─ divide_task.h / divide_pool.h/.cpp       // 解析任务与解析池
├─ work_task.h / work_pool.h/.cpp           // 业务任务与业务池
├─ thread_pool.h/.cpp          // 通用 worker（白板/指标/窗口归还）
├─ blockedtask.h / blockingqueue.h/.cpp     // 挂起协程登记表
├─ Box.h / DBResult.h / DB_task.h           // 信箱/结果/DB 任务
├─ ErrorHandler.h              // 错误回调类型
├─ EventTask.h / EventAwaiter.h/.cpp        // 协程类型与 awaiter
├─ context.h/.cpp              // 业务唯一接口
├─ thread_context.h            // thread_local 白板与全局单例
├─ connect_book.h/.cpp         // 连接名册
├─ FrameworkCall.h/.cpp        // 框架调用命令分发
├─ ConnectionSession.h/.cpp    // 框架外长连接会话句柄
├─ ConnectionFlow.h            // 每连接窗口状态机
├─ bounded_task_queue.h        // 通用有界任务队列
├─ backpressure.h              // RouteClassifier / DbCreditGate / DbWaitingAdmission
├─ ReactorControl.h/.cpp       // Reactor 控制接口适配
├─ connect_pool.h/.cpp         // 数据库连接池
├─ DB_pool.h/.cpp              // 数据库线程池
├─ epoll.h/.cpp                // 单 Reactor 旧实现（参考）
├─ Reactor.h/.cpp              // 多 Reactor（含心跳）
├─ Acceptor.h/.cpp             // 主线程 accept
├─ NetworkServer.h/.cpp        // 网络层集成类
├─ BatchSender.h/.cpp          // 批处理模块
├─ MetricsConfig.h / Metrics.h/.cpp  // 指标系统
├─ Logger.h/.cpp / LoggerStderr.h    // 正式日志
├─ Server.h/.cpp               // 集成类
├─ main.cpp                    // 业务演示 + 压测命令 + DB 配置
├─ tools/                      // 压测脚本 + 报告 + 单元测试
└─ logs/                       // 历史运行日志（协作副本归档）
```

---

## 十二、压测结论（本机回环）

> 完整数据、全部脚本与分阶段演进见 `tools/PRESSURE_TEST.md`。测试环境：WSL / i7-12700H（20 逻辑线程）/ MariaDB / 本机回环。WSL 数据只适合相对比较，正式部署前应在原生 Linux + 更强压测工具复核。

### 12.1 v0.8 并发修复回归（2026-09-05）

| 场景 | 请求量 | 正确率 | QPS | 备注 |
|---|---:|---:|---:|---|
| 2000 连接 × 16，3 分钟 | 1,492,192 | 100% | 8,192 | 零崩溃、零 conn_fail |
| TSAN 2000×16 | 1,062,432 | 100% | 7,469 | 无 data race 报告 |
| 1.5s 超载流水线 | 2,311,333 | 99.9998% | 27,505 | 峰值约 71k；DB credit 到 0 |
| 1 小时随机连接抖动 | 34,169,909 | 99.99992% | 9,477 | 104,633 次建连；timeout 26 |
| v0.7 30 分钟背压混合 | 16,925,776 | 100% | 9,401 | 队列/水位/DB 指标全绿 |

### 12.2 更早的关键对照

| 场景 | 结果 | 结论 |
|---|---|---|
| 单 Reactor 流水线 10 万 | QPS 10,658 | 网络单核阶段上限 |
| 多 Reactor 20 万 | QPS 15,087 | 事件处理扩展到多核 |
| 纯 hello（DB）20 万 | QPS 12,053 | DB 连接数是早期主要瓶颈之一 |
| DB 连接池 20→50 纯 hello 20 万 | QPS 11,648-28,133 | 扩连接后瓶颈转移到 CPU/查询本身 |
| 纯 ping 20 万 | QPS 11,370 | 受 20 业务线程上限约束 |
| 重业务 5ms/10ms（20 线程） | QPS 3,870 / 1,964 | 接近理论值 线程数 ÷ 耗时 |
| v0.6 半小时混合（1800s） | 18,178,592 请求，0 错误 | 稳定窗口约 1 万 QPS，RSS 稳定 ~20MB |

### 12.3 客观评价

- 正确性：累计千万级请求无串包/漏回/错序；压测客户端与服务器指标自洽；
- 稳定性：长时间运行 RSS 无持续增长；2000×16 高并发修复后零崩溃；TSAN 复验无报告；
- 吞吐：请求-响应稳定窗口约 1 万 QPS；预建连爆发可更高；重业务上限由业务线程数决定；
- 瓶颈：当前主要是 CPU（20 逻辑线程）、DB 查询本身、压测客户端（Python）与业务线程配置，不是网络层单点；
- 与生产级框架的差距：syscall 优化（readv/writev、recvmmsg/sendmmsg、io_uring）、内存池、编排/发现/容灾等均未做，属后续专项。

---

## 十三、当前状态与下一步（回来先看这里）

### 已完成（v0.8 全链路）

- 全链路：epoll → 解析 → 业务 → 协程 → 参数化 DB → 唤醒 → 回发；
- 多 Reactor + 哈希连接表 + BatchSender 批处理 + 心跳清理；
- 错误回调、优雅退出、协程取消结算、结构化多行多列结果；
- 指标系统（请求级 + 分模块 QPS/P99 + 队列/DB/系统指标）+ 正式日志；
- 框架调用层：连接名册 + FrameworkCall + ConnectionSession，业务层可单发/组播/分组/主动关闭；
- v0.7 背压：ConnectionFlow 窗口状态机、三层有界队列、高低水位、DB 准入/等待、Handler PushResult；
- v0.8 并发修复：Metrics 注册顺序、DB wake_guard、on_connect 可见性；
- 压测脚本与报告、中文 README（本文件）。

### 下一步（按“为什么还没做 / 回来先做哪件”排优先级）

1. **退出机制的挂起协程登记/取消协议**：现在 settle_pending 依赖 blockingqueue.take_all 与 wait_idle，关闭后重新投递可能不执行。要做成“业务任务开始时登记协程帧/Box，退出时显式取消所有挂起并把 resume 强制排到队首”，而不是关闭后再投。先给 Box/EventAwaiter 加全局登记表，再改 Server::stop 顺序（对应 5.8 节）；
2. **队列低水位信号外接 Reactor**：bounded_task_queue 的低水位只 notify 本队列的阻塞 push 等待者；当前 divide 排空后依赖 resume/下一事件恢复暂停连接，没有“divide 空位了主动告诉 Reactor”的回调。v0.7 目标是把低水位回调接到 DbWaitingAdmission / Reactor 的 schedule_resume；
3. **业务按类型分池隔离**：重任务（heavy）与轻任务同池会互相排队。解析层 RouteClassifier 已有前缀，可扩展为“业务池路由”，按消息类型投不同 work_pool；
4. **运维/管理系统**：设计图最右侧的规划：实时监控指标/日志/模块状态，并可通过 FrameworkCall 下发指令的管理端。前置条件是框架调用层稳定、指标字段冻结；
5. **框架外独立长连接线程真实业务接入**：ConnectionSession / connect_book 已能编译并过端到端冒烟，但还没接真实长连接业务（如订单状态机独立线程 + send + wait_version_change）；
6. **连接级业务状态**：重新给 Internalconnection 加 user_data（或换 small map），恢复 context 的 get/set_user_data；
7. **连接池重连与慢查询治理**：connect_pool 固定 N 条连接，DB 断线/超时后没有自动重连策略；DB 层也还没有慢查询单独采样；
8. **外卖三端协议与真实业务**：框架本身先冻结；下一步是在 divide_work / 业务类里定义外卖协议与业务模块（订单/商家/骑手），并配套业务单测。

### 框架边界（设计上就不打算由框架解决）

- ❌ 不承载“持续运行”的逻辑：长任务放业务层独立线程；
- ❌ 挂起协程状态在内存中，重启丢失：长流程状态必须落数据库；
- ✅ 长任务可借助框架短任务获取数据 / 触发事件；
- ✅ 单机本机回环稳定窗口约 1 万 QPS；更高吞吐/多机需多实例 + 网关横向扩展；
- 运维仪表盘、配置热更新（XML/TOML）、分布式部署、TLS、HTTP/WebSocket 协议适配均未做。

---

## 十四、压测脚本索引（tools/）

| 脚本 | 模式 | 说明 |
|---|---|---|
| `pressure_test.py` | 串行 | 每连接发一条等一条，功能验证 |
| `pressure_big.py` | 高并发串行 | 线程池并发连接 |
| `pressure_pipe.py` | 流水线 | 每连接一次发一批再统一收 |
| `pressure_mix.py` | 混合流水线 | ping/hello 交替 |
| `pressure_mix_heavy.py` | 混合重业务 | ping/heavy 交替 |
| `pressure_heavy.py` | 重业务 | heavy:N |
| `pressure_broadcast.py` | 推送型 | broadcast:N |
| `pressure_conns.py` | 连接承载 | 分批建连 |
| `pressure_continuous.py` | 持续流水线 | 固定连接/深度持续 N 秒 |
| `pressure_preconnect.py` | 预建连批量 | 先建连再灌请求 |
| `pressure_client_latency.py` | 客户端延迟 | 单请求在途 RTT |
| `pressure_churn.py` | 连接抖动 | 持续建连/发请求/断开 |
| `pressure_cycle.py` | 循环批量 | 每轮整批建连/断开重连 |
| `pressure_pipeline_churn.py` | 随机流水线抖动 | 随机 N×M≥100000，错峰建连 |
| `framework_call_test.py` | 框架调用 | bind/单发/组播/分组/关闭端到端 |
| `connect_book_test.cpp` | 单测 | connect_book 登记/rebind/分组/断连 |
| `event_task_state_test.cpp` | 单测 | 协程状态/窗口归还辅助实验 |
| `parse_metrics.py` | 指标解析 | 汇总 server.log 指标 |

长稳压测套路（半小时起步）：

```bash
# 1) 持续固定负载，观察 queue/bp/credit 是否爬升
python3 tools/pressure_continuous.py 500 16 1800
# 2) 随机连接抖动模拟真实错峰
python3 tools/pressure_pipeline_churn.py 1800
# 3) 超载灌压找峰值与背压行为
python3 tools/pressure_pipe.py 500 400
# 4) 结束后统一解析指标
python3 tools/parse_metrics.py server.log
```
