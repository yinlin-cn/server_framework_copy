# 服务器并发框架

> 一个基于 `epoll + 线程池 + 协程` 的短任务服务器框架，目标是为外卖系统（顾客 / 商家 / 骑手三端）提供底层网络、异步业务与数据库支持。
>
> 版本：v0.5（多 Reactor + 批处理 + 指标 + 日志 + 心跳 + 数据库参数化 + 多行多列）

---

## 快速开始

```bash
# 编译（WSL / Linux，需要 C++20 与 MariaDB 客户端库）
sudo apt install libmariadb-dev
g++ -std=c++20 -fcoroutines main.cpp Server.cpp Logger.cpp Metrics.cpp BatchSender.cpp \
    Handler_batch_make.cpp NetworkServer.cpp Reactor.cpp Acceptor.cpp epoll.cpp \
    divide_pool.cpp work_pool.cpp thread_pool.cpp blockingqueue.cpp EventAwaiter.cpp \
    context.cpp Handler_epoll_make.cpp Handler_divide_make.cpp Handler_DB_make.cpp \
    DB_pool.cpp connect_pool.cpp \
    $(mariadb_config --cflags --libs) -I . -o server -pthread

# 运行（先建好 delivery 库和 users 表）
./server

# 测试：协议为 [4 位十进制长度][消息体]
nc 127.0.0.1 9001
0005hello

# 压测
python3 tools/pressure_pipe.py 500 400
```

---

## 一、对外接口与使用规范（业务开发者对接指南）

### 1. 业务入口

框架只认识一个业务入口：**消息解析函数 `divide_work`**。收到一条完整消息后，框架调用它，返回一个可执行的业务任务（`Work`）：

```cpp
using Work = std::function<void()>;
using DivideWork = std::function<Work(const std::string& msg)>;
```

`DivideWork` 通过 `Server` 构造函数传入，业务层在内部做命令分发：

```cpp
Server server(
    [biz](const std::string& msg) -> function<void()> {
        if (msg == "ping") return [biz, msg]() { biz->echo(1, msg); };
        if (msg == "order") return [biz, msg]() { biz->place_order(1, msg); };
        return [biz, msg]() { biz->flow(1, msg); };   // 查库
    },
    9001, 8, 20, 4);   // 端口 / 解析线程 / 业务线程 / Reactor 数
```

### 2. 业务唯一接口（context.h）

业务代码只 `include context.h`，不接触框架内部：

```cpp
bool send(const std::string& data);      // 回复当前连接；返回是否真正入队（连接断开时 false）
EventAwaiter query_db(const std::string& sql, std::vector<std::string> params = {});  // 参数化查询
void* get_user_data();                  // 业务状态
void set_user_data(void* data);         // 业务状态
```

### 3. 必须遵守的原则

- ✅ 协程函数参数**按值传**（`string msg`，不要 `const string&`）——协程挂起后引用可能失效；
- ✅ 业务类**无状态**，请求数据放参数，连接状态挂 `user_data`；
- ✅ 需要等待（查库等）用 `co_await query_db(...)`，协程挂起不占线程；
- ✅ 长任务（订单状态机、定时清理）放**独立线程**，不要塞进业务池；
- ✅ SQL 用**参数化**：`query_db("... WHERE name = ?", { msg })`，不要拼接字符串；表名/列名不能参数化，需业务层白名单校验；
- ✅ 查询结果从 `res.rows`（完整多行多列）取，**限制由业务层自己决定**；
- ✅ 客户端定期发轻量消息（如 `ping`）保持活跃，空闲超过 60 秒会被心跳清理；
- ❌ 业务池线程里 `sleep` / 忙等 / 阻塞锁 / 直接操作网络；
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

        // 业务层自己限制：取第一行第一列
        string name = res.rows.empty() || res.rows[0].empty()
            ? "" : res.rows[0][0];
        if (!send("reply:" + msg + "|" + name)) co_return;   // 连接已断开则收尾
    }
};
```

### 5. 对接步骤

1. 定义 `divide_work`：消息 → 业务任务；
2. 用 `Server` 组装：传 `divide_work`、端口、线程数、Reactor 数；
3. 可选：`set_db_config`（DB 连接池）、`set_error_handler`（业务错误回调）；
4. 编译、运行、用 `nc`/压测脚本验证。

---

## 二、框架定位

本框架处理**短任务**：一条消息从进入到回复，就是一个短任务的完整生命周期。

```
网络层收包 → 解析 → 业务处理 → 回复
```

| 模式 | 主驱动 | 业务层形式 | 适用场景 |
|---|---|---|---|
| 模式 A：框架为主 | 消息请求 | 快速完成的函数库 | 外卖日常请求、CRUD 服务、网关 |
| 模式 B：短任务为主 | 持续流程 | 长任务跑主流程，框架提供 IO / 数据 / 事件 | 订单状态机、定时批处理 |

---

## 三、架构总览

```
┌─ 网络IO层（多 Reactor）────────────────────┐
│  Acceptor accept → 连接轮询分到 Reactor     │
│  收包 → 长度头拆包 → 组装任务 → 投递         │
│  BatchSender 攒批唤醒 + 心跳清理            │
└───────────────────┬─────────────────────────┘
                    ▼
┌─ 任务解析线程池（divide_pool）────────────────┐
│  协议解析 → 路由 → 封装业务任务               │
└───────────────────┬─────────────────────────┘
                    ▼
┌─ 业务工作线程池（work_pool）──────────────────┐
│  Task{fn, conn} → 设置白板 → 执行 → 清空白板   │
│  协程挂起 → 阻塞队列 → on_event 唤醒           │
└───────────────────┬─────────────────────────┘
                    ▼
┌─ 数据库逻辑层（DB_pool + connect_pool）────────┐
│  借连接 → prepared statement 参数化 → 写 rows → 唤醒 │
└───────────────────────────────────────────────┘
```

| 层 | 职责 | 关键组件 |
|---|---|---|
| 网络 IO 层 | 收发、拆包、连接管理、心跳 | `Acceptor` / `Reactor` / `Internalconnection` / `Handler_epoll` |
| 解析层 | 协议解析、路由、任务封装 | `divide_pool` / `Handler_divide` |
| 业务层 | 业务逻辑、等待挂起 | `work_pool` / `Task` / `BlockedTask` / `EventAwaiter` |
| 数据库层 | 参数化执行、结果返回 | `DB_pool` / `connect_pool` / `Box` |
| 集成层 | 组装各层、注入配置 | `Server` / `NetworkServer` |

---

## 四、网络层架构

### 多 Reactor（当前默认）

```
                  ┌─ Reactor 0（epoll + eventfd + 事件线程）→ 连接组 0
Acceptor（主线程）─┼─ Reactor 1（epoll + eventfd + 事件线程）→ 连接组 1
  新连接轮询分配    └─ Reactor N（epoll + eventfd + 事件线程）→ 连接组 N
```

- `Acceptor` 只负责 accept，新连接轮询分给某个 `Reactor`；
- 每个 `Reactor` 一个 epoll + 事件线程，连接表 `unordered_map`（O(1)）；
- 跨线程发送：业务线程入队 + 加入待发送桶 + 通知 BatchSender，Reactor 统一写出；
- **心跳**：连接记录最后活跃时间，事件循环每 5 秒节流扫描，空闲超过 60 秒自动关闭。

### 单 Reactor（旧实现，保留参考）

`epoll_make`：单 epoll + 单线程事件循环，用于理解基础链路。

### BatchSender 批处理

```text
业务线程 send → 入队 + 加待发送桶 + Handler_batch::on_need_send(reactor)
  → BatchSender 定时统一唤醒每个 Reactor
  → Reactor try_send 批量写出
```

---

## 五、核心机制

### 1. 任务携带连接（上下文随任务走）

任务（`work_task` / `divide_task` / `blockedtask`）携带 `shared_ptr<Internalconnection>`，连接生命周期由引用计数保证：连接断开不影响正在跑的任务，`send` 会返回 false 让业务收尾。

### 2. 线程白板（TLS）

worker 执行任务前设置 `tls_current_conn`，业务通过 `send()` / `get_user_data()` 访问当前连接，执行完清空。

### 3. 协程挂起 + 阻塞队列唤醒

- 业务需要等待 → `co_await` 挂起 → 线程归还；
- 完成方 → `on_event(wait_key)` → 重新入队 → `resume`；
- 不在完成线程直接 `resume`，避免协程池污染。

### 4. 结构化查询结果（DBResult）

```cpp
DBResult res = co_await query_db(sql, { msg });
if (res.cancelled) { /* 被取消 */ }
if (!res.ok)       { /* res.err 有描述 */ }
/* res.rows 完整多行多列，业务层自行限制 */
```

### 5. 错误回调（ErrorHandler）

```cpp
server.set_error_handler([](std::shared_ptr<Internalconnection> conn,
                            const std::string& stage,
                            const std::string& err) {
    // 业务层决定：打日志 / 回客户端 / 落库
});
```

worker 对任务执行包 `try/catch`，异常不再导致进程崩溃。

### 6. 优雅退出

```
关闸（停 accept）→ 解析池排干 → 业务池排干 → BatchSender flush
→ 超时兜底取消挂起协程 → 断开连接 → DB 最后关 → join 各池 → 清全局
```

---

## 六、信息流（一条消息的完整旅程）

1. `Reactor` 收包拆包，`Handler_epoll::on_message(conn, msg)`；
2. `divide_pool` 执行 `divide_work(msg)` 得到业务任务，`Handler_divide` 推入 `work_pool`；
3. worker 设白板执行；查库时 `co_await query_db(sql, params)` 挂起，线程归还；
4. `DB_pool` 用 prepared statement 执行，结果写 `box->rows`，推 `on_event` 唤醒；
5. 协程恢复，业务 `send()`（经 BatchSender）回复。

---

## 七、核心数据结构

| 名称 | 作用 | 结构 |
|---|---|---|
| `Internalconnection` | 连接 | `sock` / `read_buffer` / `connected` / `handler` / `send_queue` / `send_function` / `owner_reactor` / `last_active_us` |
| `work_task` | 业务任务 | `fn` + `conn` + `is_business` |
| `divide_task` | 解析任务 | 解析函数 + 连接 + `Handler_divide` |
| `DBTask` | 数据库任务 | `wait_name` + `box` + `sql` + `params` |
| `blockedtask` | 阻塞任务 | `wait_name` + 完整业务任务 + `box` |
| `Box` | 信箱 | `result` / `err` / `rows` / `ready` / `cancelled` / `wait_name` |
| `DBResult` | 查询结果 | `ok` / `cancelled` / `err` / `data` / `rows` |
| `EventTask` / `EventAwaiter` | 协程类型 | 可取消的协程控制器 |
| `ErrorHandler` | 错误回调 | `(conn, stage, err)` |
| `Server` | 集成类 | 组装各层 + 优雅退出 |

---

## 八、数据库层

| 接口 | 交付方式 | 适用场景 |
|---|---|---|
| `query_db(sql, params)` | `co_await`（prepared statement，返回 DBResult） | 业务短任务协程 |
| `query_db_sync(sql)` | `promise` / `future`（待实现） | 长任务独立线程 |
| `query_db_cb(sql, cb)` | 直接回调（待实现） | 异步通知场景 |

- 连接建立时设置 connect/read/write 超时；
- `connect_pool`：N 连接、借/还、`shutdown()` 显式关闭；
- `DB_pool`：prepared statement 执行参数化 SQL，完整结果写 `rows`，错误写 `err`；
- 多行多列完整返回，限制归业务层；连接池重连属于后续细化项。

---

## 九、Handler 三件套模式

接口 + make 实现 + 工厂，每层解耦：

```text
Handler_epoll  接口（on_message / on_connect / on_disconnect）
Handler_divide 接口（on_work(conn, work)）
Handler_DB     接口（submit(wait_key, box, sql, params)）
Handler_batch  接口（on_need_send(reactor)）
Handler_metrics 指标埋点接口
Handler_log    日志接口
```

---

## 十、稳定性与运维

- **错误处理**：worker 兜异常 + `ErrorHandler` + 日志 + 指标三路并行；
- **优雅退出**：逐层放水 + 协程取消结算 + 重复 join 防护；
- **指标**：业务请求级 QPS + 分模块（divide/work/db）QPS/P99 + 滚动平均 + CPU/RSS；
- **日志**：队列 + 后台写文件 + 级别过滤 + 事件日志（连接/异常/慢查询）；
- **心跳**：空闲超时（默认 60s）自动清理僵尸连接。

---

## 十一、设计要点与注意事项

- 长度头协议解决粘包半包；ET 模式读到 EAGAIN；
- 唤醒通过 `on_event` 重新入队，不在完成线程直接 `resume`；
- 白板是 `thread_local`，不是全局共享变量；
- 协程函数参数按值传，避免挂起后引用失效；
- `DB_pool::shutdown()` 幂等 + `joinable()` 防护；
- SQL 已参数化（模板 + prepared statement），表名/列名需业务层白名单校验；
- 断线后 `send` 返回 false，业务层据此收尾，不直接操作 socket。

---

## 十二、文件结构

```text
server_framework/
├─ Internalconnection.h        // 连接结构
├─ Handler_epoll.h             // 网络层 Handler 接口
├─ Handler_epoll_make.h/.cpp   // 网络层 Handler 实现
├─ Handler_divide.h            // 解析→业务 Handler 接口
├─ Handler_divide_make.h/.cpp  // 解析→业务 Handler 实现
├─ Handler_DB.h                // 业务→数据库 Handler 接口
├─ Handler_DB_make.h/.cpp      // 业务→数据库 Handler 实现
├─ Handler_batch.h             // 批处理接线接口
├─ Handler_batch_make.h/.cpp   // 批处理接口实现 + 工厂
├─ Handler_metrics.h           // 指标埋点接口
├─ Handler_log.h               // 日志接口
├─ divide_task.h               // 解析任务
├─ divide_pool.h/.cpp          // 解析线程池
├─ work_task.h                 // 业务任务
├─ work_pool.h/.cpp            // 业务工作池
├─ thread_pool.h/.cpp          // 通用线程池
├─ blockedtask.h               // 阻塞任务
├─ blockingqueue.h/.cpp        // 阻塞任务队列
├─ Box.h                       // 信箱（result / err / rows）
├─ DBResult.h                  // 结构化查询结果
├─ ErrorHandler.h              // 错误回调类型
├─ EventTask.h / EventAwaiter.h/.cpp   // 协程类型
├─ context.h/.cpp              // 业务唯一接口
├─ thread_context.h            // thread_local 白板
├─ DB_task.h                   // 数据库任务
├─ connect_pool.h/.cpp         // 数据库连接池
├─ DB_pool.h/.cpp              // 数据库线程池（参数化执行）
├─ epoll.h/.cpp                // 单 Reactor 旧实现
├─ Reactor.h/.cpp              // 多 Reactor（含心跳）
├─ Acceptor.h/.cpp             // 主线程 accept
├─ NetworkServer.h/.cpp        // 网络层集成类
├─ BatchSender.h/.cpp          // 批处理模块
├─ MetricsConfig.h / Metrics.h/.cpp  // 指标系统
├─ Logger.h/.cpp               // 正式日志
├─ Server.h/.cpp               // 集成类
├─ main.cpp                    // 业务演示
└─ tools/                      // 压测脚本 + 报告
```

---

## 十三、压测结论（本机回环，累计 100 万+ 请求全部正确）

| 场景 | 配置 | 请求量 | 正确率 | QPS |
|---|---|---|---|---|
| 串行 | 1000 连接 × 100 | 10 万 | 100% | 5918 |
| 流水线（多 Reactor） | 500 连接 × 400 | 20 万 | 100% | 15087 |
| hello 查库（50/50 + BatchSender） | 500 连接 × 400 | 20 万 | 100% | 12053 |
| 推送型 broadcast | 500 连接 × 200 条 | 10 万 | 100% | 10699 |
| 混合（ping + hello） | 500 连接 × 200 | 10 万 | 100% | 10239 |
| 重业务 5ms / 10ms | 业务池 20 线程 | 5 万 | 100% | 3870 / 1964 |
| 连接承载 | 5 万连接（环境上限 2.8 万） | 5.6 万响应 | 100% | 稳定 |

核心指标：

- 累计 100 万+ 请求全部正确，无超时、无崩溃、无泄漏；
- 多 Reactor 轻任务峰值约 1.5 万 QPS；重业务符合"线程数 ÷ 单任务耗时"模型；
- 当前瓶颈：CPU / DB 查询本身 / 压测客户端；框架层吞吐优化空间已很小；
- 详细数据见 `tools/PRESSURE_TEST.md`。

---

## 十四、当前状态与下一步

**已完成**

- 全链路：epoll → 解析 → 业务 → 协程 → 参数化 DB → 唤醒 → 回发；
- 多 Reactor + 哈希连接表 + BatchSender 批处理 + 心跳清理；
- 错误回调、优雅退出、协程取消结算、结构化多行多列结果；
- 指标系统（分模块 QPS/P99）+ 正式日志 + 数据库参数化；
- 压测脚本与报告：`tools/`。

**下一步（按优先级）**

1. 队列背压（有界队列，防极端负载内存暴涨）；
2. 业务按类型分池隔离（重任务不拖累轻任务）；
3. 路由注册表与连接名册（`cmd → Handler`、广播/通知）；
4. 连接池重连；
5. 定义外卖三端协议与业务层。

---

## 十五、框架边界

- ❌ 不承载"持续运行"的逻辑（长任务放业务层独立线程）；
- ❌ 挂起协程状态在内存中，重启丢失 → 长流程状态落数据库；
- ✅ 长任务可借助框架短任务获取数据 / 触发事件；
- ❌ 单机吞吐上限约 1.5 万 QPS（受 CPU / DB / 客户端限制），更高需多实例 + 网关横向扩展。
