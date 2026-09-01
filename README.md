# 服务器并发框架

> 一个基于 `epoll + 线程池 + 协程` 的短任务服务器框架，目标是为外卖系统（顾客 / 商家 / 骑手三端）提供底层网络、异步业务与数据库支持。
>
> 版本：v0.4（多 Reactor 网络层 + 哈希连接表 + BatchSender 批处理 + 优雅退出 + 结构化查询结果 + 百万级压测验证）

---

## 快速开始

```bash
# 编译（WSL / Linux，需要 C++20 与 MariaDB 客户端库）
sudo apt install libmariadb-dev
g++ -std=c++20 -fcoroutines main.cpp Server.cpp Metrics.cpp BatchSender.cpp Handler_batch_make.cpp \
    NetworkServer.cpp Reactor.cpp Acceptor.cpp epoll.cpp divide_pool.cpp work_pool.cpp \
    thread_pool.cpp blockingqueue.cpp EventAwaiter.cpp context.cpp \
    Handler_epoll_make.cpp Handler_divide_make.cpp Handler_DB_make.cpp \
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

## 一、框架定位

本框架处理**短任务**：一条消息从进入到回复，就是一个短任务的完整生命周期。

```
网络层收包 → 解析 → 业务处理 → 回复
```

它擅长用有限线程高效处理大量并发小任务；等待（查库、等事件）通过协程挂起，不占线程。

### 两种运行模式

| 模式 | 主驱动 | 业务层形式 | 适用场景 |
|---|---|---|---|
| 模式 A：框架为主 | 消息请求 | 快速完成的函数库 | 外卖日常请求、CRUD 服务、网关 |
| 模式 B：短任务为主 | 持续流程 | 长任务跑主流程，框架提供 IO / 数据 / 事件 | 订单状态机、定时批处理 |

判断标准：**谁是主驱动？** 消息请求 → 模式 A；持续流程 → 模式 B。两者可并存。

---

## 二、架构总览

```
┌─ 网络IO层（多 Reactor）────────────────────┐
│  Acceptor accept → 连接轮询分到 Reactor     │
│  收包 → 长度头拆包 → 组装任务 → 投递         │
│  BatchSender 攒批唤醒 + EPOLLOUT 管理       │
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
│  借连接 → 执行 SQL → 写信箱 → 唤醒             │
└───────────────────────────────────────────────┘
```

| 层 | 职责 | 关键组件 |
|---|---|---|
| 网络 IO 层 | 收发、拆包、连接管理 | `Acceptor` / `Reactor` / `Internalconnection` / `Handler_epoll` |
| 解析层 | 协议解析、路由、任务封装 | `divide_pool` / `Handler_divide` |
| 业务层 | 业务逻辑、等待挂起 | `work_pool` / `Task` / `BlockedTask` / `EventAwaiter` |
| 数据库层 | SQL 执行、结果返回 | `DB_pool` / `connect_pool` / `Box` |
| 集成层 | 组装各层、注入配置 | `Server` / `NetworkServer` |

---

## 三、网络层架构

### 多 Reactor（当前默认）

```
                  ┌─ Reactor 0（epoll + eventfd + 事件线程）→ 连接组 0
Acceptor（主线程）─┼─ Reactor 1（epoll + eventfd + 事件线程）→ 连接组 1
  新连接轮询分配    └─ Reactor N（epoll + eventfd + 事件线程）→ 连接组 N
```

- `Acceptor` 只负责 listen + accept，新连接按轮询分给某个 `Reactor`；
- 每个 `Reactor` 一个 epoll + 一个事件循环线程，只处理自己那组连接，连接表用 `unordered_map`（O(1) 查找）；
- 跨线程发送依赖 `conn->owner_reactor` 记录归属，消息缓冲仍在 `conn->send_queue`；
- 业务线程 `send` 只入队 + 加入待发送桶，由 BatchSender 定时统一唤醒所属 Reactor，Reactor 线程统一写出。

### 单 Reactor（旧实现，保留参考）

`epoll_make`：单 epoll + 单线程事件循环，结构简单，用于理解基础链路。当前 `Server` 默认走多 Reactor。

### BatchSender 批处理

```text
业务线程 send
  → 入 conn->send_queue + 加入 Reactor 待发送桶
  → Handler_batch::on_need_send(reactor)（不立即唤醒）
  ↓
BatchSender flush 线程（2ms）
  → 收集待唤醒的 Reactor（set 去重）
  → 每个 Reactor 一次 eventfd 唤醒
  ↓
Reactor try_send 批量写出
```

`Handler_batch` 是接口，`BatchSender` 是独立模块，Reactor 只依赖接口，无批处理时回退直接唤醒。

---

## 四、核心机制

### 1. 任务携带连接（上下文随任务走）

```cpp
struct Task {
    std::function<void()> fn;
    std::shared_ptr<ConnControlBlock> conn;   // 连接跟着任务走
};
```

### 2. 线程白板（TLS）

- worker 执行任务前：`tls_current_conn = task.conn`；
- 业务函数运行期间：`send()` / `get_user_data()` 通过白板访问当前连接；
- worker 归还前：清空白板。

`tls_current_conn` 是 `thread_local`，每个 worker 线程一份；协程跨线程恢复时，任务对象仍带着连接。

### 3. 协程挂起 + 阻塞队列唤醒

- 业务需要等待 → `co_await` 挂起 → 线程归还；
- 完成方 → `on_event(wait_key)` → 取出阻塞任务 → 重新入队；
- 业务线程 → `resume` → 协程继续。

唤醒必须通过 `on_event` 任务重新入队，不在完成线程直接 `resume`，避免协程池污染。

### 4. 结构化查询结果（DBResult）

```cpp
DBResult res = co_await query_db(sql);
if (res.cancelled) { /* 查询被取消（退出/超时），收尾 */ }
if (!res.ok)       { /* 查询失败，res.err 有描述 */ }
/* res.data 才是正常结果 */
```

DB worker 把成功结果写入 `box->result`，错误写入 `box->err`，业务层必须显式判断状态。

### 5. 错误回调（ErrorHandler）

```cpp
server.set_error_handler([](std::shared_ptr<Internalconnection> conn,
                            const std::string& stage,
                            const std::string& err) {
    // 业务层自己决定：打日志 / 回客户端 / 落库
});
```

`thread_pool` / `divide_pool` 的 worker 对任务执行包了 `try/catch`，异常不再导致进程崩溃。

### 6. 优雅退出

`Server::stop()` 按任务流向逐层放水：

```
关闸（停 accept）→ 解析池关闸 + 排干 → 业务池关闸 → 业务排干（DB 保持可用）
→ BatchSender flush 排空 → 超时兜底取消挂起协程 → 断开全部连接
→ DB 最后关闭 → join 各池 → 清全局
```

- 每个池支持 `shutdown()` 与 `wait_idle(timeout)`（带在途计数）；
- 挂起协程通过 `cancelled` 结算，不泄漏；
- `DB_pool::shutdown()` 幂等，join 前 `joinable()` 防护。

---

## 五、信息流（一条消息的完整旅程）

1. `Reactor` 收到数据，长度头拆包，`Handler_epoll::on_message(conn, msg)`；
2. `Handler_epoll_make` 把消息丢给业务解析函数 `divide_work`，连同连接和 `Handler_divide` 封装成 `divide_task`，提交 `divide_pool`；
3. `divide_pool` worker 执行解析函数，得到业务任务，封装成 `work_task` 提交 `Handler_divide`；
4. `Handler_divide_make` 把 `work_task` 推入 `work_pool`；
5. `work_pool` worker 设置白板，执行任务；需要查库时 `co_await query_db(sql)` 挂起，线程归还；
6. `EventAwaiter` 把恢复任务插入阻塞队列，调用 `Handler_DB::submit(wait_key, box, sql)`；
7. `DB_pool` worker 借连接执行 `mysql_query`，成功写 `box->result` / 失败写 `box->err`，还连接；
8. `DB_pool` 推 `on_event` 任务到 `work_pool`；worker 取出阻塞任务重新入队，协程恢复，`await_resume()` 返回 `DBResult`；
9. 业务继续执行，`send()` 回发（经 BatchSender 攒批 + Reactor 写出）。

---

## 六、核心数据结构

| 名称 | 作用 | 结构 |
|---|---|---|
| `Internalconnection` | 网络连接存储 | `sock` / `read_buffer` / `connected` / `handler` / `send_queue` / `send_mutex` / `send_function` / `owner_reactor` |
| `work_task` | 业务任务 | `fn` + `conn` |
| `divide_task` | 解析任务 | 解析函数 + 连接 + `Handler_divide` |
| `DBTask` | 数据库任务 | `wait_name` + `sql_message` + `box` |
| `blockedtask` | 阻塞任务 | `wait_name` + 完整业务任务 + `box` |
| `blockingqueue` | 阻塞任务队列 | `unordered_map<key, vector<blockedtask>>` |
| `Box` | 信箱 | `result` / `err` / `ready` / `cancelled` / `wait_name` |
| `DBResult` | 结构化查询结果 | `ok` / `cancelled` / `err` / `data` |
| `EventTask` | 协程返回类型 | `promise_type` |
| `EventAwaiter` | 协程控制器 | `wait_key` / `queue` / `box` / `message` / `db_handler` / `handle` |
| `ErrorHandler` | 错误回调 | `(conn, stage, err)` |
| `Server` | 集成类 | 组装各池 / Handler / 网络层，负责优雅退出 |

---

## 七、业务层使用规范

业务代码只 `include context.h`：

```cpp
void send(const std::string& data);              // 回复当前连接（协程内可用）
EventAwaiter query_db(const std::string& sql);   // 协程查询（co_await 返回 DBResult）
// 后续：query_db_sync / query_db_cb
void* get_user_data();                           // 业务状态
void set_user_data(void* data);                  // 业务状态
```

规则：

- ✅ 短任务需要等待 → `co_await`；长任务需要数据 → `query_db_sync`（待实现）；
- ✅ 业务状态挂 `user_data`，业务类无状态；
- ✅ 复杂流程拆成多步短任务；
- ✅ **协程函数参数按值传**（`string msg`，不要 `const string&`）——协程挂起后引用可能失效（压测真实踩过 use-after-free）；
- ❌ 业务池线程里 `sleep` / 忙等 / 阻塞锁 / 直接操作网络；
- ❌ 长任务塞进线程池。

### 短任务 vs 长任务

| 维度 | 短任务（框架） | 长任务（业务层） |
|---|---|---|
| 特征 | 请求-响应周期内完成 | 持续运行 / 占用资源 |
| 等待 | `co_await` 挂起，不占线程 | 本身是流程 |
| 例子 | 下单、查询、登录 | 定时清理、状态机推进 |
| 处理 | 线程池 | 独立线程 |

选择标准：能不能拆成"处理 → 挂起 → 处理"？能 → 短任务；不能（必须一直跑）→ 长任务独立线程。**长任务可以阻塞，短任务不能阻塞。**

| 场景 | 类型 | 数据获取方式 |
|---|---|---|
| 下单 / 查询 / 登录 | 短任务 | `co_await query_db` |
| 订单状态机 | 长任务 | `query_db_sync` |
| 定时清理 | 长任务 | `query_db_sync` |
| 抢单路径 | 短任务 | `co_await` |

### 典型场景

| 场景 | 处理方式 |
|---|---|
| 用户下单 | 短任务：解析 → `co_await` 查库 → 回复 |
| 商家接单 | 短任务：更新状态 → 通知 |
| 骑手抢单 | 短任务：抢单 → 路径 → 回复 |
| 订单生命周期 | 长任务：读状态变化 → 推进 |
| 过期清理 | 长任务：定时扫描 → 标记 |

---

## 八、数据库层

| 接口 | 交付方式 | 适用场景 |
|---|---|---|
| `query_db(sql)` | `co_await`（box + 唤醒，返回 DBResult） | 业务短任务协程 |
| `query_db_sync(sql)` | `promise` / `future`（阻塞，待实现） | 长任务独立线程 |
| `query_db_cb(sql, cb)` | 直接回调（待实现） | 异步通知场景 |

- 连接建立时设置 connect/read/write 超时，防止 `mysql_query` 永久阻塞；
- `connect_pool`：启动创建 N 连接，`get()` 池空阻塞，`release()` 归还唤醒，`shutdown()` 显式关闭；
- `DB_pool`：worker 借连接、执行 SQL、成功写 `box->result` / 失败写 `box->err`、还连接、推 `on_event` 唤醒业务；
- 当前 DB 结果只取第一行第一列；多行多列解析、连接池重连、SQL 参数化属于后续细化项；
- 测试 SQL 使用字符串拼接，接入真实业务前应改为参数化查询或转义，防止 SQL 注入。

---

## 九、Handler 三件套模式

每一层都遵循"接口 + make 实现 + 工厂"：

```text
Handler_epoll.h     接口（on_message / on_connect / on_disconnect）
Handler_epoll_make  实现：持有 divide_pool，把消息封装成 divide_task 推入

Handler_divide.h    接口（on_work(conn, work)）
Handler_divide_make 实现：持有 work_pool，把业务任务推入

Handler_DB.h        接口（submit(wait_key, box, message)）
Handler_DB_make     实现：持有 DB_pool，生成 DBTask 推入

Handler_batch.h     接口（on_need_send(reactor)）
Handler_batch_make  实现：持有 BatchSender，转发待发信号
```

接口负责解耦，make 负责接线，上层只持有接口指针，实现由集成体创建并注入。

---

## 十、稳定性与运维

- **错误处理**：worker 兜异常 + `ErrorHandler` 回调；DB 超时、连接池显式关闭；
- **优雅退出**：按任务流向逐层放水，挂起协程可取消结算，重复 join 有防护；
- **指标（已落地）**：`Metrics` 原子计数器 + 每秒采样线程；业务请求级 `req_qps`（含 60 秒滚动平均 `req_avg60`）；divide / work / db 三模块各一组 `qps / avg / p99`；错误率（分阶段）、队列深度（分池）、连接数、CPU / RSS；模块通过 `Handler_metrics` 接口埋点。
- **日志（设计）**：业务线程提交 + 后台线程批量写文件的队列模式，`Handler_log` 接口解耦，支持级别过滤与优雅退出排空；日志格式可升级为 JSON 行（固定键名）便于指标解析。

---

## 十一、设计要点与注意事项

- 长度头协议解决粘包与半包；非法头部逐字节跳过重新同步；
- `epoll` 使用 ET 模式，读取循环到 `EAGAIN`；发送走缓冲队列 + `EPOLLOUT`；
- 唤醒必须通过 `on_event` 任务重新入队，不在完成线程直接 `resume`；
- 白板是 `thread_local`，不是全局共享变量；
- `DBTask` 在 `Handler_DB_make` 内部生成，控制器只传参数，不接触数据库层类型；
- 协程函数参数按值传，避免挂起后引用失效；
- `DB_pool::shutdown()` 幂等 + 各池 join 前 `joinable()` 防护，防止重复 join 崩溃；
- 优雅退出按任务流向逐层放水，挂起协程通过 `cancelled` 结算；
- 测试 SQL 使用字符串拼接，接入真实业务前应改为参数化查询或转义。

---

## 十二、文件结构

```text
server_framework/
├─ Internalconnection.h        // 连接结构
├─ Handler_epoll.h             // 网络层 Handler 接口
├─ Handler_epoll_make.h/.cpp   // 网络层 Handler 实现（持有 divide_pool）
├─ Handler_divide.h            // 解析→业务 Handler 接口
├─ Handler_divide_make.h/.cpp  // 解析→业务 Handler 实现（持有 work_pool）
├─ Handler_DB.h                // 业务→数据库 Handler 接口
├─ Handler_DB_make.h/.cpp      // 业务→数据库 Handler 实现（持有 DB_pool）
├─ Handler_batch.h             // 批处理接线接口
├─ Handler_batch_make.h/.cpp   // 批处理接口实现 + 工厂
├─ divide_task.h               // 解析任务
├─ divide_pool.h/.cpp          // 解析线程池
├─ work_task.h                 // 业务任务
├─ work_pool.h/.cpp            // 业务工作池（线程池 + 阻塞队列 + on_event + 错误回调透传）
├─ thread_pool.h/.cpp          // 通用线程池（worker 设白板 + 兜异常 + shutdown/wait_idle）
├─ blockedtask.h               // 阻塞任务（带 box）
├─ blockingqueue.h/.cpp        // 阻塞任务队列（take / take_all）
├─ Box.h                       // 信箱（result / err / cancelled）
├─ DBResult.h                  // 结构化查询结果
├─ ErrorHandler.h              // 错误回调类型
├─ EventTask.h                 // 协程返回类型
├─ EventAwaiter.h/.cpp         // 协程控制器（可取消）
├─ context.h/.cpp              // 业务唯一接口：send / query_db / 白板
├─ thread_context.h            // thread_local 白板与全局单例声明
├─ DB_task.h                   // 数据库任务
├─ connect_pool.h/.cpp         // 数据库连接池（超时 + shutdown）
├─ DB_pool.h/.cpp              // 数据库线程池（显式 shutdown）
├─ epoll.h/.cpp                // 单 Reactor epoll 事件循环（旧实现，保留参考）
├─ Reactor.h/.cpp              // 多 Reactor：单 epoll + eventfd + 事件线程 + 待发送桶
├─ Acceptor.h/.cpp             // 多 Reactor：主线程 accept + 连接轮询分配
├─ NetworkServer.h/.cpp        // 网络层集成类：组装 Acceptor + N 个 Reactor
├─ BatchSender.h/.cpp          // 批处理模块：攒 Reactor 待发信号，定时统一唤醒
├─ MetricsConfig.h             // 指标配置（开关）
├─ Handler_metrics.h           // 指标埋点接口（模块只依赖它）
├─ Handler_log.h               // 日志接口（Metrics 快照出口）
├─ Metrics.h/.cpp              // 指标实现：原子计数器 + 采样线程 + 分模块 QPS/P99 + 滚动平均
├─ LoggerStderr.h              // 最简单日志实现（stderr）
├─ Server.h/.cpp               // 集成类：组装各层 + 优雅退出
├─ main.cpp                    // 业务演示：集成 + 信号处理
└─ tools/                      // 压测脚本 + 压测报告
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
| 重业务 5ms | 业务池 20 线程 | 5 万 | 100% | 3870 |
| 重业务 10ms | 业务池 20 线程 | 5 万 | 100% | 1964 |
| 混合重业务（ping + heavy:5） | 500 连接 × 100 | 5 万 | 100% | 7722 |
| 连接承载 | 5 万连接（环境上限 2.8 万） | 5.6 万响应 | 100% | 稳定 |

核心指标：

- 累计 100 万+ 请求全部正确，无超时、无崩溃、无泄漏；
- 多 Reactor 轻任务峰值约 1.5 万 QPS；重业务符合"线程数 ÷ 单任务耗时"模型；
- 连接承载受测试环境（WSL）限制，已建 2.8 万连接全部正确处理；
- 当前瓶颈：CPU / DB 查询本身 / 压测客户端；框架层吞吐优化空间已很小；
- 详细数据见 `tools/PRESSURE_TEST.md`。

---

## 十四、当前状态与下一步

**已完成**

- 全链路跑通：`epoll → 解析层 → 业务层 → 协程 → 真实数据库 → 唤醒 → 回发`；
- 集成类 `Server` + 网络层集成类 `NetworkServer`；
- 多 Reactor + 哈希连接表 + BatchSender 批处理；
- 错误回调、优雅退出、协程取消结算、结构化查询结果；
- 指标系统：`Metrics` + 采样线程，业务请求级 QPS / 分模块（divide/work/db）QPS·P99 / 错误 / 队列 / 连接 / CPU / RSS；
- 压测脚本与报告：`tools/`。

**下一步（按优先级）**

1. 正式日志系统（`Logger` 队列 + 后台写文件，JSON 行格式）；
2. 队列背压（有界队列，防极端负载内存暴涨）；
3. 业务按类型分池隔离（重任务不拖累轻任务）；
4. 路由注册表与连接名册（`cmd → Handler`、广播/通知）；
5. 心跳与僵尸连接清理；
6. SQL 参数化、多行多列解析、连接池重连；
7. 定义外卖三端协议与业务层。

---

## 十五、框架边界

- ❌ 不承载"持续运行"的逻辑（长任务放业务层独立线程）；
- ❌ 挂起协程状态在内存中，服务器重启会丢失 → 长流程状态落数据库；
- ✅ 长任务可借助框架短任务获取数据 / 触发事件；
- ❌ 单机吞吐上限约 1.5 万 QPS（受 CPU / DB / 客户端限制），更高需多实例 + 网关横向扩展。
