# 服务器并发框架 API 接口文档

> 版本：对齐 `aggregation/` v0.8 实际代码（2026-09-06）
> 适用范围：在框架上开发外卖系统业务层（顾客 / 商家 / 骑手）
> 注意：本文只记录已经落地的接口；设计文档里的 `query_db_sync`、`query_db_cb`、连接级 `user_data`、异步 `framework_call_async` 等均未实现，不作为开发依据。

## 1. 总体说明

框架只认识一个业务入口：`divide_work`（消息到业务闭包的路由函数）。业务代码不接触 Reactor、线程池、DB 连接池等内部对象，只通过 `context.h` 提供的全局接口工作。

单进程只应创建一个 `Server`，因为框架依赖三个全局入口（`g_work_pool` / `g_db_handler` / `g_framework_call`）。

完整架构、线程模型和压测结论见根目录 [README.md](../README.md)。

## 2. 网络协议

### 2.1 帧格式

```text
[4 位十进制 ASCII 长度][消息体]
```

示例：

```text
0005hello
0017reply:hello|hello
```

- 长度是消息体的字节数，不是整帧字节数。
- 4 位十进制可表达的最大长度是 `9999` 字节；超过该长度需要业务层自己拆帧。
- 客户端发送时自己拼长度头；框架收到的每条业务消息已经被剥掉长度头。
- 业务代码调用 `send()` / 框架调用发送时，只传消息体，框架自动补长度头。
- 框架支持同一 TCP 流中连续多帧（流水线）；半包会留在连接读缓冲等待补全。
- 非法长度头会按逐字节跳过的方式重新同步；当前主路径是 `Reactor::take_one_message()`，读缓冲上限为 1MB。

客户端封装示例（Python）：

```python
def frame(payload):
    return ("%04d" % len(payload) + payload).encode()
```

### 2.2 连接与心跳

- 服务端默认监听端口 `9001`，构造 `Server` 时可改。
- 连接空闲超过 60 秒会被框架心跳清理；事件循环约 5 秒节流扫描一次。
- 业务客户端如果长连接但低频，应定期发轻量消息（如 `ping`）保活。

### 2.3 演示消息（`main.cpp` 内置，非框架协议）

框架本身不解析消息体；以下只是当前演示业务使用的消息格式：

| 消息体 | 预期响应 | 链路 |
|---|---|---|
| `ping` | `echo:ping` | fast |
| `hello` | `reply:hello|hello` | 参数化查库 |
| `broadcast:N` | `msg:0 ... msg:N-1` | 单任务多次发送 |
| `heavy:N` | `done:heavy:N` | 模拟 N 毫秒负载 |
| `fwbind:1001:7` | `fwbind-ok` | 绑定当前连接到 virtual_fd=1001、组 7 |
| `fwsend:1001:xxx` | `fwsend-ok` | 按 virtual_fd 单发 |
| `fwgroup:7:xxx` | `fwgroup-ok` | 组播 |
| `fwdivide:7:1001,1002` | `fwdivide-ok` | 重建组 7 的连接集合 |
| `fwclose:1001` | `fwclose-ok` | 主动关闭目标连接 |

真实业务的消息协议、命令字、参数分隔方式由业务层在 `divide_work` 中自行定义和校验。

## 3. 集成类 Server

定义：[Server.h](../aggregation/Server.h)

### 3.1 构造函数

```cpp
Server(DivideWork divide_work,
       int listen_port = 9001,
       int parse_threads = 4,
       int work_threads = 8,
       int reactor_count = 4);
```

类型别名：

```cpp
using Work = std::function<void()>;
using DivideWork = std::function<Work(const std::string& msg)>;
```

参数含义：

- `divide_work`：收到一条完整消息后的路由函数，返回一个可执行业务闭包。
- `parse_threads`：解析线程数（当前 `main.cpp` 用 16）。
- `work_threads`：业务线程数（当前 `main.cpp` 用 20）。
- `reactor_count`：Reactor/事件线程数，默认 4。

### 3.2 配置接口

| 接口 | 作用 | 必须在 start 前调用 |
|---|---|---|
| `set_db_config(const DBConfig&)` | 配置 MariaDB 连接池；不配置则没有 DB 链路 | 是 |
| `mark_fast_prefix(prefix)` | 声明 fast 前缀，不占 DB 额度 | 是 |
| `mark_db_prefix(prefix)` | 声明 db 前缀，走 DB 准入 | 是 |
| `set_error_handler(ErrorHandler)` | 注册业务/解析异常回调 | 是（当前只在 `start()` 注入各池） |
| `start()` | 组装并启动全部模块，返回是否成功 | - |
| `stop()` | 优雅退出编排 | - |
| `settle_pending()` | 结算挂起协程：标记取消并重新投递 | 框架内部使用 |

`DBConfig` 字段：

```cpp
struct DBConfig {
    int connections = 4;          // 连接池大小
    int db_workers = 4;           // DB worker 线程数
    std::string host = "127.0.0.1";
    std::string user;
    std::string password;
    std::string database;
    unsigned int port = 3306;
};
```

路由分类规则（`RouteClassifier::classify`）：先匹配 db 前缀，再匹配 fast 前缀；前缀用 `rfind(prefix, 0) == 0` 判断；都不匹配时按 Fast 处理。

`Server` 生命周期约束：

- `divide_work` 必须已设置；`Server` 拷贝被删除。
- 不提供热重启；正常流程是构造、配置、`start()`、信号等待、`stop()`。
- `stop()` 应在 main 线程执行；内部顺序是先关闸、再逐层排干、DB 最后关。
- 未配置 DB 时调用 `query_db()` 会直接得到已取消的结果（`g_db_handler` 为空）。

### 3.3 最小接入代码

```cpp
Server server(
    [biz](const std::string& msg) -> std::function<void()> {
        if (msg == "ping")   return [biz, msg]() { biz->echo(1, msg); };
        if (msg == "order")  return [biz, msg]() { biz->place_order(1, msg); };
        return [biz, msg]() { biz->flow(1, msg); };
    },
    9001,   // 端口
    16,     // 解析线程数
    20);    // 业务线程数

server.mark_fast_prefix("ping");
server.mark_db_prefix("hello");
server.set_error_handler(my_error_handler);
server.start();
```

## 4. 业务层接口 context.h

定义：[context.h](../aggregation/context.h)

业务代码只应包含此头文件：

```cpp
struct virtual_conn_info {
    bool valid = false;        // 查询是否成功
    uint64_t virtual_fd = 0;   // 当前连接的业务虚拟标识
    int group_name = -1;       // 当前连接所属组，-1 表示未入组
};

bool send(const std::string& data);
EventAwaiter query_db(const std::string& sql,
                      std::vector<std::string> params = {});
bool framework_call(const std::string& cmd,
                    const std::vector<std::string>& args);
virtual_conn_info get_current_virtual_conn();
std::vector<uint64_t> get_group_info(int group_name);
```

### 4.1 send

```cpp
bool send(const std::string& data);
```

- 回复“当前连接”。当前连接来自 worker 的 thread_local 白板 `tls_current_conn`。
- 返回值表示是否真正入队；连接已断开 / 当前没有连接时返回 `false`。
- 只入队，不直接写 socket；实际写出由所属 Reactor 事件线程完成，业务线程不可阻塞等发送。
- `data` 是消息体，框架自动加 4 位长度头。

### 4.2 query_db

```cpp
EventAwaiter query_db(const std::string& sql,
                      std::vector<std::string> params = {});
```

- SQL 模板用 `?` 作参数占位符，参数按顺序对应。
- 参数全部按字符串绑定，由 MySQL/MariaDB 自动转换类型。
- 返回值 `EventAwaiter` 只能配合 `co_await` 使用；查询由 DB worker 执行，协程挂起不占业务线程。
- 表名、列名、排序方向等结构不能参数化，业务层必须使用白名单校验。
- DB 结果没有框架层行数上限；是否加 `LIMIT` 由业务层决定。

示例：

```cpp
DBResult res = co_await query_db(
    "SELECT name FROM users WHERE name = ? LIMIT 1",
    { msg });
```

### 4.3 framework_call

```cpp
bool framework_call(const std::string& cmd,
                    const std::vector<std::string>& args);
```

- 业务层便捷入口，只返回是否成功。
- 命令与参数见第 7 节。
- 需要拿到具体错误文本时，应持有 `FrameworkCall` 对象调用 `call()`，得到 `call_result`。

### 4.4 get_current_virtual_conn / get_group_info

```cpp
struct virtual_conn_info {
    bool valid = false;
    uint64_t virtual_fd = 0;
    int group_name = -1;
};

virtual_conn_info get_current_virtual_conn();
std::vector<uint64_t> get_group_info(int group_name);
```

业务层想对“自己”或“某个组”发起框架调用时，先用这两个接口拿到虚拟连接标识，再把它交给 `framework_call`。返回的都是 `virtual_fd`，不会暴露内部 `Internalconnection` 指针。

`get_current_virtual_conn()`：

- 读取 worker 白板 `tls_current_conn`，再在连接名册里反查当前连接的 `virtual_fd` 和 `group_name`。
- 只能在业务 worker 上下文里调用；脱离业务线程（或没有当前连接）时返回 `valid == false`。
- 查询失败时保持默认值：`valid = false`、`virtual_fd = 0`、`group_name = -1`。
- `connection_info()` 用引用输出参数返回结果，`bool` 只表示查询是否成功。

`get_group_info(int group_name)`：

- 返回该组当前有效连接的 `virtual_fd` 快照（`std::vector<uint64_t>`）。
- 返回的是快照而不是实时视图；调用后组成员变化不会反映到已返回的容器里。
- 名册里已断开的成员会被跳过（内部持 `weak_ptr`，锁定失败即视为无效）。
- 组不存在或组内没有有效连接时返回空 vector。

示例：

```cpp
// 当前连接的虚拟标识与组号
auto self = get_current_virtual_conn();
if (self.valid) {
    framework_call("send_to_sb",
                   {std::to_string(self.virtual_fd), "hello"});
}

// 向整个组广播
auto members = get_group_info(7);
for (uint64_t fd : members) {
    framework_call("send_to_sb", {std::to_string(fd), "hello"});
}
```

## 5. 协程与 EventTask

定义：[EventTask.h](../aggregation/EventTask.h)、[EventAwaiter.h](../aggregation/EventAwaiter.h)

业务协程函数必须返回 `EventTask`：

```cpp
class BusinessLogic {
public:
    EventTask flow(int id, std::string msg);   // 参数按值传
};
```

`EventTask` 语义：

- `initial_suspend` / `final_suspend` 都是 `suspend_never`：函数被调用即开始，不需要 `.resume()`。
- 业务协程没有返回值，用 `co_return;` 结束或直接跑到函数末尾。
- `unhandled_exception` 会把异常暂存到 `tls_coroutine_exception`，再由 work worker 重新抛出，交给框架 worker 层错误处理。

业务编码红线：

- 协程参数一律按值传；协程挂起后，引用参数可能失效。
- 业务类保持无状态；请求数据放参数，状态应设计为连接级或数据库状态。
- 只有 `co_await query_db(...)` 是当前支持的挂起点；不要在线程池任务里 sleep、忙等、阻塞锁。

## 6. DBResult 与错误返回约定

定义：[DBResult.h](../aggregation/DBResult.h)

```cpp
struct DBResult {
    bool ok = false;                    // 是否成功拿到结果
    bool cancelled = false;             // 是否被取消（退出/超时兜底）
    std::string err;                    // 错误描述
    std::string data;                   // ok 时第一行第一列
    std::vector<std::vector<std::string>> rows;   // 完整多行多列结果
};
```

业务判定顺序必须固定：

```cpp
DBResult res = co_await query_db(sql, params);

if (res.cancelled) { /* 被取消：收尾返回 */ }
if (!res.ok)       { /* res.err 有描述 */ }
// 到这里才允许读 res.data / res.rows
```

实现细节：

- SELECT：完整结果写入 `res.rows`；`res.data` 取第一行第一列，作为便捷字段。
- 非 SELECT：`res.data` 为 `"OK"`，`rows` 为空。
- SQL 预编译 / 参数绑定 / 执行失败：写入 `res.err`，不抛到业务协程。
- DB worker 异常只记 Metrics 的 `ErrorStage::DB`，不会进 `ErrorHandler`。
- 连接池当前是固定连接，不做自动重连；慢查询治理、重连策略是后续项。

## 7. 错误处理接口

### 7.1 ErrorHandler

定义：[ErrorHandler.h](../aggregation/ErrorHandler.h)

```cpp
using ErrorHandler = std::function<void(
    std::shared_ptr<Internalconnection> conn,
    const std::string& stage,   // 出错阶段
    const std::string& err)>;   // 错误描述
```

注册：

```cpp
server.set_error_handler(
    [](std::shared_ptr<Internalconnection> conn,
       const std::string& stage,
       const std::string& err) {
        // 记录日志、上报监控或按业务需要回包
    });
```

触发位置（当前代码）：

| stage | 触发点 | 代码位置 |
|---|---|---|
| `"divide"` | `divide_work` 抛出异常 | `divide_pool.cpp` worker catch |
| `"work"` | 业务闭包（非协程）抛出异常 | `thread_pool.cpp` worker catch |

回调调用时机：

- 在对应 worker 线程上执行；回调应快速、线程安全，不能阻塞或投递后等待同一池结果。
- 异常会被框架捕获，不会让 worker 退出或进程崩溃。
- catch 后框架还会走 Logger 与 Metrics 两条路。
- 当前实现只在 `Server::start()` 时把回调注入 divide/work 池，因此必须在 `start()` 之前调用 `set_error_handler`。

需要回包时不要直接写 socket。可以安全调用传入连接的发送入口：

```cpp
if (conn) conn->send_function("ERROR: " + err);
```

`conn->send_function` 内部同样会加长度头并走 Reactor 发送队列，连接断开时返回 `false`。

### 7.2 DB 错误不经过 ErrorHandler

SQL 层错误路径：

```text
DB worker 出错
  -> job.box->err = mysql_stmt_error(...)
  -> 协程恢复后业务看到 DBResult{ ok=false, err="..." }
  -> Metrics 记 ErrorStage::DB
```

因此“查询失败”是业务结果，不是 worker 异常；业务代码按第 6 节顺序判断。

### 7.3 call_result（框架调用错误）

定义：[FrameworkCall.h](../aggregation/FrameworkCall.h)

```cpp
struct call_result {
    bool success = false;
    std::string err;
};
```

`context::framework_call` 把 `success` 折叠成 `bool`；需要错误文本时直接调用：

```cpp
call_result r = fc.call("send_to_sb", {fd, msg});
if (!r.success) log_->warn(r.err);
```

## 8. FrameworkCall 命令

入口有：

- 业务线程：`framework_call(cmd, args)`（全局，返回 bool）。
- 持有 `FrameworkCall` 对象：`fc.call(cmd, args)`（返回 `call_result`）或便捷方法。
- 自定义内部命令：`register_internal(cmd, handler)`。

参数统一为字符串数组，顺序敏感：

| 命令 | 参数 | 作用 |
|---|---|---|
| `bind` | `业务virtual_fd [, 组号]` | 把当前连接从临时 fd 绑定为业务 virtual_fd |
| `send_to_sb` | `virtual_fd, 消息体` | 单发到指定连接 |
| `send_to_gp` | `组号, 消息体` | 组播到指定组 |
| `divide_gp` | `组号, 逗号分隔fd列表` | 重建指定组 |
| `set_group` | `virtual_fd, 组号` | 只修改一个连接的组 |
| `close_conn` | `virtual_fd [, reason]` | 框架侧主动关闭连接 |

### 8.1 bind 语义

- 新连接进入框架时，名册先以 `conn->sock` 作为临时 virtual_fd，组为 `-1`。
- 业务登录/鉴权成功后调用 `bind`，把当前连接换成业务 virtual_fd。
- 业务 virtual_fd 已被其他连接占用时绑定失败。
- 组号省略为 `-1`，表示不进任何组；组号 `>= 0` 才会登记进组。
- `bind` 依赖“当前连接”白板，只能在业务 worker（业务任务或协程内）调用。

### 8.2 发送/分组语义

- `send_to_sb` / `send_to_gp` 发送的是业务消息体，框架自动补长度头。
- `send_to_sb`：目标不存在、已断连、发送入队失败时返回失败。
- `send_to_gp`：遍历组快照逐个入队，不因个别连接失效而失败。
- `divide_gp`：把该组重建为传入的 fd 集合；不在传入列表中的旧成员移出，传入但未登记的 fd 被跳过。
- `set_group`：只把单个连接移入目标组；组号 `< 0` 表示移出所有组。

### 8.3 扩展自定义命令

```cpp
fc.register_internal("notify_rider",
    [](const FrameworkCall::Args& args) -> call_result {
        if (args.size() < 2)
            return {false, "notify_rider needs rider_id,msg"};
        // 查名册并发送，或做业务内部调度
        return {true, ""};
    });
```

约束：

- 内置命令优先于自定义命令，不能覆盖 `bind` 等内置命令。
- `register_internal` 的 handler 在调用线程同步执行；只适合快速、非阻塞操作。
- handler 需要查库时应走业务协程而不是在内部 handler 里同步阻塞。

### 8.4 FrameworkCall 便捷方法

```cpp
call_result send_to_sb(uint64_t virtual_fd, const std::string& msg);
call_result send_to_gp(int group_name, const std::string& msg);
call_result divide_gp(int group_name, const std::vector<uint64_t>& fds);
call_result close_conn(uint64_t virtual_fd, const std::string& reason = "");
```

## 9. 连接名册与 ConnectionSession

### 9.1 connect_book（框架内部）

定义：[connect_book.h](../aggregation/connect_book.h)

保存：

- `virtual_fd -> 连接（weak_ptr）`
- `组号 -> virtual_fd 集合`
- `版本号 + 最近 4096 条 net_changer 变更记录`

业务代码不应直接持有连接名册操作；框架内部通过名册保证发送时连接已断不会悬垂。名册变化命令包括 `add`、`remove`、`rebind`、`set_group`、`divide_gp`，每次变化版本号 `+1`。

名册对外查询方法（由 `context.h` 包装后提供给业务层）：

- `connection_info(conn, virtual_fd, group_name)`：反查连接当前的虚拟标识与组号，`bool` 表示查询是否成功，结果通过引用输出。
- `group_virtual_fds(group_name)`：返回组内当前有效连接的 `virtual_fd` 快照，已断开的成员会被跳过。
- `all_virtual_fds()`：返回名册内全部有效连接的 `virtual_fd` 快照。

### 9.2 ConnectionSession（框架外长连接线程）

定义：[ConnectionSession.h](../aggregation/ConnectionSession.h)

```cpp
bool send(const std::string& msg) const;
call_result call(const std::string& cmd,
                 const FrameworkCall::Args& args) const;
bool close(const std::string& reason = "") const;
bool connected() const;

uint64_t virtual_fd() const;
int group_name() const;
uint64_t version() const;
bool wait_version_change(uint64_t old_version,
                         std::chrono::milliseconds timeout =
                             std::chrono::milliseconds(5000)) const;
```

适用场景：订单状态机、定时器等待名册变化等“框架外持续运行”的线程，需要先持有会话句柄再调用。当前 `Server` 没有暴露创建会话的公开入口，真实接入时需要在集成层把 `FrameworkCall` 提供给长连接线程。

## 10. 模块扩展接口（框架维护者）

各层遵循“接口 + make 接线 + 工厂”模式：

| 接口 | 方法 | 用途 |
|---|---|---|
| [Handler_epoll.h](../aggregation/Handler_epoll.h) | `on_message(conn, msg)` / `on_connect(conn)` / `on_disconnect(conn)` | 网络层消息、连接生命周期 |
| [Handler_divide.h](../aggregation/Handler_divide.h) | `on_work(conn, work)` | 解析层把业务任务交给业务层 |
| [Handler_DB.h](../aggregation/Handler_DB.h) | `submit(wait_key, box, sql, params)` | 提交 DB 任务 |
| [Handler_batch.h](../aggregation/Handler_batch.h) | `on_need_send(reactor)` | Reactor 待发通知 |
| [Handler_metrics.h](../aggregation/Handler_metrics.h) | 请求/模块/错误/连接/队列埋点 | 指标 |
| [Handler_log.h](../aggregation/Handler_log.h) | `info` / `warn` / `error` | 日志 |

`on_message` 返回 `push_result`，值来自 [bounded_task_queue.h](../aggregation/bounded_task_queue.h)：

- `Ok`：已投递。
- `Full`：队列满，消息留在读缓冲，窗口位归还，后续再处理。
- `Closed`：池已关闭。

修改约定：

- `PoolId` / `ErrorStage` 新增枚举项必须插在 `Count` 前面。
- 给 Handler 接口加方法时，所有 make 实现必须同步更新。
- 增加新池/新任务类型时保持接口 + 接线 + 工厂三层，不反向依赖业务。

## 11. 编译与验证

框架依赖 Linux epoll + C++20 协程 + MariaDB 客户端库，当前在 WSL 编译运行：

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

冒烟：

```bash
./server &
printf '0005hello' | nc -q2 127.0.0.1 9001
# 预期：0017reply:hello|hello
```

框架调用端到端：

```bash
python3 aggregation/tools/framework_call_test.py
```

Windows 副本用于协作/文档；实际编译与压测以 WSL 主仓库为准，并定期同步代码（见 README 第 0 节）。
