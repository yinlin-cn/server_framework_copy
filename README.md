# 服务器并发框架

> 一个基于 `epoll + 线程池 + 协程` 的短任务服务器框架，目标是为外卖系统（顾客 / 商家 / 骑手三端）提供底层网络、异步业务与数据库支持。
>
> 版本：v0.2（设计定稿 + 核心链路已实现）

---

## 一、框架定位

本框架处理的是**短任务**：一条消息从进入到回复，就是一个短任务的完整生命周期。

```
网络层收包 → 解析 → 业务处理 → 回复
```

它擅长把大量并发的小任务用有限的线程高效处理完；等待（查库、等事件）通过协程挂起，不占线程。

### 两种运行模式

| 模式 | 主驱动 | 业务层形式 | 适用场景 |
|---|---|---|---|
| 模式 A：框架为主 | 消息请求 | 快速完成的函数库 | 外卖日常请求、CRUD 服务、网关 |
| 模式 B：短任务为主 | 持续流程 | 长任务跑主流程，框架提供 IO / 数据 / 事件 | 订单状态机、定时批处理 |

判断标准：**谁是主驱动？** 消息请求 → 模式 A；持续流程 → 模式 B。两者可并存。

---

## 二、架构总览

```
┌─ 网络IO层（epoll 单线程事件循环）─────────────┐
│  收包 → 长度头拆包 → 组装任务 → 投递          │
│  发送缓冲 + EPOLLOUT 管理                     │
└───────────────────┬─────────────────────────┘
                    ▼
┌─ 任务解析线程池（divide_pool）────────────────┐
│  协议解析（agreement）→ 路由 → 封装业务任务    │
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

分层职责：

| 层 | 职责 | 关键组件 |
|---|---|---|
| 网络 IO 层 | 收发、拆包、连接管理 | `epoll` / `Internalconnection` / `Handler_epoll` |
| 解析层 | 协议解析、路由、任务封装 | `divide_pool` / `Handler_divide` |
| 业务层 | 业务逻辑、等待挂起 | `work_pool` / `Task` / `BlockedTask` / `EventAwaiter` |
| 数据库层 | SQL 执行、结果返回 | `DB_pool` / `connect_pool` / `Box` |

---

## 三、信息流

1. `epoll` 接收信息，把连接信息 `Internalconnection` 和收到的消息封装，提交给 `Handler_epoll`。
2. `Handler_epoll` 把消息作为参数丢给业务层定义的解析函数 `divide_work`，连同连接信息和 `Handler_divide` 一起封装成 `divide_task`，提交给 `divide_pool`。
3. `divide_pool` worker 执行解析函数，得到一个已被消息初始化完成的业务任务，连同连接信息封装成 `work_task`，提交给 `Handler_divide`。
4. `Handler_divide_make` 把 `work_task` 推入 `work_pool`。
5. `work_pool` worker 取出任务，设置白板 `tls_current_conn = conn`，执行任务函数，跑完清空白板。
6. 业务触发数据库协程：`co_await query_db(sql)`，`EventAwaiter` 挂起，把 resume 任务插入阻塞队列，并调用 `Handler_DB::submit(wait_key, box, sql)`。
7. `Handler_DB_make` 生成 `DBTask{wait_name, sql_message, box}` 推入 `DB_pool`。
8. DB worker 从连接池借连接，执行 `mysql_query`，把结果写入 `box->result`，归还连接。
9. `DB_pool` 把 `on_event` 任务推入 `work_pool`（不在 DB 线程直接 resume）。
10. `work_pool` worker 执行 `on_event`，从阻塞队列取出对应任务重新入队，协程恢复，`await_resume()` 返回 `box->result`。
11. 业务继续执行，调用 `send()` 回发消息。

---

## 四、核心数据结构

| 名称 | 作用 | 结构 |
|---|---|---|
| `Internalconnection` | 网络连接存储 | `sock` / `read_buffer` / `connected` / `handler` / `send_queue` / `send_mutex` / `send_function` |
| `work_task` | 业务任务 | `fn` + `conn` |
| `divide_task` | 解析任务 | 解析函数 + 连接 + `Handler_divide` |
| `DBTask` | 数据库任务 | `wait_name` + `sql_message` + `box` |
| `blockedtask` | 阻塞任务 | `wait_name` + 完整业务任务 |
| `blockingqueue` | 阻塞任务队列 | `unordered_map<key, vector<blockedtask>>` |
| `Box` | 信箱 | `result` / `ready` / `wait_name` |
| `EventTask` | 协程返回类型 | `promise_type` |
| `EventAwaiter` | 协程控制器 | `wait_key` / `queue` / `box` / `message` / `db_handler` / `handle` |

---

## 五、核心机制

### 1. 任务携带连接（上下文随任务走）

```cpp
struct Task {
    std::function<void()> fn;
    std::shared_ptr<ConnControlBlock> conn;   // 连接跟着任务走
};
```

### 2. 线程白板（TLS）

- worker 执行任务前：`tls_current_conn = task.conn`
- 业务函数运行期间：`send()` / `get_user_data()` 通过白板访问当前连接
- worker 归还前：清空白板

`tls_current_conn` 是 `thread_local`，每个 worker 线程一份，不是全局共享变量；保证协程跨线程恢复时也能拿到正确连接。

### 3. 协程挂起 + 阻塞队列唤醒

- 业务需要等待（查库 / 等事件）→ `co_await` 挂起 → 线程归还
- 完成方 → `on_event(wait_key)` → 取出阻塞任务 → 重新入队
- 业务线程 → `resume` → 协程继续

唤醒必须通过 `on_event` 任务重新入队，不在完成线程直接 `resume`，避免协程池污染。

### 4. 业务零侵入接口（context.h）

业务代码只 `include context.h`，不接触框架内部结构：

```cpp
bool send(const std::string& data);             // 回复当前连接（协程内可用）
std::string query_db(const std::string& sql);   // 协程查询（co_await）
std::string query_db_sync(const std::string& sql); // 同步查询（长任务独立线程）
void* get_user_data();                          // 业务状态
void set_user_data(void* data);                 // 业务状态
```

---

## 六、短任务 vs 长任务

| 维度 | 短任务（框架） | 长任务（业务层） |
|---|---|---|
| 特征 | 请求-响应周期内完成 | 持续运行 / 占用资源 |
| 等待 | `co_await` 挂起，不占线程 | 本身就是流程 |
| 例子 | 下单、查询、登录 | 定时清理、状态机推进 |
| 处理 | 线程池 | 独立线程 |

**选择标准：** 能不能拆成"处理 → 挂起 → 处理"？
- 能 → 短任务，用框架（`co_await`）
- 不能（必须一直跑）→ 长任务，独立线程

关键认知：**长任务可以阻塞，短任务不能阻塞。** 所以长任务用同步接口 `query_db_sync` 最简单，短任务用协程接口 `query_db`。

| 场景 | 类型 | 数据获取方式 |
|---|---|---|
| 下单 / 查询 / 登录 | 短任务 | `co_await query_db` |
| 订单状态机 | 长任务 | `query_db_sync` |
| 定时清理 | 长任务 | `query_db_sync` |
| 抢单路径 | 短任务 | `co_await` |

---

## 七、业务层使用规范

```cpp
// 长任务线程（比如订单状态机循环）
void order_lifecycle_loop() {
    while (running) {
        auto orders = query_db_sync("SELECT * FROM orders WHERE status='pending'");
        // 处理订单...
        query_db_sync("UPDATE orders SET status='accepted' WHERE id=...");
    }
}
```

规则：

- ✅ `include context.h`，用 `send` / `query_db` / `query_db_sync` 等接口
- ✅ 短任务需要等待 → `co_await`；长任务需要数据 → `query_db_sync`
- ✅ 业务状态挂 `user_data`，业务类无状态
- ✅ 复杂流程拆成多步短任务
- ❌ 业务池线程里 `sleep` / 忙等 / 阻塞锁 / 直接操作网络
- ❌ 长任务塞进线程池

---

## 八、典型场景

| 场景 | 处理方式 |
|---|---|
| 用户下单 | 短任务：解析 → `co_await` 查库 → 回复 |
| 商家接单 | 短任务：更新状态 → 通知 |
| 骑手抢单 | 短任务：抢单 → 路径 → 回复 |
| 订单生命周期 | 长任务：读状态变化 → 推进 |
| 过期清理 | 长任务：定时扫描 → 标记 |

---

## 九、数据库层

三个接口共用**同一个** `DB_pool` + `connect_pool`，区别只在结果怎么交付：

| 接口 | 交付方式 | 适用场景 |
|---|---|---|
| `query_db(sql)` | `co_await`（box + 唤醒） | 业务短任务协程 |
| `query_db_sync(sql)` | `promise` / `future`（阻塞） | 长任务独立线程 |
| `query_db_cb(sql, cb)` | 直接回调 | 异步通知场景 |

实现要点：

- 同步版内部用 `std::promise<std::string>`，`future.get()` 阻塞等待。
- 选择依据：**能不能阻塞**——业务池线程不能阻塞（用协程），独立线程可以（用同步）。
- `connect_pool`：启动时创建 N 个连接；`get()` 池空时阻塞等待，`release()` 归还并唤醒等待者，析构统一关闭。
- `DB_pool`：独立 worker 线程池 + 任务队列；worker 借连接、执行 `mysql_query`、把结果写入 `Box`、还连接、推 `on_event` 任务唤醒业务。
- `Handler_DB_make` 在接口层生成 `DBTask{wait_name, sql_message, box}`，DB 本体不依赖业务层类型。

---

## 十、Handler 三件套模式

每一层都遵循“接口 + make 实现 + 工厂”模式：

```text
Handler_epoll.h     接口（on_message / on_connect / on_disconnect）
Handler_epoll_make  实现：持有 divide_pool，把消息封装成 divide_task 推入

Handler_divide.h    接口（on_work(conn, work)）
Handler_divide_make 实现：持有 work_pool，把业务任务推入

Handler_DB.h        接口（submit(wait_key, box, message)）
Handler_DB_make     实现：持有 DB_pool，生成 DBTask 推入
```

接口只负责解耦，make 负责接线。上层只持有接口指针，具体实现由集成体（main）创建并注入。

---

## 十一、文件结构

```text
server_framework/
├─ Internalconnection.h        // 连接结构
├─ Handler_epoll.h             // 网络层 Handler 接口
├─ Handler_epoll_make.h/.cpp   // 网络层 Handler 实现（持有 divide_pool）
├─ Handler_divide.h            // 解析→业务 Handler 接口
├─ Handler_divide_make.h/.cpp  // 解析→业务 Handler 实现（持有 work_pool）
├─ Handler_DB.h                // 业务→数据库 Handler 接口
├─ Handler_DB_make.h/.cpp      // 业务→数据库 Handler 实现（持有 DB_pool）
├─ divide_task.h               // 解析任务
├─ divide_pool.h/.cpp          // 解析线程池
├─ work_task.h                 // 业务任务
├─ work_pool.h/.cpp            // 业务工作池（线程池 + 阻塞队列 + on_event）
├─ thread_pool.h/.cpp          // 通用线程池（存 work_task，worker 设白板）
├─ blockedtask.h               // 阻塞任务
├─ blockingqueue.h/.cpp        // 阻塞任务队列
├─ Box.h                       // 信箱
├─ EventTask.h                 // 协程返回类型
├─ EventAwaiter.h/.cpp         // 协程控制器
├─ context.h/.cpp              // 业务唯一接口：send / query_db / 白板
├─ thread_context.h            // thread_local 白板与全局单例声明
├─ DB_task.h                   // 数据库任务
├─ connect_pool.h/.cpp         // 数据库连接池
├─ DB_pool.h/.cpp              // 数据库线程池
└─ main.cpp                    // 集成与组装
```

---

## 十二、编译与运行

### 编译

需要 C++20 与 MariaDB / MySQL 客户端开发库：

```bash
sudo apt install libmariadb-dev
```

```bash
g++ -std=c++20 -fcoroutines main.cpp epoll.cpp divide_pool.cpp work_pool.cpp \
    thread_pool.cpp blockingqueue.cpp EventAwaiter.cpp context.cpp \
    Handler_epoll_make.cpp Handler_divide_make.cpp Handler_DB_make.cpp \
    DB_pool.cpp connect_pool.cpp \
    $(mariadb_config --cflags --libs) \
    -I . -o server -pthread
```

> 说明：`libmariadb-dev` 的头文件在 `/usr/include/mariadb/`，与 MySQL 的 `/usr/include/mysql/` 路径不同。使用 MariaDB 时把 `#include <mysql/mysql.h>` 改为 `#include <mysql.h>` 或 `#include <mariadb/mysql.h>`；若坚持用 `<mysql/mysql.h>` 则需安装 `libmysqlclient-dev` 并链接 `-lmysqlclient`。二选一，别混着用。

### 运行与测试

1. 启动数据库并建库建账号建表（示例：`delivery` 库、`users` 表）。
2. 启动服务器：`./server`。
3. 客户端测试：

```bash
nc 127.0.0.1 9001
0005hello
```

协议为 `[4位十进制长度][消息体]`。收到完整消息后，框架会依次经过解析层、业务层、协程、数据库，并把结果回发。

---

## 十三、设计要点与注意事项

- 长度头协议解决粘包与半包；非法头部逐字节跳过重新同步。
- `epoll` 使用 ET 模式，读取循环到 `EAGAIN`；发送走缓冲队列 + `EPOLLOUT`。
- 唤醒必须通过 `on_event` 任务重新入队，不在完成线程直接 `resume`，避免协程池污染。
- 白板是 `thread_local`，不是全局共享变量。
- `DBTask` 在 `Handler_DB_make` 内部生成，控制器只传参数，不接触数据库层类型。
- 当前 DB 结果只取第一行第一列；多行多列解析、连接池重连、SQL 参数化属于后续细化项。
- 测试 SQL 使用字符串拼接，接入真实业务前应改为参数化查询或转义，防止 SQL 注入。

---

## 十四、框架边界

- ❌ 不承载"持续运行"的逻辑（长任务放业务层独立线程）
- ❌ 挂起协程状态在内存中，服务器重启会丢失 → 长流程状态落数据库（订单状态机）
- ✅ 长任务可借助框架短任务获取数据 / 触发事件

### 后续可能的演进（远期）

1. **工作流引擎化**：长任务状态机抽成通用模块（类似 Temporal）——状态持久化到 DB，支持重试、超时、恢复。
2. **业务可测试性**：`context.h` 接口可 mock，测试业务函数时注入假的 `send` / `query_db`。
3. **业务模块化**：每种业务一个独立目录（order / login / rider），注册路由时按业务模块注册。

---

## 十五、当前状态

- **已跑通全链路**：`epoll → 解析层 → 业务层 → 协程 → 真实数据库 → 唤醒 → 回发`（`hello` / `world` 已从 `users` 表真查成功）。
- **下一步**：封装集成类 `Server`；DB 结果解析（多行多列）与连接池重连；SQL 参数化；定义外卖三端协议与业务层。
