服务器框架 README
一个基于 epoll + 线程池 + 协程的短任务服务器框架
版本：v0.1（设计定稿，待实现）

一、框架定位
本框架是一个短任务处理框架：一条消息从进入到回复，是一个短任务的完整生命周期。
网络层收包 → 解析 → 业务处理 → 回复
框架擅长把大量并发的小任务用有限的线程高效处理完；等待（查库、等事件）通过协程挂起，不占线程。
二、两种运行模式
模式A：框架为主（纯短任务）
消息进来 → 业务函数处理 → 回复 → 完成
业务层 = 快速完成的函数库
长任务基本不存在
适合：外卖日常请求、CRUD 服务、网关。
模式B：长任务为主（业务层主导）
业务层跑长任务主流程
框架短任务为长任务提供 IO / 数据 / 事件通知
适合：订单状态机、定时批处理。
判断标准：谁是主驱动？消息请求 → 模式A；持续流程 → 模式B。两者可并存。
三、架构总览
┌── 网络IO层（epoll 单线程事件循环）──────────┐
│ 收包 → 长度头拆包 → 组装任务 → 投递        │
│ 发送缓冲 + EPOLLOUT 管理                   │
└────────────────┬──────────────────────────┘
                 ▼
┌── 任务解析线程池 ──────────────────────────┐
│ 协议解析（agreement）→ 路由 → 封装业务任务  │
└────────────────┬──────────────────────────┘
                 ▼
┌── 业务工作线程池 ──────────────────────────┐
│ Task{fn, conn} → 设置白板 → 执行 → 清空白板 │
│ 协程挂起 → 阻塞队列 → on_event 唤醒        │
└────────────────┬──────────────────────────┘
                 ▼
┌── 数据库逻辑层 ────────────────────────────┐
│ DB线程池：借连接 → 执行SQL → 写信箱 → 唤醒  │
│ 连接池：借/还 MYSQL*                       │
└────────────────────────────────────────────┘
四、分层设计
层	职责	关键组件
网络IO层	收发、拆包、连接管理	EpollServer / InternalConnection
解析层	协议解析、路由、任务封装	agreement / ParsePool
业务层	业务逻辑、等待挂起	WorkPool / Task / BlockedTask
数据库层	SQL执行、结果返回	DBWorkerPool / DBPool / Box


五、核心机制
1. 任务携带连接（上下文随任务走）
struct Task {
    std::function<void()> fn;
    std::shared_ptr<ConnControlBlock> conn;   // 连接跟着任务走
};
2. 线程白板（TLS）
worker 执行任务前: tls_current_conn = task.conn
业务函数运行期间:  send()/get_user_data() 通过白板访问当前连接
worker 归还前:    清空白板
3. 协程挂起 + 阻塞队列唤醒
业务需要等待（查库/等事件）→ co_await 挂起 → 线程归还
完成方 → on_event(wait_key) → 取出阻塞任务 → 重新入队
业务线程 → resume → 协程继续
4. 业务零侵入接口（context.h）
bool send(const std::string& data);            // 回复当前连接
std::string query_db(const std::string& sql);  // 查询（co_await）
void* get_user_data();                         // 业务状态
void set_user_data(void* data);
业务代码只 include context.h，不接触框架内部结构。
六、短任务 vs 长任务
	短任务（框架）	长任务（业务层）
特征	请求-响应周期内完成	持续运行/占用资源
等待	co_await 挂起，不占线程	本身就是流程
例子	下单、查询、登录	定时清理、状态机推进
处理	线程池	独立线程


判断：能不能拆成"处理→挂起→处理"？能→框架；必须一直跑→独立线程。
七、业务层使用规范
✅ include context.h，用 send/query_db 等接口
✅ 需要等待用 co_await，禁止阻塞
✅ 业务状态挂 user_data，业务类无状态
✅ 复杂流程拆成多步短任务
❌ sleep / 忙等 / 阻塞锁 / 直接操作网络
❌ 长任务塞进线程池
八、典型场景
场景	处理方式
用户下单	短任务：解析 → co_await查库 → 回复
商家接单	短任务：更新状态 → 通知
骑手抢单	短任务：抢单 → 路径 → 回复
订单生命周期	长任务：读状态变化 → 推进
过期清理	长任务：定时扫描 → 标记


九、框架边界
❌ 不承载"持续运行"的逻辑（长任务放业务层独立线程）
❌ 挂起协程状态在内存中，服务器重启会丢失
   → 长流程状态落数据库（订单状态机）
✅ 长任务可借助框架短任务获取数据 / 触发事件


业务层新想法（待实现）
记录日期：2026-08-22
状态：设计想法，未实现

1. on_result 可插拔结果交付
核心：数据库执行层复用，结果交付方式可插拔。
struct DbTask {
    std::string sql;
    int wait_key;
    std::function<void(std::string)> on_result;   // ★ 结果怎么交付
};
DBWorkerPool 只负责：借连接 → 执行SQL → 调 on_result(结果)。
三种交付方式，一个执行层：
接口	交付方式	适用场景
query_db(sql)	co_await（box+唤醒）	业务短任务协程
query_db_sync(sql)	promise/future（阻塞）	长任务独立线程
query_db_cb(sql, cb)	直接回调	异步通知场景


实现要点：
- 同步版内部用 std::promise<std::string>，fut.get() 阻塞等
- 三种接口共用同一个 DBWorkerPool + 连接池
- 选择依据："能不能阻塞"——业务池线程不能阻塞（用协程），独立线程可以（用同步）
2. 长任务复用数据库逻辑
业务层的长任务（独立线程）需要查库时，直接调 query_db_sync：
// 长任务线程（比如订单状态机循环）
void order_lifecycle_loop() {
    while (running) {
        auto orders = query_db_sync("SELECT * FROM orders WHERE status='pending'");
        // 处理订单...
        query_db_sync("UPDATE orders SET status='accepted' WHERE id=...");
    }
}
关键认知：长任务可以阻塞，短任务不能阻塞。 所以长任务用同步接口最简单，短任务用协程接口。
3. 两种运行模式并存
模式A（框架为主）:  消息进来 → 业务函数处理 → 回复
                    业务层 = 快速完成的函数库（大多情况下）

模式B（长任务为主）: 业务层跑主流程，框架短任务提供IO/数据/事件
                    长任务独立线程运行
同一个项目两种并存：大部分消息走模式A，少数持续流程走模式B。
协作方式：
① 数据共享: 框架短任务写共享数据（加锁），长任务读取
② 事件通知: 框架完成 → on_event → 长任务被唤醒
③ 长任务驱动: 长任务主循环调框架接口（query_db_sync等）
4. 业务层接口规范（context.h 操作面板）
// 业务代码唯一 include
bool send(const std::string& data);            // 回复当前连接
std::string query_db(const std::string& sql);  // 协程查询
std::string query_db_sync(const std::string& sql); // 同步查询（长任务）
void* get_user_data();                          // 业务状态
void set_user_data(void* data);                 // 业务状态
业务层规则：
✅ include context.h，用接口操作
✅ 短任务需要等待 → co_await
✅ 长任务需要数据 → query_db_sync
✅ 业务类无状态，状态挂 user_data
❌ 业务池线程里 sleep / 忙等 / 阻塞
5. 短任务/长任务选择标准
能不能拆成"处理→挂起→处理"？
  能 → 短任务，用框架（co_await）
  不能（必须一直跑）→ 长任务，独立线程
场景	类型	数据获取方式
下单/查询/登录	短任务	co_await query_db
订单状态机	长任务	query_db_sync
定时清理	长任务	query_db_sync
抢单路径	短任务	co_await


6. 后续可能的演进（远期）
1. 工作流引擎化: 长任务状态机抽成通用模块（类似 Temporal）
   - 状态持久化到 DB
   - 支持重试、超时、恢复
2. 业务可测试性: context.h 接口可 mock
   - 测试业务函数时注入假的 send/query_db
3. 业务模块化: 每种业务一个独立目录（order/login/rider）
   - 注册路由时按业务模块注册