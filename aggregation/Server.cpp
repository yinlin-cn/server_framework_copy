#include "Server.h"

#include <iostream>

#include "work_pool.h"
#include "connect_book.h"
#include "FrameworkCall.h"
#include "divide_pool.h"
#include "Handler_divide_make.h"
#include "Handler_DB_make.h"
#include "DB_pool.h"
#include "Handler_epoll_make.h"
#include "NetworkServer.h"
#include "Reactor.h"
#include "BatchSender.h"
#include "Handler_batch_make.h"
#include "Logger.h"
#include "Metrics.h"
#include "MetricsConfig.h"
#include "thread_context.h"

namespace {
void clear_globals() {
    g_work_pool = nullptr;
    g_db_handler = nullptr;
    g_framework_call = nullptr;
}
}  // namespace

Server::Server(DivideWork divide_work,
               int listen_port,
               int parse_threads,
               int work_threads,
               int reactor_count)
    : divide_work_(std::move(divide_work)),
      listen_port_(listen_port),
      parse_threads_(parse_threads),
      work_threads_(work_threads),
      reactor_count_(reactor_count),
      route_(std::make_unique<RouteClassifier>()),
      db_gate_(std::make_unique<DbCreditGate>(4096)) {}

Server::~Server() {
    stop();
    clear_globals();
}

void Server::set_db_config(const DBConfig& cfg) {
    db_cfg_ = cfg;
    has_db_ = true;
}

void Server::mark_fast_prefix(const std::string& prefix) {
    route_->add_fast(prefix);
}

void Server::mark_db_prefix(const std::string& prefix) {
    route_->add_db(prefix);
}

bool Server::start() {
    if (started_) return false;
    if (!divide_work_) {
        std::cerr << "[Server] divide_work is not set" << std::endl;
        return false;
    }

    // DB 额度按真实连接池容量收紧，触发 pending 等待路径。
    if (has_db_)
        db_gate_ = std::make_unique<DbCreditGate>(
            std::max(4, db_cfg_.connections));

    // 业务工作池。
    work_pool_ = std::make_shared<work_pool>(work_threads_);
    g_work_pool = work_pool_.get();
    work_pool_->set_error_handler(error_handler_);

    // 连接名册 + 框架调用入口。
    connect_book_ = std::make_shared<connect_book>();
    work_pool_->set_connect_book(connect_book_);
    framework_call_ = std::make_shared<FrameworkCall>(work_pool_);
    framework_call_->set_conn_provider(
        []() { return tls_current_conn; });
    g_framework_call = framework_call_.get();

    // 解析 -> 业务分发器。
    divide_handler_ = std::make_shared<Handler_divide_make>(work_pool_);

    // 数据库层（可选）。只有配置了 DB 才创建，避免无 DB 时启动失败。
    if (has_db_) {
        db_pool_ = std::make_shared<DB_pool>(
            db_cfg_.connections, db_cfg_.db_workers, work_pool_.get(),
            db_cfg_.host, db_cfg_.user, db_cfg_.password,
            db_cfg_.database, db_cfg_.port);
        db_handler_ = std::make_shared<Handler_DB_make>(db_pool_);
        g_db_handler = db_handler_.get();
        db_pool_->set_db_credit_gate(db_gate_.get());
    }

    // 解析线程池 + 网络工厂。
    parse_pool_ = std::make_shared<divide_pool>(parse_threads_);
    parse_pool_->set_error_handler(error_handler_);
    reactor_control_ = std::make_unique<ReactorControl>();
    db_waiting_ = std::make_shared<DbWaitingAdmission>(
        db_gate_.get(),
        [this](std::shared_ptr<Internalconnection> conn,
               const std::string& msg) {
            auto parse = [this, msg]() -> std::function<void()> {
                return divide_work_(msg);
            };
            parse_pool_->add_task(
                divide_task{parse, conn, divide_handler_});
            if (reactor_control_ && conn)
                reactor_control_->schedule_resume(conn);
        });
    factory_ = std::make_unique<Handler_epoll_Factory_make>(
        divide_work_, parse_pool_, divide_handler_, connect_book_,
        route_.get(), db_gate_.get(), db_waiting_.get(),
        reactor_control_.get());

    // 批处理模块：攒 Reactor 待发信号，定时统一唤醒。
    batch_sender_ = std::make_unique<BatchSender>(2);
    batch_handler_ = std::make_unique<Handler_batch_make>(batch_sender_.get());
    batch_sender_->start();

    // 指标模块：创建后注入各层，采样线程每秒输出。
    logger_ = std::make_unique<Logger>("server.log", LogLevel::Info);
    logger_->start();
    metrics_ = std::make_unique<Metrics>(MetricsConfig{}, logger_.get());

    // 背压指标采样：队列真实 size/high/full + DB 专用计数器
    metrics_->register_queue_sampler(
        PoolId::Divide, [p = parse_pool_.get()]() {
            return QueueMetricsSnapshot{
                p->queue_size(), p->queue_high(), p->queue_low(),
                p->queue_full_count()};
        });
    metrics_->register_queue_sampler(
        PoolId::Work, [w = work_pool_.get()]() {
            return QueueMetricsSnapshot{
                w->queue_size(), w->queue_high(), w->queue_low(),
                w->queue_full_count()};
        });
    if (db_pool_) {
        metrics_->register_queue_sampler(
            PoolId::DB, [d = db_pool_.get()]() {
                return QueueMetricsSnapshot{
                    d->queue_size(), d->queue_high(), d->queue_low(),
                    d->queue_full_count()};
            });
        metrics_->register_db_sampler([this]() {
            return DbMetricsSnapshot{
                db_pool_ ? db_pool_->queue_size() : 0,
                db_pool_ ? db_pool_->queue_high() : 0,
                db_pool_ ? db_pool_->queue_low() : 0,
                db_pool_ ? db_pool_->queue_full_count() : 0,
                db_waiting_ ? db_waiting_->size() : 0,
                db_gate_ ? db_gate_->available() : 0,
                db_gate_ ? db_gate_->limit() : 0,
                db_pool_ ? db_pool_->active_queries() : 0};
        });
    }

    metrics_->start_sampler(1000);   // 注册完成后再启动采样线程
    work_pool_->set_metrics(metrics_.get());
    parse_pool_->set_metrics(metrics_.get());
    if (db_pool_) db_pool_->set_metrics(metrics_.get());
    work_pool_->set_log(logger_.get());
    parse_pool_->set_log(logger_.get());
    if (db_pool_) db_pool_->set_log(logger_.get());

    // 网络层：集成类 NetworkServer 内部组装 Acceptor + N 个 Reactor。
    network_ = std::make_unique<NetworkServer>(
        listen_port_, reactor_count_, 4096, 4096, factory_.get());
    network_->set_batch_handler(batch_handler_.get());
    network_->set_metrics(metrics_.get());
    network_->set_log(logger_.get());
    framework_call_->set_close_handler(
        [this](std::shared_ptr<Internalconnection> conn,
               const std::string& reason) {
            if (network_) network_->request_close(conn, reason);
        });
    if (!network_->start()) {
        std::cerr << "[Server] network start failed" << std::endl;
        return false;
    }
    started_ = true;
    return true;
}

void Server::stop() {
    if (!started_) return;
    if (network_) network_->stop_accept();           // 1. 关闸：停 accept
    parse_pool_->shutdown();                         // 2. 解析关闸
    parse_pool_->wait_idle(5s);                      //    解析排干
    work_pool_->shutdown();                          // 3. 业务关闸
    work_pool_->wait_idle(5s);                       // 4. 业务排干（DB 保持可用）
    settle_pending();                                // 5. 兜底取消
    work_pool_->wait_idle(5s);                       //    等取消任务跑完
    if (batch_sender_) batch_sender_->flush_and_stop();  // 5.5 排空批处理模块
    if (network_) network_->stop();                  // 6. 停 Reactor 并断连接
    if (connect_book_) connect_book_->shutdown();    // 唤醒可能的版本等待者
    if (db_pool_) db_pool_->shutdown();              // 7. DB 最后关
    if (metrics_) metrics_->stop_sampler();          // 7.5 停指标采样线程
    if (logger_) logger_->flush_and_stop();          // 7.6 排空日志
    clear_globals();                                 // 8. 清全局
    started_ = false;
}

void Server::set_error_handler(ErrorHandler handler) {
    error_handler_ = std::move(handler);
}

void Server::settle_pending() {
    auto pending = work_pool_->get_queue().take_all();
    for (auto& t : pending) {
        if (t.box) t.box->cancelled = true;
        work_pool_->add_task(std::move(t.funtion));   // 重新投递，协程恢复后自己收尾
    }
}
