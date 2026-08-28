#include "Server.h"

#include <iostream>

#include "work_pool.h"
#include "divide_pool.h"
#include "Handler_divide_make.h"
#include "Handler_DB_make.h"
#include "DB_pool.h"
#include "Handler_epoll_make.h"
#include "epoll.h"
#include "thread_context.h"

namespace {
void clear_globals() {
    g_work_pool = nullptr;
    g_db_handler = nullptr;
}
}  // namespace

Server::Server(DivideWork divide_work,
               int listen_port,
               int parse_threads,
               int work_threads)
    : divide_work_(std::move(divide_work)),
      listen_port_(listen_port),
      parse_threads_(parse_threads),
      work_threads_(work_threads) {}

Server::~Server() {
    stop();
    clear_globals();
}

void Server::set_db_config(const DBConfig& cfg) {
    db_cfg_ = cfg;
    has_db_ = true;
}

bool Server::start() {
    if (started_) return false;
    if (!divide_work_) {
        std::cerr << "[Server] divide_work is not set" << std::endl;
        return false;
    }

    // 业务工作池。
    work_pool_ = std::make_shared<work_pool>(work_threads_);
    g_work_pool = work_pool_.get();
    work_pool_->set_error_handler(error_handler_);

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
    }

    // 解析线程池 + 网络工厂。
    parse_pool_ = std::make_shared<divide_pool>(parse_threads_);
    parse_pool_->set_error_handler(error_handler_);
    factory_ = std::make_unique<Handler_epoll_Factory_make>(
        divide_work_, parse_pool_, divide_handler_);

    // epoll 事件循环。
    server_ = std::make_unique<epoll_make>(listen_port_);
    server_->start(factory_.get());
    started_ = true;
    return true;
}

void Server::stop() {
    if (!started_) return;
    server_->stop_accept();                          // 1. 关闸
    parse_pool_->shutdown();                         // 2. 解析关闸
    parse_pool_->wait_idle(5s);                      //    解析排干
    work_pool_->shutdown();                          // 3. 业务关闸
    work_pool_->wait_idle(5s);                       // 4. 业务排干（DB 保持可用）
    settle_pending();                                // 5. 兜底取消
    work_pool_->wait_idle(5s);                       //    等取消任务跑完
    server_->stop();                                 // 停事件线程
    server_->close_all_connections();                // 6. 断连接
    if (db_pool_) db_pool_->shutdown();              // 7. DB 最后关
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