#include "context.h"

thread_local std::shared_ptr<internalconnection> tls_current_conn;

// 框架内部注入点，业务层看不到
thread_local work_pool* tls_work_pool = nullptr;   // 当前业务池
DBHandler* g_db_handler = nullptr;                 // 数据库 handler，启动时注册

bool send(const std::string& data) {
    if (!tls_current_conn) return false;
    return tls_current_conn->send(data);
}

EventAwaiter<blockingqueue<>> query_db(const std::string& sql) {
    static std::atomic<uint64_t> key_alloc{0};
    uint64_t key = key_alloc.fetch_add(1);

    auto box = std::make_shared<Box>();
    box->wait_name = key;

    // 控制器只传参数，不构造 DBTask
    return EventAwaiter<blockingqueue<>>{
        key,
        &tls_work_pool->get_queue(),
        box,
        sql,
        g_db_handler
    };
}