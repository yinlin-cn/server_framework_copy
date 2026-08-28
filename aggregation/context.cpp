#include <atomic>
#include "context.h"
#include "work_pool.h"
#include "Box.h"
using namespace std;
thread_local std::shared_ptr<Internalconnection> tls_current_conn;
work_pool* g_work_pool = nullptr;
Handler_DB* g_db_handler = nullptr;

void send(const std::string& data) {
    if (tls_current_conn)
        tls_current_conn->send_function(data);
}

EventAwaiter query_db(const std::string& sql) {
    static std::atomic<uint64_t> key_alloc{0};
    uint64_t key = key_alloc.fetch_add(1);
    auto box = std::make_shared<Box>();
    box->wait_name = key;
    if (!g_work_pool || !g_db_handler) {
        box->cancelled = true;   // 框架未就绪，直接已取消
        return EventAwaiter{ key, nullptr, box, sql, g_db_handler };
    }
    return EventAwaiter{
        key,
        &g_work_pool->get_queue(),   // 直接用全局，不用 tls
        box,
        sql,
        g_db_handler
    };
}