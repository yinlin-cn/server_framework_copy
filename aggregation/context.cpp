#include <atomic>
#include "context.h"
#include "connect_book.h"
#include "FrameworkCall.h"
#include "work_pool.h"
#include "Box.h"
using namespace std;
thread_local std::shared_ptr<Internalconnection> tls_current_conn;
thread_local std::exception_ptr tls_coroutine_exception;
work_pool* g_work_pool = nullptr;
Handler_DB* g_db_handler = nullptr;
FrameworkCall* g_framework_call = nullptr;

bool send(const std::string& data) {
    if (tls_current_conn && tls_current_conn->send_function)
        return tls_current_conn->send_function(data);
    return false;
}

bool framework_call(const std::string& cmd,
                    const std::vector<std::string>& args) {
    if (!g_framework_call)
        return false;
    return g_framework_call->call(cmd, args).success;
}

virtual_conn_info get_current_virtual_conn() {
    virtual_conn_info result;
    if (!g_framework_call)
        return result;
    auto book = g_framework_call->connection_book();
    if (!book)
        return result;
    result.valid = book->connection_info(
        tls_current_conn, result.virtual_fd, result.group_name);
    return result;
}

std::vector<uint64_t> get_group_info(int group_name) {
    if (!g_framework_call)
        return {};
    auto book = g_framework_call->connection_book();
    return book ? book->group_virtual_fds(group_name)
                : std::vector<uint64_t>{};
}

EventAwaiter query_db(const std::string& sql, std::vector<std::string> params) {
    static std::atomic<uint64_t> key_alloc{0};
    uint64_t key = key_alloc.fetch_add(1);
    auto box = std::make_shared<Box>();
    box->wait_name = key;
    if (!g_work_pool || !g_db_handler) {
        box->cancelled = true;   // 框架未就绪，直接已取消
        return EventAwaiter{ key, nullptr, box, sql, std::move(params), g_db_handler };
    }
    return EventAwaiter{
        key,
        &g_work_pool->get_queue(),   // 直接用全局，不用 tls
        box,
        sql,
        std::move(params),
        g_db_handler
    };
}
