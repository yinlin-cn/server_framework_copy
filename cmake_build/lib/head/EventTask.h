#pragma once
#include <atomic>
#include<coroutine>
#include <exception>
#include <memory>
using namespace std;
class DB_credit_token;
class coroutine_suspend_guard;

extern thread_local std::exception_ptr tls_coroutine_exception;

// 当前 work fn 内的协程是否真正挂起；业务池据此决定是否归还窗口。
extern thread_local bool g_coroutine_suspended;
// 当前 work fn 是否仍在执行；DB 唤醒前要等它结束，避免提前 resume 协程。
extern thread_local std::shared_ptr<coroutine_suspend_guard> g_current_task_suspend_guard;
// 当前任务占用的 DB 准入额度；await_suspend 把它带到下一条 resume 任务。
extern thread_local std::shared_ptr<DB_credit_token> g_current_task_db_credit_token;

struct EventTask {
    struct promise_type {
        EventTask get_return_object() { return {}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() noexcept {
            tls_coroutine_exception = std::current_exception();
        }
    };
};
