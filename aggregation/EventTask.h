#pragma once
#include <atomic>
#include<coroutine>
#include <exception>
#include <memory>
using namespace std;

// 当前 work fn 内的协程是否真正挂起；业务池据此决定是否归还窗口。
extern thread_local bool g_coroutine_suspended;
// 当前 work fn 是否仍在执行；DB 唤醒前要等它结束，避免提前 resume 协程。
extern thread_local std::shared_ptr<std::atomic<bool>> g_current_task_active;

struct EventTask {
    struct promise_type {
        EventTask get_return_object() { return {}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { terminate(); }
    };
};
