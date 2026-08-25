#pragma once
#include<coroutine>
#include <exception>
using namespace std;
struct EventTask {
    struct promise_type {
        EventTask get_return_object() { return {}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { terminate(); }
    };
};