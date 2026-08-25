#pragma once
#include"blockedtask.h"
#include"blockingqueue.h"
#include"Handler_DB.h"
#include"thread_context.h"
#include"work_task.h"
#include"Box.h"
#include<memory>
#include<string>
#include<cstdint>
#include<coroutine>
#include<functional>
using namespace std;
struct EventAwaiter {
    uint64_t wait_key;
    blockingqueue* queue;
    std::shared_ptr<Box> box;
    std::string message;
    Handler_DB* db_handler;
    coroutine_handle<> handle;
    bool await_ready();
    void await_suspend(coroutine_handle<> h);
    std::string await_resume();
};