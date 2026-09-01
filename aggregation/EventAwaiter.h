#pragma once
#include"blockedtask.h"
#include"blockingqueue.h"
#include"Handler_DB.h"
#include"thread_context.h"
#include"work_task.h"
#include"Box.h"
#include"DBResult.h"
#include<memory>
#include<string>
#include<cstdint>
#include<coroutine>
#include<functional>
#include<vector>
using namespace std;
struct EventAwaiter {
    uint64_t wait_key;
    blockingqueue* queue;
    std::shared_ptr<Box> box;
    std::string message;
    std::vector<std::string> params;   // SQL 参数，按顺序对应 ?
    Handler_DB* db_handler;
    coroutine_handle<> handle;
    bool await_ready();
    void await_suspend(coroutine_handle<> h);
    DBResult await_resume();
};
