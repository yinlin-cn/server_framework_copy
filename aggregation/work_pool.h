#pragma once
#include"work_task.h"
#include"blockingqueue.h"
#include"blockedtask.h"
#include"thread_pool.h"
#include "ErrorHandler.h"
#include<functional>
#include<cstdint>
using namespace std;
class work_pool {
    thread_pool pool;
    blockingqueue queue_;
public:
    work_pool(int N=8);
    blockingqueue& get_queue();
    void add_task(std::function<void()> task);
    void add_task(work_task task);
    void on_event(uint64_t key);
    void add_blockingtask(blockedtask b);
    void set_error_handler(ErrorHandler h) { pool.set_error_handler(std::move(h)); }   // ② 新增透传
};