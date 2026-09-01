#pragma once
#include"work_task.h"
#include"blockingqueue.h"
#include"blockedtask.h"
#include"thread_pool.h"
#include "ErrorHandler.h"
#include "Handler_metrics.h"
#include<functional>
#include<cstdint>
#include<chrono>
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
    void set_metrics(Handler_metrics* m) { pool.set_metrics(m); }   // 指标透传
    void shutdown() { pool.shutdown(); }   // 透传：置业务线程池停止标志
    bool wait_idle(const std::chrono::milliseconds& timeout) { return pool.wait_idle(timeout); }   // 透传：等待在途业务任务完成
};
