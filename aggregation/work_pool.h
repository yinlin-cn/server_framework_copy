#pragma once
#include"work_task.h"
#include"blockingqueue.h"
#include"blockedtask.h"
#include"thread_pool.h"
#include"connect_book.h"
#include "ErrorHandler.h"
#include "Handler_metrics.h"
#include "Handler_log.h"
#include<functional>
#include<cstdint>
#include<chrono>
using namespace std;
class work_pool {
    thread_pool pool;
    blockingqueue queue_;
    std::shared_ptr<connect_book> book_;   // 连接名册，逻辑上属于业务池这一侧
public:
    work_pool(int N=8);
    blockingqueue& get_queue();
    void add_task(std::function<void()> task);
    void add_task(work_task task);
    void on_event(uint64_t key);
    void add_blockingtask(blockedtask b);
    void set_error_handler(ErrorHandler h) { pool.set_error_handler(std::move(h)); }   // ② 新增透传
    void set_metrics(Handler_metrics* m) { pool.set_metrics(m); }   // 指标透传
    void set_log(Handler_log* l) { pool.set_log(l); }   // 日志透传
    void shutdown() { pool.shutdown(); }   // 透传：置业务线程池停止标志
    bool wait_idle(const std::chrono::milliseconds& timeout) { return pool.wait_idle(timeout); }   // 透传：等待在途业务任务完成
    void set_connect_book(std::shared_ptr<connect_book> b) { book_ = std::move(b); }
    std::shared_ptr<connect_book> connection_book() const { return book_; }
    std::size_t queue_size() const { return pool.queue_size(); }
    std::size_t queue_high() const { return pool.queue_high(); }
    std::size_t queue_low() const { return pool.queue_low(); }
    uint64_t queue_full_count() const { return pool.queue_full_count(); }
};
