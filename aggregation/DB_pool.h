#pragma once
#include <condition_variable>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "DB_task.h"
#include "connect_pool.h"
#include "work_pool.h"
#include "Handler_metrics.h"
#include "Handler_log.h"
#include "bounded_task_queue.h"

class DB_pool {
    bounded_task_queue<DBTask> tasks_;
    std::vector<std::thread> workers_;
    connect_pool conn_pool_;
    work_pool* business_pool_;
    int worker_count_;
    std::atomic<bool> stopped_{false};     // 防重复 join，shutdown 只执行一次
    Handler_metrics* metrics_ = nullptr;   // 指标埋点接口，可空
    Handler_log* log_ = nullptr;           // 日志接口，可空
public:
    DB_pool(int conns, int workers, work_pool* business_pool,
            const std::string& host, const std::string& user,
            const std::string& password, const std::string& database,
            unsigned int port = 3306);
    ~DB_pool();

    push_result submit(DBTask task);
    void shutdown();   // 显式关闭：先关连接池，再停 worker 并 join
    bool wait_idle(const std::chrono::milliseconds& timeout);
    void set_metrics(Handler_metrics* m) { metrics_ = m; }
    void set_log(Handler_log* l) { log_ = l; }
    std::size_t queue_size() const { return tasks_.size(); }
    std::size_t queue_high() const { return tasks_.high_water(); }
    std::size_t queue_low() const { return tasks_.low_water(); }
    uint64_t queue_full_count() const { return tasks_.full_count(); }
    int active_queries() const { return active_query_count_.load(); }
private:
    void record_task_enqueued();
    void finish_task();
    void worker_loop();
    std::atomic<int> active_query_count_{0};
    std::size_t unfinished_task_count_ = 0;                 // 已入队但未完成并投递恢复的任务数
    std::mutex idle_mutex_;
    std::condition_variable idle_cv_;
};
