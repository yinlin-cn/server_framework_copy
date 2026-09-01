#pragma once
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include "DB_task.h"
#include "connect_pool.h"
#include "work_pool.h"
#include "Handler_metrics.h"

class DB_pool {
    std::queue<DBTask> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_ = false;
    std::vector<std::thread> workers_;
    connect_pool conn_pool_;
    work_pool* business_pool_;
    int worker_count_;
    bool stopped_ = false;                 // 新增：防重复 join，shutdown 只执行一次
    Handler_metrics* metrics_ = nullptr;   // 指标埋点接口，可空
public:
    DB_pool(int conns, int workers, work_pool* business_pool,
            const std::string& host, const std::string& user,
            const std::string& password, const std::string& database,
            unsigned int port = 3306);
    ~DB_pool();

    void submit(DBTask task);
    void shutdown();   // 显式关闭：先关连接池，再停 worker 并 join
    void set_metrics(Handler_metrics* m) { metrics_ = m; }
private:
    void worker_loop();
};
