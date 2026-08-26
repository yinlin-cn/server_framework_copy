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

class DB_pool {
    std::queue<DBTask> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_ = false;
    std::vector<std::thread> workers_;
    connect_pool conn_pool_;
    work_pool* business_pool_;
    int worker_count_;

public:
    DB_pool(int conns, int workers, work_pool* business_pool,
            const std::string& host, const std::string& user,
            const std::string& password, const std::string& database,
            unsigned int port = 3306);
    ~DB_pool();

    void submit(DBTask task);

private:
    void worker_loop();
};