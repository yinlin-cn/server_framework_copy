#pragma once
#include <iostream>
#include <thread>
#include <string>
#include <memory>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include "divide_task.h"
#include "ErrorHandler.h"
#include "Handler_metrics.h"
#include"Handler_divide.h"
using namespace std;
class divide_pool {
private:
    queue<divide_task> tasks;
    mutex for_task;
    bool stop = false;
    vector<thread> pool;
    condition_variable cv;
    ErrorHandler error_handler_;   // 业务层可选注入，默认空
    Handler_metrics* metrics_ = nullptr;   // 指标埋点接口，可空

    std::atomic<int> active_{0};              // 在途任务计数
    std::mutex idle_mutex_;
    std::condition_variable idle_cv_;
public:
    divide_pool(int N=8);
    ~divide_pool();
    void worker();
    void add_task(divide_task funtion);
    void set_error_handler(ErrorHandler h) { error_handler_ = std::move(h); }
    void set_metrics(Handler_metrics* m) { metrics_ = m; }
    void shutdown();                                                      // 置 stop + notify
    bool wait_idle(const std::chrono::milliseconds& timeout);             // 等 active 归零
};
