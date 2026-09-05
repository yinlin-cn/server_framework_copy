#include <thread>
#include <memory>
#include <vector>
#include <functional>
#include <mutex>
#include<queue>
#include <condition_variable>
#include <atomic>
#include <cstdint>
#include "bounded_task_queue.h"
#include "thread_context.h"
#include"work_task.h"
#include "ErrorHandler.h"
#include "Handler_metrics.h"
#include "Handler_log.h"

using namespace std;
class thread_pool {
private:
    bounded_task_queue<work_task> tasks_;
    vector<thread> pool;
    ErrorHandler error_handler_;   // 业务层可选注入，默认空
    Handler_metrics* metrics_ = nullptr;   // 指标埋点接口，可空
    Handler_log* log_ = nullptr;           // 日志接口，可空

    std::atomic<int> active_{0};              // 在途任务计数
    std::mutex idle_mutex_;
    std::condition_variable idle_cv_;
public:
    thread_pool(int N=8);
    ~thread_pool();
    void worker();
    void add_task(work_task f);
    void set_error_handler(ErrorHandler h) { error_handler_ = std::move(h); }
    void set_metrics(Handler_metrics* m) { metrics_ = m; }
    void set_log(Handler_log* l) { log_ = l; }
    void shutdown();                                                      // 置 stop + notify
    bool wait_idle(const std::chrono::milliseconds& timeout);             // 等 active 归零
    std::size_t queue_size() const { return tasks_.size(); }
    std::size_t queue_high() const { return tasks_.high_water(); }
    std::size_t queue_low() const { return tasks_.low_water(); }
    uint64_t queue_full_count() const { return tasks_.full_count(); }
};
