#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>

class Reactor;

// 批处理模块：攒一批"有待发数据的 Reactor"，定时统一唤醒一次。
// 它不依赖业务层和连接对象，只认 Reactor 指针。
class BatchSender {
public:
    explicit BatchSender(int flush_ms = 2);
    ~BatchSender();

    void start();              // 启动 flush 线程
    void flush_and_stop();     // 唤醒剩余待发 Reactor，再停线程并 join
    void mark_reactor(Reactor* reactor);

private:
    int flush_ms_;
    std::set<Reactor*> pending_reactors_;   // 待唤醒的 Reactor（set 去重）
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::thread thread_;

    void flush_loop();
};
