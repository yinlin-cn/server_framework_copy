#include "BatchSender.h"
#include "Reactor.h"

BatchSender::BatchSender(int flush_ms) : flush_ms_(flush_ms) {}

BatchSender::~BatchSender() {
    flush_and_stop();
}

void BatchSender::mark_reactor(Reactor* reactor) {
    bool should_notify = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        should_notify = pending_reactors_.insert(reactor).second;
    }
    if (should_notify)
        cv_.notify_one();
}

void BatchSender::start() {
    running_ = true;
    thread_ = std::thread(&BatchSender::flush_loop, this);
}

void BatchSender::flush_and_stop() {
    running_ = false;
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();

    // 最后一次处理：把剩余待发 Reactor 全部唤醒，保证 closesocket 前发完。
    std::set<Reactor*> remaining;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        remaining.swap(pending_reactors_);
    }
    for (auto* r : remaining)
        if (r) r->wakeup();
}

void BatchSender::flush_loop() {
    while (running_) {
        std::set<Reactor*> batch;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(flush_ms_));
            batch.swap(pending_reactors_);
        }
        for (auto* r : batch)
            if (r) r->wakeup();
    }
}
