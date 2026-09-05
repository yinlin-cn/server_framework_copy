#pragma once

#include <mutex>

// 每连接窗口状态机：in_flight、是否暂停、是否要恢复全部在同一把锁内完成。
class ConnectionFlow {
public:
    explicit ConnectionFlow(int window = 8) : window_(window) {}

    // Reactor 准备取一条消息；成功时窗口位已占用。
    bool try_take() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (resume_pending_ || full_ || in_flight_ >= window_)
            return false;
        in_flight_++;
        if (in_flight_ >= window_)
            full_ = true;
        return true;
    }

    // 请求完成一条；若需要通知 Reactor 恢复读取，返回 true。
    bool finish_one() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (in_flight_ > 0)
            in_flight_--;
        if (full_ && in_flight_ < window_ && !resume_pending_) {
            full_ = false;
            resume_pending_ = true;   // 通知一次即可
            return true;
        }
        return false;
    }

    // Reactor 处理恢复请求时消费一次。
    bool consume_resume() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!resume_pending_)
            return false;
        resume_pending_ = false;
        return true;
    }

    // try_dispatch 失败时归还刚占用的窗口位。
    void release_slot() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (in_flight_ > 0)
            in_flight_--;
        if (full_ && in_flight_ < window_) {
            full_ = false;
            resume_pending_ = true;
        }
    }

    int in_flight() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return in_flight_;
    }

private:
    mutable std::mutex mutex_;
    int window_;
    int in_flight_ = 0;
    bool full_ = false;
    bool resume_pending_ = false;
};
