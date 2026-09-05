#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <utility>

enum class PushResult {
    Ok,
    Full,
    Closed,
};

// 通用有界任务队列：可阻塞 push、非阻塞 try_push、关闭后唤醒所有等待者。
template <typename T>
class bounded_task_queue {
public:
    explicit bounded_task_queue(std::size_t capacity)
        : high_(capacity),
          low_(capacity > 1 ? capacity / 2 : 1) {}

    void set_watermarks(std::size_t high, std::size_t low) {
        std::lock_guard<std::mutex> lock(mutex_);
        high_ = high;
        low_ = std::min(low, high_);
    }

    PushResult try_push(T task) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_)
            return PushResult::Closed;
        if (queue_.size() >= high_) {
            full_count_++;
            return PushResult::Full;
        }
        queue_.push_back(std::move(task));
        not_empty_.notify_one();
        if (queue_.size() > low_ && queue_.size() < high_)
            not_full_.notify_one();   // 有空位即可补一个
        return PushResult::Ok;
    }

    void push(T task) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.size() >= high_)
            full_count_++;
        not_full_.wait(lock, [&] { return closed_ || queue_.size() < high_; });
        if (closed_)
            return;
        queue_.push_back(std::move(task));
        not_empty_.notify_one();
        if (queue_.size() <= low_)
            not_full_.notify_all();   // 排到低水位，批量唤醒等待者
    }

    bool pop(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty())
            return false;
        out = std::move(queue_.front());
        queue_.pop_front();
        if (queue_.size() <= low_)
            not_full_.notify_all();   // 低水位：一次让一批生产者恢复
        else if (queue_.size() < high_)
            not_full_.notify_one();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    std::size_t high_water() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return high_;
    }

    std::size_t low_water() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return low_;
    }

    uint64_t full_count() const { return full_count_.load(); }

private:
    std::size_t high_;
    std::size_t low_;
    std::deque<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    bool closed_ = false;
    std::atomic<uint64_t> full_count_{0};
};
