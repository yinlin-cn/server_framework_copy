#pragma once

#include <atomic>
#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "Internalconnection.h"

// 消息类型：fast 不查 DB，db 需要 DB 准入额度。
enum class work_type {
    Fast,
    DB,
};

class IReactorControl {
public:
    virtual ~IReactorControl() = default;
    virtual void pause_reading(std::shared_ptr<Internalconnection> conn) = 0;
    virtual void schedule_resume(std::shared_ptr<Internalconnection> conn) = 0;
};

// 路由分类：只做轻量判断，不承担协议解析。
class RouteClassifier {
public:
    void add_fast(const std::string& prefix) {
        fast_.push_back(prefix);
    }

    void add_db(const std::string& prefix) {
        db_.push_back(prefix);
    }

    work_type classify(const std::string& msg) const {
        for (const auto& prefix : db_)
            if (msg.rfind(prefix, 0) == 0)
                return work_type::DB;
        for (const auto& prefix : fast_)
            if (msg.rfind(prefix, 0) == 0)
                return work_type::Fast;
        return work_type::Fast;
    }

private:
    std::vector<std::string> fast_;
    std::vector<std::string> db_;
};

// DB 准入额度：背压第一道闸，后续接入 pending 等待队列。
class DB_credit_gate {
public:
    explicit DB_credit_gate(size_t limit)
        : limit_(limit), available_(limit) {}

    bool try_acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_ || available_ == 0)
            return false;
        available_--;
        return true;
    }

    bool acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] {
            return closed_ || available_ > 0;
        });
        if (closed_)
            return false;
        available_--;
        return true;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!closed_ && available_ < limit_)
                available_++;
        }
        cv_.notify_one();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        cv_.notify_all();
    }

    size_t available() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return available_;
    }

    size_t limit() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return limit_;
    }

private:
    size_t limit_;
    size_t available_;
    bool closed_ = false;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

// 一个 DB 业务流程只占一个准入额度；令牌随任务挂起/恢复传递，
// 整个业务流程真正结束时才释放（防止一次业务多次查库导致额度提前归还）。
class DB_credit_token {
public:
    explicit DB_credit_token(DB_credit_gate* db_credit_gate)
        : db_credit_gate_(db_credit_gate) {}

    DB_credit_token(const DB_credit_token&) = delete;
    DB_credit_token& operator=(const DB_credit_token&) = delete;

    void release() {
        bool expected = false;
        if (released_.compare_exchange_strong(expected, true))
            if (db_credit_gate_) db_credit_gate_->release();
    }

private:
    DB_credit_gate* db_credit_gate_;
    std::atomic<bool> released_{false};
};

// DB 等待队列：额度不足时先在这里停放完整消息，等额度释放后补投。
class DB_waiting_queue {
public:
    using Conn = std::shared_ptr<Internalconnection>;
    using Dispatch = std::function<bool(
        Conn, const std::string&, std::shared_ptr<DB_credit_token>)>;

    DB_waiting_queue(DB_credit_gate* db_credit_gate, Dispatch dispatch)
        : db_credit_gate_(db_credit_gate), dispatch_(std::move(dispatch)),
          drain_thread_(&DB_waiting_queue::dispatch_waiting_tasks, this) {}

    ~DB_waiting_queue() {
        shutdown();
    }

    bool add_waiting_task(Conn conn, const std::string& msg) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_)
                return false;
            waiting_tasks_.push_back({std::move(conn), msg});
        }
        cv_.notify_one();
        return true;
    }

    size_t waiting_task_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return waiting_tasks_.size();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_)
                return;
            stopped_ = true;
        }
        if (db_credit_gate_)
            db_credit_gate_->shutdown();   // 唤醒可能卡在 acquire() 的 drain 线程
        cv_.notify_all();
        if (drain_thread_.joinable())
            drain_thread_.join();
    }

private:
    struct waiting_task {
        Conn conn;
        std::string msg;
    };

    void dispatch_waiting_tasks() {
        while (true) {
            waiting_task p;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] {
                    return stopped_ || !waiting_tasks_.empty();
                });
                if (stopped_)
                    return;
                p = std::move(waiting_tasks_.front());
                waiting_tasks_.pop_front();
            }

            if (!db_credit_gate_->acquire()) {
                std::lock_guard<std::mutex> lock(mutex_);
                waiting_tasks_.push_front(std::move(p));
                return;
            }

            auto token = std::make_shared<DB_credit_token>(db_credit_gate_);
            bool dispatched = false;
            try {
                dispatched = dispatch_(p.conn, p.msg, token);
            } catch (...) {
                dispatched = false;
            }
            if (!dispatched)
                token->release();
        }
    }

    DB_credit_gate* db_credit_gate_;
    Dispatch dispatch_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped_ = false;
    std::deque<waiting_task> waiting_tasks_;
    std::thread drain_thread_;
};
