#pragma once

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "Internalconnection.h"

// 消息类型：fast 不查 DB，db 需要 DB 准入额度。
enum class WorkClass {
    Fast,
    Db,
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

    WorkClass classify(const std::string& msg) const {
        for (const auto& prefix : db_)
            if (msg.rfind(prefix, 0) == 0)
                return WorkClass::Db;
        for (const auto& prefix : fast_)
            if (msg.rfind(prefix, 0) == 0)
                return WorkClass::Fast;
        return WorkClass::Fast;
    }

private:
    std::vector<std::string> fast_;
    std::vector<std::string> db_;
};

// DB 准入额度：背压第一道闸，后续接入 pending 等待队列。
class DbCreditGate {
public:
    explicit DbCreditGate(size_t limit)
        : limit_(limit), available_(limit) {}

    bool try_acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (available_ == 0)
            return false;
        available_--;
        return true;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (available_ < limit_)
                available_++;
        }
        for (auto& cb : listeners_)
            cb();
    }

    void subscribe(std::function<void()> cb) {
        listeners_.push_back(std::move(cb));
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
    mutable std::mutex mutex_;
    std::vector<std::function<void()>> listeners_;
};

// DB 等待队列：额度不足时先在这里停放完整消息，等额度释放后补投。
class DbWaitingAdmission {
public:
    using Conn = std::shared_ptr<Internalconnection>;
    using Dispatch = std::function<void(Conn, const std::string&)>;

    DbWaitingAdmission(DbCreditGate* gate, Dispatch dispatch)
        : gate_(gate), dispatch_(std::move(dispatch)) {
        gate_->subscribe([this] { drain(); });
    }

    void add(Conn conn, const std::string& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back({std::move(conn), msg});
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_.size();
    }

private:
    struct Pending {
        Conn conn;
        std::string msg;
    };

    void drain() {
        while (true) {
            Pending p;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (pending_.empty())
                    return;
                p = std::move(pending_.front());
                pending_.pop_front();
            }

            if (!gate_->try_acquire()) {
                std::lock_guard<std::mutex> lock(mutex_);
                pending_.push_front(std::move(p));
                return;
            }

            dispatch_(p.conn, p.msg);
        }
    }

    DbCreditGate* gate_;
    Dispatch dispatch_;
    mutable std::mutex mutex_;
    std::deque<Pending> pending_;
};
