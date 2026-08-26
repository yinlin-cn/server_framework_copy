#pragma once
#include <functional>
#include <memory>
#include "Handler_epoll.h"
#include "divide_task.h"
#include "divide_pool.h"
#include "Handler_divide.h"

using Work = std::function<void()>;

class Handler_epoll_make : public Handler_epoll {
private:
    std::function<Work(const std::string&)> divide_work;   // 解析函数：message → 业务任务
    std::shared_ptr<divide_pool> divide_pool_;             // 解析任务推进哪个池
    std::shared_ptr<Handler_divide> divide_handler_;       // 业务分发器

public:
    Handler_epoll_make(std::function<Work(const std::string&)> a,
                       std::shared_ptr<divide_pool> p,
                       std::shared_ptr<Handler_divide> c)
        : divide_work(a), divide_pool_(p), divide_handler_(c) {}

    void on_message(std::shared_ptr<Internalconnection> conn,
                    const std::string& msg) override;
};

class Handler_epoll_Factory_make : public Handler_epoll_Factory {
private:
    std::function<Work(const std::string&)> divide_work;
    std::shared_ptr<divide_pool> divide_pool_;
    std::shared_ptr<Handler_divide> divide_handler_;

public:
    Handler_epoll_Factory_make(std::function<Work(const std::string&)> a,
                               std::shared_ptr<divide_pool> p,
                               std::shared_ptr<Handler_divide> c)
        : divide_work(a), divide_pool_(p), divide_handler_(c) {}

    Handler_epoll* create_handler() override;
};