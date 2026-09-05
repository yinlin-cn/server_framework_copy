#pragma once
#include <functional>
#include <memory>
#include "Handler_epoll.h"
#include "divide_task.h"
#include "divide_pool.h"
#include "Handler_divide.h"
#include "connect_book.h"

using Work = std::function<void()>;

class RouteClassifier;
class DbCreditGate;
class DbWaitingAdmission;
class IReactorControl;

class Handler_epoll_make : public Handler_epoll {
private:
    std::function<Work(const std::string&)> divide_work;   // 解析函数：message → 业务任务
    std::shared_ptr<divide_pool> divide_pool_;             // 解析任务推进哪个池
    std::shared_ptr<Handler_divide> divide_handler_;       // 业务分发器
    std::shared_ptr<connect_book> book_;                   // 连接名册生命周期回调
    RouteClassifier* route_ = nullptr;                     // 协议层路由分类
    DbCreditGate* db_gate_ = nullptr;                      // DB 准入额度
    DbWaitingAdmission* waiting_ = nullptr;                // DB 等待队列
    IReactorControl* reactor_control_ = nullptr;           // Reactor 控制接口

public:
    Handler_epoll_make(std::function<Work(const std::string&)> a,
                       std::shared_ptr<divide_pool> p,
                       std::shared_ptr<Handler_divide> c,
                       std::shared_ptr<connect_book> book,
                       RouteClassifier* route = nullptr,
                       DbCreditGate* db_gate = nullptr,
                       DbWaitingAdmission* waiting = nullptr,
                       IReactorControl* reactor_control = nullptr)
        : divide_work(a), divide_pool_(p), divide_handler_(c),
          book_(std::move(book)), route_(route), db_gate_(db_gate),
          waiting_(waiting), reactor_control_(reactor_control) {}

    PushResult on_message(std::shared_ptr<Internalconnection> conn,
                          const std::string& msg) override;
    void on_connect(std::shared_ptr<Internalconnection> conn) override;
    void on_disconnect(std::shared_ptr<Internalconnection> conn) override;
};

class Handler_epoll_Factory_make : public Handler_epoll_Factory {
private:
    std::function<Work(const std::string&)> divide_work;
    std::shared_ptr<divide_pool> divide_pool_;
    std::shared_ptr<Handler_divide> divide_handler_;
    std::shared_ptr<connect_book> book_;
    RouteClassifier* route_ = nullptr;
    DbCreditGate* db_gate_ = nullptr;
    DbWaitingAdmission* waiting_ = nullptr;
    IReactorControl* reactor_control_ = nullptr;

public:
    Handler_epoll_Factory_make(std::function<Work(const std::string&)> a,
                               std::shared_ptr<divide_pool> p,
                               std::shared_ptr<Handler_divide> c,
                               std::shared_ptr<connect_book> book,
                               RouteClassifier* route = nullptr,
                               DbCreditGate* db_gate = nullptr,
                               DbWaitingAdmission* waiting = nullptr,
                               IReactorControl* reactor_control = nullptr)
        : divide_work(a), divide_pool_(p), divide_handler_(c),
          book_(std::move(book)), route_(route), db_gate_(db_gate),
          waiting_(waiting), reactor_control_(reactor_control) {}

    Handler_epoll* create_handler() override;
};
