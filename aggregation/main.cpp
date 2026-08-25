// main.cpp
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <functional>

#include "context.h"
#include "epoll.h"
#include "divide_pool.h"
#include "divide_task.h"
#include "work_pool.h"
#include "work_task.h"
#include "Handler_epoll.h"
#include "Handler_divide.h"
#include "Handler_DB.h"
#include "Box.h"
#include "EventAwaiter.h"
#include "EventTask.h"
#include "thread_context.h"

using namespace std;

// ===== 业务逻辑：协程查询，DB 由 FakeDBHandler 模拟 =====
class BusinessLogic {
public:
    EventTask flow(int id, const string& msg) {
        cout << "[" << id << "] 前段：准备查询" << endl;
        string res = co_await query_db("select:" + msg);
        cout << "[" << id << "] 后段：拿到 " << res << endl;
        send("reply:" + msg + "|" + res);
    }
};

// ===== 业务分发器：把解析出的业务任务推入 work_pool =====
class WorkPoolHandler : public Handler_divide {
    work_pool* pool_;
public:
    explicit WorkPoolHandler(work_pool* p) : pool_(p) {}

    void on_work(std::shared_ptr<Internalconnection> conn,
                 std::function<void()> work) override {
        pool_->add_task(work_task{work, conn});
    }
};

// ===== 假数据库：填 box，再通过 on_event 任务唤醒 =====
class FakeDBHandler : public Handler_DB {
    work_pool* pool_;
public:
    explicit FakeDBHandler(work_pool* p) : pool_(p) {}

    void submit(uint64_t key, std::shared_ptr<Box> box,
                const std::string& message) override {
        // 模拟真实 DB：换个线程做，让协程先完成挂起
        std::thread([pool = pool_, key, box, message] {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            box->result = "db(" + message + ")";
            box->ready = true;
            pool->add_task([pool, key] {
                pool->on_event(key);
            });
        }).detach();
    }
};

// ===== 解析层：epoll 收到消息 → 封装 divide_task =====
class ParseHandler : public Handler_epoll {
    divide_pool* divide_pool_;
    Handler_divide* divide_handler_;
    std::shared_ptr<BusinessLogic> biz_;

public:
    ParseHandler(divide_pool* p, Handler_divide* h,
                 std::shared_ptr<BusinessLogic> b)
        : divide_pool_(p), divide_handler_(h), biz_(b) {}

    void on_message(std::shared_ptr<Internalconnection> conn,
                    const std::string& msg) override {
        auto biz = biz_;
        std::function<std::function<void()>()> parse = [biz, msg]() {
            return [biz, msg]() {
                biz->flow(1, msg);
            };
        };
        divide_pool_->add_task(divide_task{parse, conn, divide_handler_});
    }
};

class ParseHandlerFactory : public Handler_epoll_Factory {
    divide_pool* divide_pool_;
    Handler_divide* divide_handler_;
    std::shared_ptr<BusinessLogic> biz_;

public:
    ParseHandlerFactory(divide_pool* p, Handler_divide* h,
                        std::shared_ptr<BusinessLogic> b)
        : divide_pool_(p), divide_handler_(h), biz_(b) {}

    Handler_epoll* create_handler() override {
        return new ParseHandler(divide_pool_, divide_handler_, biz_);
    }
};

int main() {
    work_pool business_pool;
    g_work_pool = &business_pool;                  // 注册业务池单例

    WorkPoolHandler work_handler(&business_pool);

    FakeDBHandler db_handler(&business_pool);
    g_db_handler = &db_handler;                    // 注册 DB 接口

    auto biz = std::make_shared<BusinessLogic>();

    divide_pool parse_pool(4);
    ParseHandlerFactory factory(&parse_pool, &work_handler, biz);

    epoll_make server(9001);
    server.start(&factory);
    std::cout << "服务器启动，端口 9001" << std::endl;

    while(true);
    return 0;
}