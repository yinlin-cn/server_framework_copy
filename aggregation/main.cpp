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
#include "Handler_epoll_make.h"
#include "Handler_divide_make.h"
#include "Handler_DB.h"
#include "Box.h"
#include "EventAwaiter.h"
#include "EventTask.h"
#include "thread_context.h"

using namespace std;
using Work = std::function<void()>;

class BusinessLogic {
public:
    EventTask flow(int id, const string& msg) {
        cout << "[" << id << "] 前段：准备查询" << endl;
        string res = co_await query_db("select:" + msg);
        cout << "[" << id << "] 后段：拿到 " << res << endl;
        send("reply:" + msg + "|" + res);
    }
};

class FakeDBHandler : public Handler_DB {
    work_pool* pool_;
public:
    explicit FakeDBHandler(work_pool* p) : pool_(p) {}

    void submit(uint64_t key, std::shared_ptr<Box> box,
                const std::string& message) override {
        box->result = "db(" + message + ")";
        box->ready = true;
        pool_->add_task([pool = pool_, key] {
            pool->on_event(key);
        });
    }
};

int main() {
    auto business_pool = std::make_shared<work_pool>();
    g_work_pool = business_pool.get();

    auto divide_handler = std::make_shared<Handler_divide_make>(business_pool);

    FakeDBHandler db_handler(business_pool.get());
    g_db_handler = &db_handler;

    auto biz = std::make_shared<BusinessLogic>();

    // 外界传入的解析函数：message → 业务任务
    auto divide_work = [biz](const std::string& msg) -> Work {
        return [biz, msg]() {
            biz->flow(1, msg);
        };
    };

    auto parse_pool = std::make_shared<divide_pool>(4);
    Handler_epoll_Factory_make factory(divide_work, parse_pool, divide_handler);

    epoll_make server(9001);
    server.start(&factory);
    std::cout << "服务器启动，端口 9001" << std::endl;

    for (;;)
        std::this_thread::sleep_for(std::chrono::hours(1));
}