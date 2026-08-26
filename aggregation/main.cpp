#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "context.h"
#include "epoll.h"
#include "divide_pool.h"
#include "divide_task.h"
#include "work_pool.h"
#include "work_task.h"
#include "Handler_epoll_make.h"
#include "Handler_divide_make.h"
#include "Handler_DB_make.h"
#include "DB_pool.h"
#include "connect_pool.h"
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

        // 真 SQL：从 users 表查 name 等于消息的那一行
        string sql = "SELECT name FROM users WHERE name='" + msg + "' LIMIT 1";
        string res = co_await query_db(sql);

        cout << "[" << id << "] 后段：拿到 " << res << endl;
        send("reply:" + msg + "|" + res);
    }
};

int main() {
    auto business_pool = std::make_shared<work_pool>();
    g_work_pool = business_pool.get();

    // 解析层 → 业务层
    auto divide_handler = std::make_shared<Handler_divide_make>(business_pool);

    // 业务层 → 数据库
    auto db_pool = std::make_shared<DB_pool>(4, 4, business_pool.get(),"127.0.0.1", "delivery", "delivery123", "delivery", 3306);
    auto db_handler = std::make_shared<Handler_DB_make>(db_pool);
    g_db_handler = db_handler.get();

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