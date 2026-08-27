#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "Server.h"
#include "context.h"

using namespace std;

class BusinessLogic {
public:
    EventTask flow(int id, const string& msg) {
        cout << "[" << id << "] 前段：准备查询 " << msg << endl;

        string sql = "SELECT name FROM users WHERE name='" + msg + "' LIMIT 1";
        string res = co_await query_db(sql);

        cout << "[" << id << "] 后段：拿到 " << res << endl;
        send("reply:" + msg + "|" + res);
    }
};

int main() {
    auto biz = std::make_shared<BusinessLogic>();

    // 集成类：divide_work 走构造函数，消息 -> 业务任务。
    Server server(
        [biz](const string& msg) -> function<void()> {
            return [biz, msg]() { biz->flow(1, msg); };
        },
        9001, 4, 8);

    server.set_db_config(Server::DBConfig{
        4, 4, "127.0.0.1", "delivery", "delivery123", "delivery", 3306,
    });

    if (!server.start()) {
        std::cout << "服务器启动失败" << std::endl;
        return 1;
    }
    std::cout << "服务器启动，端口 9001" << std::endl;

    for (;;) std::this_thread::sleep_for(std::chrono::hours(1));
}
