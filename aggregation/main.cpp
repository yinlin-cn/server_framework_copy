#include <chrono>
#include <csignal>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "Server.h"
#include "context.h"

using namespace std;

static vector<string> split(const string& s, char sep) {
    vector<string> parts;
    size_t pos = 0;
    while (true) {
        size_t next = s.find(sep, pos);
        if (next == string::npos) {
            parts.push_back(s.substr(pos));
            break;
        }
        parts.push_back(s.substr(pos, next - pos));
        pos = next + 1;
    }
    return parts;
}

// 业务类：唯一入口是 flow()，通过 context.h 的接口访问框架。
class BusinessLogic {
public:
    // 压测用：推送型业务，消息格式 broadcast:N，连续向当前连接发 N 条消息。
    void broadcast(int id, const string& msg) {
        size_t pos = msg.find(':');
        int n = 5;
        if (pos != string::npos)
            n = std::stoi(msg.substr(pos + 1));
        for (int i = 0; i < n; i++)
            send("msg:" + std::to_string(i));
    }

    // 压测用：模拟重业务，消息格式 heavy:N，空转 N 毫秒占住业务线程。
    void heavy(int id, const string& msg) {
        size_t pos = msg.find(':');
        int ms = 5;
        if (pos != string::npos)
            ms = std::stoi(msg.substr(pos + 1));
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        send("done:" + msg);
    }

    void echo(int id, const string& msg) {
            send("echo:" + msg);
        }
    EventTask flow(int id, const string msg) {
        cout << "[" << id << "] 前段：准备查询 " << msg << endl;

        // 临时集成测试命令：验证业务层能否触发框架调用
        if (msg.rfind("fwbind:", 0) == 0) {
            auto p = split(msg, ':');
            bool ok = p.size() >= 3
                && framework_call("bind", {p[1], p[2]});
            send(ok ? "fwbind-ok" : "fwbind-fail");
            co_return;
        }
        if (msg.rfind("fwsend:", 0) == 0) {
            auto p = split(msg, ':');
            bool ok = p.size() >= 3
                && framework_call("send_to_sb", {p[1], "fw:to:" + p[2]});
            send(ok ? "fwsend-ok" : "fwsend-fail");
            co_return;
        }
        if (msg.rfind("fwgroup:", 0) == 0) {
            auto p = split(msg, ':');
            bool ok = p.size() >= 3
                && framework_call("send_to_gp", {p[1], "fw:group:" + p[2]});
            send(ok ? "fwgroup-ok" : "fwgroup-fail");
            co_return;
        }
        if (msg.rfind("fwdivide:", 0) == 0) {
            auto p = split(msg, ':');
            bool ok = p.size() >= 3
                && framework_call("divide_gp", {p[1], p[2]});
            send(ok ? "fwdivide-ok" : "fwdivide-fail");
            co_return;
        }
        if (msg.rfind("fwclose:", 0) == 0) {
            auto p = split(msg, ':');
            bool ok = p.size() >= 2
                && framework_call("close_conn", {p[1], "framework-test"});
            send(ok ? "fwclose-ok" : "fwclose-fail");
            co_return;
        }

        // 参数化查询：SQL 模板 + 参数分离，杜绝注入
        string sql = "SELECT name FROM users WHERE name = ? LIMIT 1";
        DBResult res = co_await query_db(sql, { msg });

        if (res.cancelled) {
            send("查询被取消，请稍后重试");
            co_return;
        }
        if (!res.ok) {
            send("查询失败: " + res.err);
            co_return;
        }

        // 业务层自己做限制：从完整结果里取第一行第一列
        std::string name = res.rows.empty() || res.rows[0].empty()
            ? "" : res.rows[0][0];
        cout << "[" << id << "] 后段：拿到 " << name << endl;
        send("reply:" + msg + "|" + name);
    }
};

static volatile sig_atomic_t g_exit_flag = 0;

static void handle_signal(int) {
    g_exit_flag = 1;   // 信号处理器只置标志，不在信号上下文里做复杂操作
}

int main() {
    // 1. 业务对象
    auto biz = std::make_shared<BusinessLogic>();

    // 2. 集成类：divide_work 走构造函数（消息 -> 业务任务）
    Server server(
        [biz](const string& msg) -> function<void()> {
        if (msg.rfind("broadcast", 0) == 0) {
            return [biz, msg]() { biz->broadcast(1, msg); };
        }
        if (msg.rfind("heavy", 0) == 0) {
            return [biz, msg]() { biz->heavy(1, msg); };
        }
        if (msg == "ping") {
            return [biz, msg]() { biz->echo(1, msg); };
        }
        return [biz, msg]() { biz->flow(1, msg); };
        },
        9001,     // 监听端口
        16,       // 解析线程数
        20);       // 业务线程数

    // 3. 数据库配置（可选）
    server.set_db_config(Server::DBConfig{
        50,                       // 连接池大小
        50,                       // DB worker 线程数
        "127.0.0.1",             // 主机
        "delivery",              // 用户名
        "delivery123",           // 密码
        "delivery",              // 数据库
        3306,                    // 端口
    });

    // 4. 错误回调（可选）：业务层决定怎么记录/响应
    server.set_error_handler(
        [](std::shared_ptr<Internalconnection> conn,
           const string& stage, const string& err) {
            // 这里只打日志；真实业务可回客户端、落库、上报监控
            std::cerr << "[" << stage << "] error: " << err << std::endl;
            if (conn) {
                conn->send_function("ERROR: " + err);   // 可选：回客户端
            }
        });

    // 5. 启动
    if (!server.start()) {
        std::cout << "服务器启动失败" << std::endl;
        return 1;
    }
    std::cout << "服务器启动，端口 9001" << std::endl;

    // 6. 注册信号，优雅退出
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    while (!g_exit_flag) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "收到退出信号，开始停止..." << std::endl;
    server.stop();
    std::cout << "已停止" << std::endl;
    return 0;
}
