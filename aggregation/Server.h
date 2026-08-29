#pragma once
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "ErrorHandler.h"

class work_pool;
class divide_pool;
class Handler_divide_make;
class DB_pool;
class Handler_DB_make;
class Handler_epoll_Factory_make;
class NetworkServer;
class BatchSender;
class Handler_batch_make;

// 集成类：负责把网络层 / 解析层 / 业务层 / 数据库层组装起来。
// 业务代码只需传入 divide_work（消息 -> 业务任务）与可选的数据库配置。
class Server {
public:
    using Work = std::function<void()>;
    // divide_work：收到一条完整消息，返回一个可执行的业务任务（Work）。
    using DivideWork = std::function<Work(const std::string&)>;

    // 数据库配置，字段顺序与 DB_pool 构造参数对齐：
    // (conns, workers, host, user, password, database, port)
    struct DBConfig {
        int connections = 4;          // 连接池大小
        int db_workers = 4;           // DB worker 线程数
        std::string host = "127.0.0.1";
        std::string user;
        std::string password;
        std::string database;
        unsigned int port = 3306;
    };

    Server(DivideWork divide_work,
           int listen_port = 9001,
           int parse_threads = 4,
           int work_threads = 8,
           int reactor_count = 4);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // 配置数据库连接池（可选；不配置则 query_db 不会真正投递任务）。
    void set_db_config(const DBConfig& cfg);

    // 组装并启动各线程池与 epoll 循环（epoll 运行在独立事件线程）。
    // 返回是否启动成功。
    bool start();

    // 停止 epoll 事件循环并回收事件线程。
    void stop();

    void set_error_handler(ErrorHandler handler);   // 业务层错误回调

    void settle_pending();   // 结算挂起协程：标记取消并重新投递
private:
    DivideWork divide_work_;          // 消息 -> 业务任务
    int listen_port_;
    int parse_threads_;
    int work_threads_;
    int reactor_count_;

    bool has_db_ = false;
    DBConfig db_cfg_;

    // 持有各组件所有权，保证生命周期超出事件线程。
    std::shared_ptr<work_pool> work_pool_;
    std::shared_ptr<Handler_divide_make> divide_handler_;
    std::shared_ptr<DB_pool> db_pool_;
    std::shared_ptr<Handler_DB_make> db_handler_;
    std::shared_ptr<divide_pool> parse_pool_;
    std::unique_ptr<Handler_epoll_Factory_make> factory_;
    std::unique_ptr<NetworkServer> network_;
    std::unique_ptr<BatchSender> batch_sender_;
    std::unique_ptr<Handler_batch_make> batch_handler_;
    bool started_ = false;
    ErrorHandler error_handler_;                    // 业务层可选注入
};
