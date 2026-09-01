#pragma once
#include <memory>
#include <vector>
#include "Reactor.h"
#include "Acceptor.h"

// 网络层集成类：把 Acceptor 与 N 个 Reactor 组装在一起，
// 对外只暴露启动/停止，Server 不需要关心网络层内部结构。
class NetworkServer {
public:
    NetworkServer(int port, int reactor_count, int max_events, int backlog,
                  Handler_epoll_Factory* factory);
    ~NetworkServer();

    bool start();          // 创建并启动全部 Reactor + Acceptor
    void stop();           // 停 Acceptor 与全部 Reactor，并断开连接
    void stop_accept();    // 只停 Acceptor（优雅退出第一步：关闸）
    void set_batch_handler(Handler_batch* handler);   // 注入批处理接口给所有 Reactor
    void set_metrics(Handler_metrics* m) { metrics_ = m; }

private:
    int port_;
    int reactor_count_;
    int max_events_;
    int backlog_;
    Handler_epoll_Factory* factory_;
    Handler_batch* batch_handler_ = nullptr;
    Handler_metrics* metrics_ = nullptr;
    std::vector<std::shared_ptr<Reactor>> reactors_;
    std::unique_ptr<Acceptor> acceptor_;
    bool started_ = false;
};
