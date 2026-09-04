#pragma once
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cerrno>
#include <thread>
#include <mutex>
#include <vector>
#include <memory>
#include <atomic>
#include <string>
#include <algorithm>
#include <unordered_map>
#include "Internalconnection.h"
#include "Handler_epoll.h"
#include "Handler_batch.h"
#include "Handler_metrics.h"
#include "Handler_log.h"
using namespace std;

// 一个 Reactor = 一个 epoll + 一个 eventfd + 一个事件循环线程。
// 它只负责自己那一组连接的读/写/关闭，内部单线程处理。
class Reactor {
public:
    Reactor(int max_events, Handler_epoll_Factory* factory,
            uint64_t idle_timeout_us = 60000000);   // 默认 60 秒空闲超时
    ~Reactor();

    void start();                          // 创建 epoll/eventfd 并启动事件线程
    void stop();                           // 停止事件线程并清理连接
    void add_connection(shared_ptr<Internalconnection> conn);   // acceptor 调用，登记连接
    void wakeup();                         // 跨线程唤醒（业务线程 send 后调用）
    void request_close(shared_ptr<Internalconnection> conn,
                       const std::string& reason = "");   // 跨线程请求关闭，事件线程统一处理
    void set_batch_handler(Handler_batch* handler);   // 注入批处理接线接口
    void set_metrics(Handler_metrics* m) { metrics_ = m; }
    void set_log(Handler_log* l) { log_ = l; }

private:
    int epoll_fd_;
    int wake_fd_;
    int max_events_;
    uint64_t idle_timeout_us_;
    Handler_epoll_Factory* factory_;
    atomic<bool> running_{false};
    thread event_thread_;
    vector<epoll_event> events_;
    unordered_map<int, shared_ptr<Internalconnection>> connections_;   // key = fd，本 reactor 连接组
    mutex conn_mutex_;                                     // 连接表保护
    vector<weak_ptr<Internalconnection>> pending_send_;    // 待发送桶
    mutex pending_mutex_;
    vector<weak_ptr<Internalconnection>> pending_close_;   // 待关闭桶
    mutex pending_close_mutex_;
    Handler_batch* batch_handler_ = nullptr;               // 批处理接口，可空
    Handler_metrics* metrics_ = nullptr;                   // 指标埋点接口，可空
    Handler_log* log_ = nullptr;                           // 日志接口，可空

    int set_nonblocking(int fd);
    void mod_event(shared_ptr<Internalconnection> conn, uint32_t evs);
    void del_event(shared_ptr<Internalconnection> conn);
    void handle_read(shared_ptr<Internalconnection> conn);
    bool enqueue_send(shared_ptr<Internalconnection> conn, const string& msg);
    void try_send(shared_ptr<Internalconnection> conn);
    void process_pending_close();
    void close_client(shared_ptr<Internalconnection> conn);
    string send_preview(const string& msg);
    vector<string> spilit_message(string& message);
    void event_loop();
};
