#pragma once
#include <functional>
#include <deque>
#include<string>
#include<mutex>
#include<memory>
#include<atomic>
#include<cstdint>
#include "ConnectionFlow.h"
using namespace std;
class Handler_epoll;
class Reactor;
struct Internalconnection : enable_shared_from_this<Internalconnection> {
    int sock;
    std::string read_buffer;                       // 半包暂存
    std::atomic<bool> connected;
    uint64_t reactor_conn_id = 0;                      // Reactor 内部事件身份，避免裸指针悬垂
    Handler_epoll* handler;                         // 本连接专属
    deque<string> send_queue;                 // 发送缓冲（批发送后剩余部分可插回队首）
    size_t queued_send_bytes = 0;              // 发送队列中的总字节数
    mutex send_mutex;
    function<bool(const string&)> send_function;   // 业务发送入口，返回是否真正入队
    Reactor* owner_reactor = nullptr;              // 这个连接归哪个 Reactor 管
    std::atomic<uint64_t> last_active_us{0};       // 最后活跃时间，心跳超时检测用
    std::atomic<bool> reading_paused{false};       // 背压：暂停 EPOLLIN 读取
    ConnectionFlow flow;                           // 每连接窗口状态机

    Internalconnection(int fd)
        : sock(fd), connected(true), handler(nullptr) {}
};
