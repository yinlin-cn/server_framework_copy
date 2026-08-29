#pragma once
#include <functional>
#include<queue>
#include<string>
#include<mutex>
#include<memory>
using namespace std;
class Handler_epoll;
class Reactor;
struct Internalconnection : enable_shared_from_this<Internalconnection> {
    int sock;
    std::string read_buffer;                       // 半包暂存
    bool connected;
    Handler_epoll* handler;                         // 本连接专属
    queue<string> send_queue;                 // 发送缓冲
    mutex send_mutex;
    function<void(const string&)> send_function;   // 业务发送入口
    Reactor* owner_reactor = nullptr;              // 这个连接归哪个 Reactor 管

    Internalconnection(int fd)
        : sock(fd), connected(true), handler(nullptr) {}
};
