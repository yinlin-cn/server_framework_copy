#pragma once
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include "Reactor.h"
#include "Handler_epoll.h"
using namespace std;

// Acceptor：主线程只负责 listen + accept，把新连接轮询分配给某个 Reactor。
class Acceptor {
public:
    Acceptor(int port, vector<shared_ptr<Reactor>> reactors,
             Handler_epoll_Factory* factory, int backlog = 4096);
    ~Acceptor();

    void start();
    void stop();

private:
    int listen_fd_;
    int port_;
    int backlog_;
    Handler_epoll_Factory* factory_;
    vector<shared_ptr<Reactor>> reactors_;
    atomic<uint64_t> next_{0};       // 轮询分配游标
    atomic<bool> running_{false};
    thread thread_;
    void run();
};
