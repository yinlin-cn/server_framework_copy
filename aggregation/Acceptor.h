#pragma once
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <cstdint>
#include "Reactor.h"
#include "Handler_epoll.h"
using namespace std;

// Acceptor：主线程通过 epoll 等待 listen_fd，把新连接轮询分配给某个 Reactor。
class Acceptor {
public:
    Acceptor(int port, vector<shared_ptr<Reactor>> reactors,
             Handler_epoll_Factory* factory, int backlog = 4096);
    ~Acceptor();

    bool start();
    void stop();

private:
    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    int port_;
    int backlog_;
    Handler_epoll_Factory* factory_;
    vector<shared_ptr<Reactor>> reactors_;
    vector<epoll_event> events_;
    static constexpr uint64_t listen_event_id = 0;
    static constexpr uint64_t wake_event_id = 1;
    atomic<uint64_t> next_{0};       // 轮询分配游标
    atomic<bool> running_{false};
    thread thread_;

    void event_loop();
    void handle_accept();
    void wakeup();
};
