#pragma once
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <cerrno>
#include <thread>
#include <mutex>
#include <vector>
#include <memory>
#include <atomic>
#include <functional>
#include <queue>
#include <string>
#include <algorithm>
#include"Handler_epoll.h"
#include"Internalconnection.h"
using namespace std;
class epoll_make {
private:
    int epoll_fd;
    int listen_fd;
    int wake_fd;                    // 修复3：eventfd，用于安全唤醒 epoll_wait
    int port;
    int N;                          // 监听队列长度
    int M;                          // epoll 最大事件数
    atomic<bool> running{false};     // 修复3：跨线程读写必须原子
    Handler_epoll_Factory* factory;
    thread event_thread;
    vector<epoll_event> events;
    vector<shared_ptr<Internalconnection>> connections;
    int set_nonblocking(int fd);
    void mod_event(shared_ptr<Internalconnection> conn, uint32_t evs);
    void del_event(shared_ptr<Internalconnection> conn);
    void handle_accept();
    void handle_read(shared_ptr<Internalconnection> conn);
    bool send(shared_ptr<Internalconnection> conn, const char* data, size_t len);
    void try_send(shared_ptr<Internalconnection> conn);
    void close_client(shared_ptr<Internalconnection> conn);
    string send_preview(string send_message);
    vector<string> spilit_message(string& message);
    void event_loop();
public:
    epoll_make(int a = 9001, int b = 128, int c = 64);
    ~epoll_make();
    void start(Handler_epoll_Factory* f);
};