#include "Acceptor.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstdio>
#include <chrono>
#include <thread>
using namespace std;

Acceptor::Acceptor(int port, vector<shared_ptr<Reactor>> reactors,
                   Handler_epoll_Factory* factory, int backlog)
    : port_(port), backlog_(backlog), factory_(factory),
      reactors_(std::move(reactors)), listen_fd_(-1) {}

Acceptor::~Acceptor() {
    stop();
}

void Acceptor::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) { perror("socket"); return; }
    int reuse = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return; }
    if (listen(listen_fd_, backlog_) < 0) { perror("listen"); return; }
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);

    running_ = true;
    thread_ = thread(&Acceptor::run, this);
}

void Acceptor::run() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));   // 没新连接，让出 CPU 再试
                continue;
            }
            break;
        }
        auto conn = make_shared<Internalconnection>(client_fd);
        conn->handler = factory_->create_handler();

        // 轮询分配：新连接均匀打散到各个 Reactor
        auto& reactor = reactors_[next_.fetch_add(1) % reactors_.size()];
        reactor->add_connection(conn);
    }
}

void Acceptor::stop() {
    running_ = false;
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) close(listen_fd_);
}
