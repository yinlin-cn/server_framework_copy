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

namespace {

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void close_fd(int& fd) {
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}

}  // namespace

Acceptor::Acceptor(int port, vector<shared_ptr<Reactor>> reactors,
                   Handler_epoll_Factory* factory, int backlog)
    : port_(port), backlog_(backlog), factory_(factory),
      reactors_(std::move(reactors)), events_(2) {}

Acceptor::~Acceptor() {
    stop();
}

bool Acceptor::start() {
    if (running_ || listen_fd_ >= 0 || reactors_.empty())
        return false;

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        perror("socket");
        return false;
    }

    int reuse = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close_fd(listen_fd_);
        return false;
    }
    if (listen(listen_fd_, backlog_) < 0) {
        perror("listen");
        close_fd(listen_fd_);
        return false;
    }
    if (set_nonblocking(listen_fd_) < 0) {
        perror("fcntl listen_fd");
        close_fd(listen_fd_);
        return false;
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        perror("epoll_create1");
        close_fd(listen_fd_);
        return false;
    }

    wake_fd_ = eventfd(0, EFD_NONBLOCK);
    if (wake_fd_ < 0) {
        perror("eventfd");
        close_fd(epoll_fd_);
        close_fd(listen_fd_);
        return false;
    }

    epoll_event listen_ev{};
    listen_ev.events = EPOLLIN;
    listen_ev.data.u64 = listen_event_id;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &listen_ev) < 0) {
        perror("epoll_ctl listen");
        close_fd(wake_fd_);
        close_fd(epoll_fd_);
        close_fd(listen_fd_);
        return false;
    }

    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.u64 = wake_event_id;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_ev) < 0) {
        perror("epoll_ctl wake");
        close_fd(wake_fd_);
        close_fd(epoll_fd_);
        close_fd(listen_fd_);
        return false;
    }

    running_ = true;
    thread_ = thread(&Acceptor::event_loop, this);
    return true;
}

void Acceptor::handle_accept() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            if (errno == EMFILE || errno == ENFILE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            perror("accept");
            return;
        }
        if (set_nonblocking(client_fd) < 0) {
            close(client_fd);
            continue;
        }

        auto conn = make_shared<Internalconnection>(client_fd);
        conn->handler = factory_->create_handler();

        // 轮询分配：新连接均匀打散到各个 Reactor
        auto& reactor = reactors_[next_.fetch_add(1) % reactors_.size()];
        reactor->add_connection(conn);
    }
}

void Acceptor::event_loop() {
    while (running_) {
        int n = epoll_wait(epoll_fd_, events_.data(), events_.size(), -1);
        if (n < 0) {
            if (errno == EINTR || !running_)
                continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events_[i].data.u64 == wake_event_id) {
                uint64_t one = 0;
                read(wake_fd_, &one, sizeof(one));
                continue;
            }
            if (events_[i].data.u64 == listen_event_id) {
                handle_accept();
                continue;
            }
        }
    }
}

void Acceptor::wakeup() {
    if (wake_fd_ >= 0) {
        uint64_t one = 1;
        write(wake_fd_, &one, sizeof(one));
    }
}

void Acceptor::stop() {
    running_ = false;
    wakeup();
    if (thread_.joinable()) thread_.join();
    close_fd(wake_fd_);
    close_fd(epoll_fd_);
    close_fd(listen_fd_);
}
