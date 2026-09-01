#include"epoll.h"

using namespace std;

int epoll_make::set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

void epoll_make::mod_event(shared_ptr<Internalconnection> conn, uint32_t evs) {
        epoll_event ev{};
        ev.events = evs;
        ev.data.ptr = conn.get();
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn->sock, &ev);
    }

void epoll_make::del_event(shared_ptr<Internalconnection> conn) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->sock, nullptr);
    }

void epoll_make::handle_accept() {
        while (true) {   // ET：一次接完所有排队连接
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);
            if (client_fd < 0) {
                if (errno == EINTR) continue;
                break;   // EAGAIN 说明接完了
            }
            set_nonblocking(client_fd);

            auto conn = make_shared<Internalconnection>(client_fd);
            conn->handler = factory->create_handler();   // 每连接一个专属handler
            // 修复1：捕获 weak_ptr，避免 conn 自己持有自己形成循环
            conn->send_function = [this, weak = weak_ptr<Internalconnection>(conn)](const string& msg) -> bool {
                if (auto c = weak.lock())
                    return this->send(c, msg.data(), msg.size());
                return false;
            };

            connections.push_back(conn);

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;               // 只监听可读
            ev.data.ptr = conn.get();
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

            if (conn->handler) conn->handler->on_connect(conn);
        }
    }

void epoll_make::handle_read(shared_ptr<Internalconnection> conn) {
        char buffer[1024];
        while (conn->connected) {   // ET：读到 EAGAIN
            ssize_t count = read(conn->sock, buffer, sizeof(buffer));
            if (count == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 读完了
                close_client(conn);
                return;
            }
            else if (count == 0) {   // 对端关闭
                close_client(conn);
                return;
            }
            else {
                conn->read_buffer.append(buffer, count);          // 追加
                vector<string> messages = spilit_message(conn->read_buffer);
                for (auto& message : messages) {
                    if (conn->handler)
                        conn->handler->on_message(conn,
                                                  message);
                }
            }
        }
    }

bool epoll_make::send(shared_ptr<Internalconnection> conn, const char* data, size_t len) {
        string message = send_preview(string(data, len));   // 加长度头
        lock_guard<mutex> lock(conn->send_mutex);
        if (!conn->connected) return false;
        conn->send_queue.push(message);
        mod_event(conn, EPOLLIN | EPOLLET | EPOLLOUT);      // 请求可写通知
        return true;
    }

void epoll_make::try_send(shared_ptr<Internalconnection> conn) {
        if (!conn->connected) return;
        bool write_error = false;
        {
            unique_lock<mutex> lock(conn->send_mutex);
            while (!conn->send_queue.empty()) {
                auto& data = conn->send_queue.front();
                ssize_t n = write(conn->sock, data.data(), data.size());
                if (n > 0) {
                    if ((size_t)n == data.size()) conn->send_queue.pop();   // 发完
                    else { data.erase(0, n); break; }                        // 部分发送
                }
                else {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;     // 等下次
                    write_error = true;                                     // 修复2：真错误
                    break;
                }
            }
            if (!write_error && conn->send_queue.empty() && conn->connected)
                mod_event(conn, EPOLLIN | EPOLLET);   // 发完取消EPOLLOUT
        }
        // 修复2：释放锁后再关，避免 on_disconnect 里回调 send 造成死锁
        if (write_error) close_client(conn);
    }

void epoll_make::close_client(shared_ptr<Internalconnection> conn) {
        if (!conn || !conn->connected) return;
        conn->connected = false;
        if (conn->handler) {
            conn->handler->on_disconnect(conn);
            delete conn->handler;
            conn->handler = nullptr;
        }
        del_event(conn);
        close(conn->sock);
        auto it = find(connections.begin(), connections.end(), conn);
        if (it != connections.end()) connections.erase(it);
    }

string epoll_make::send_preview(string send_message) {
        size_t len = send_message.size();
        string len_str = to_string(len);
        while (len_str.size() < 4) len_str = "0" + len_str;
        return len_str + send_message;
    }

vector<string> epoll_make::spilit_message(string& message) {
        vector<string> messages;
        size_t pos = 0;
        while (pos + 4 <= message.size()) {
            string len_str = message.substr(pos, 4);
            bool valid = true;
            for (char c : len_str) {
                if (c < '0' || c > '9') { valid = false; break; }
            }
            if (!valid) {
                pos++;         // 跳过坏字节，继续找下一个长度头
                continue;
            }
            size_t len = stoi(len_str);
            if (pos + 4 + len > message.size()) break;   // 半包，等下次
            messages.push_back(message.substr(pos + 4, len));
            pos += 4 + len;
        }
        message.erase(0, pos);   // 已处理的消息移除
        return messages;
    }

void epoll_make::event_loop() {
        while (running) {
            int n = epoll_wait(epoll_fd, events.data(), events.size(), -1);
            if (n < 0) { if (errno == EINTR) continue; break; }

            for (int i = 0; i < n; i++) {
                if (events[i].data.fd == listen_fd) {
                    handle_accept();
                    continue;
                }
                if (events[i].data.fd == wake_fd) {   // 修复3：退出信号
                    uint64_t one = 0;
                    read(wake_fd, &one, sizeof(one));
                    continue;
                }
                auto* raw = (Internalconnection*)events[i].data.ptr;
                // 修复4：本批事件可能已由前面的处理关掉了这条连接，先确认还活着
                auto it = find_if(connections.begin(), connections.end(),
                                  [raw](const shared_ptr<Internalconnection>& c) {
                                      return c.get() == raw;
                                  });
                if (it == connections.end()) continue;
                auto conn = *it;

                if (conn->connected && (events[i].events & (EPOLLERR | EPOLLHUP))) {
                    close_client(conn);
                    continue;
                }
                if (conn->connected && (events[i].events & EPOLLIN))
                    handle_read(conn);
                if (conn->connected && (events[i].events & EPOLLOUT))
                    try_send(conn);
            }
        }
    }

epoll_make::epoll_make(int a, int b, int c)
    : port(a), N(b), M(c),
      listen_fd(-1), epoll_fd(-1), wake_fd(-1) {
    events.resize(M);
}

epoll_make::~epoll_make() {
        running = false;
        if (wake_fd >= 0) {
            uint64_t one = 1;
            write(wake_fd, &one, sizeof(one));   // 修复3：唤醒 epoll_wait
        }
        if (event_thread.joinable()) event_thread.join();
        for (auto& conn : connections) {
            conn->connected = false;
            if (conn->handler) {
                conn->handler->on_disconnect(conn);
                delete conn->handler;
                conn->handler = nullptr;
            }
            if (conn->sock >= 0) close(conn->sock);
        }
        connections.clear();
        if (epoll_fd >= 0) close(epoll_fd);
        if (wake_fd >= 0) close(wake_fd);
        if (listen_fd >= 0) close(listen_fd);
    }

void epoll_make::start(Handler_epoll_Factory* f) {
        factory = f;

        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) { perror("socket"); return; }
        int reuse = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return; }
        if (listen(listen_fd, N) < 0) { perror("listen"); return; }
        set_nonblocking(listen_fd);

        epoll_fd = epoll_create1(0);
        if (epoll_fd < 0) { perror("epoll_create1"); return; }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) { perror("epoll_ctl listen"); return; }

        wake_fd = eventfd(0, EFD_NONBLOCK);   // 修复3：退出唤醒通道
        if (wake_fd < 0) { perror("eventfd"); return; }
        epoll_event wev{};
        wev.events = EPOLLIN;
        wev.data.fd = wake_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wake_fd, &wev) < 0) { perror("epoll_ctl wake"); return; }

        running = true;
        event_thread = thread(&epoll_make::event_loop, this);
    }

    void epoll_make::stop() {
    running = false;
    if (wake_fd >= 0) {
        uint64_t one = 1;
        write(wake_fd, &one, sizeof(one));   // 唤醒 epoll_wait
    }
    if (event_thread.joinable()) event_thread.join();
}

void epoll_make::stop_accept() {
    if (listen_fd >= 0) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, listen_fd, nullptr);
    }
}
void epoll_make::close_all_connections() {
    for (auto& conn : connections) {
        conn->connected = false;
        if (conn->handler) {
            conn->handler->on_disconnect(conn);
            delete conn->handler;
            conn->handler = nullptr;
        }
        if (conn->sock >= 0) close(conn->sock);
    }
    connections.clear();
}
