#include "Reactor.h"
#include "Metrics.h"

using namespace std;

Reactor::Reactor(int max_events, Handler_epoll_Factory* factory,
                 uint64_t idle_timeout_us)
    : max_events_(max_events), idle_timeout_us_(idle_timeout_us), factory_(factory),
      epoll_fd_(-1), wake_fd_(-1) {
    events_.resize(max_events_);
}

Reactor::~Reactor() {
    stop();
}

int Reactor::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void Reactor::mod_event(shared_ptr<Internalconnection> conn, uint32_t evs) {
    epoll_event ev{};
    ev.events = evs;
    ev.data.ptr = conn.get();
    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn->sock, &ev);
}

void Reactor::del_event(shared_ptr<Internalconnection> conn) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn->sock, nullptr);
}

string Reactor::send_preview(const string& msg) {
    string len_str = to_string(msg.size());
    while (len_str.size() < 4) len_str = "0" + len_str;
    return len_str + msg;
}

vector<string> Reactor::spilit_message(string& message) {
    vector<string> messages;
    size_t pos = 0;
    while (pos + 4 <= message.size()) {
        string len_str = message.substr(pos, 4);
        bool valid = true;
        for (char c : len_str) {
            if (c < '0' || c > '9') { valid = false; break; }
        }
        if (!valid) { pos++; continue; }
        size_t len = stoi(len_str);
        if (pos + 4 + len > message.size()) break;
        messages.push_back(message.substr(pos + 4, len));
        pos += 4 + len;
    }
    message.erase(0, pos);
    return messages;
}

bool Reactor::take_one_message(std::string& buffer, std::string& out) {
    if (!peek_one_message(buffer, out))
        return false;
    buffer.erase(0, 4 + out.size());
    return true;
}

bool Reactor::peek_one_message(const std::string& buffer,
                               std::string& out) const {
    if (buffer.size() < 4)
        return false;

    for (int i = 0; i < 4; i++)
        if (buffer[i] < '0' || buffer[i] > '9')
            return false;

    size_t len = static_cast<size_t>(stoi(buffer.substr(0, 4)));
    if (buffer.size() < 4 + len)
        return false;

    out = buffer.substr(4, len);
    return true;
}

bool Reactor::enqueue_send(shared_ptr<Internalconnection> conn, const string& msg) {
    lock_guard<mutex> lock(conn->send_mutex);
    if (!conn->connected) return false;
    conn->send_queue.push(send_preview(msg));
    return true;
}

void Reactor::add_connection(shared_ptr<Internalconnection> conn) {
    set_nonblocking(conn->sock);
    conn->owner_reactor = this;
    // 连接刚登记就算活跃，避免 last_active_us=0 被心跳扫描误判。
    conn->last_active_us = Metrics::now_us();

    // 业务线程跨线程发送：入队 + 登记待发送桶 + 唤醒本 reactor
    conn->send_function = [this, weak = weak_ptr<Internalconnection>(conn)](const string& msg) -> bool {
        if (auto c = weak.lock()) {
            if (!enqueue_send(c, msg)) return false;   // 连接已断开，入队失败
            {
                lock_guard<mutex> lock(pending_mutex_);
                pending_send_.push_back(weak);
            }
            if (batch_handler_)
                batch_handler_->on_need_send(this);   // 走批处理：只标记，不立即唤醒
            else
                wakeup();                              // 无批处理时回退直接唤醒
            return true;
        }
        return false;                                  // 连接对象已销毁
    };

    // on_connect 必须在连接对事件线程可见之前执行，否则客户端立即断开时
    // close_client 可能并发 delete handler，和 add_connection 产生数据竞争。
    if (conn->handler) conn->handler->on_connect(conn);

    {
        lock_guard<mutex> lock(conn_mutex_);
        connections_[conn->sock] = conn;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = conn.get();
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn->sock, &ev);

    if (log_) log_->info("conn=" + std::to_string(conn->sock) + " opened");
    if (metrics_) metrics_->on_conn_open();
}

void Reactor::wakeup() {
    if (wake_fd_ >= 0) {
        uint64_t one = 1;
        write(wake_fd_, &one, sizeof(one));
    }
}

void Reactor::pause_reading(shared_ptr<Internalconnection> conn) {
    if (!conn || conn->reading_paused)
        return;
    conn->reading_paused = true;
    mod_event(conn, 0);   // 关闭 EPOLLIN，保留连接本身
}

void Reactor::schedule_resume(shared_ptr<Internalconnection> conn) {
    if (!conn)
        return;
    {
        lock_guard<mutex> lock(pending_resume_mutex_);
        pending_resume_.push_back(conn);
    }
    wakeup();
}

void Reactor::process_pending_resume() {
    vector<weak_ptr<Internalconnection>> bucket;
    {
        lock_guard<mutex> lock(pending_resume_mutex_);
        bucket.swap(pending_resume_);
    }

    vector<shared_ptr<Internalconnection>> conns;
    for (auto& wk : bucket)
        if (auto c = wk.lock())
            conns.push_back(std::move(c));

    // 同一条连接可能积累多个恢复请求，先去重再恢复，避免重复清 reading_paused。
    sort(conns.begin(), conns.end(),
         [](const auto& a, const auto& b) { return a.get() < b.get(); });
    conns.erase(unique(conns.begin(), conns.end(),
                       [](const auto& a, const auto& b) {
                           return a.get() == b.get();
                       }),
                conns.end());

    for (auto& c : conns) {
        bool resume_signal = c->flow.consume_resume();
        bool db_signal = c->reading_paused.exchange(false);
        if ((resume_signal || db_signal) && c->connected) {
            mod_event(c, EPOLLIN | EPOLLET);
            handle_read(c);
        }
    }
}

void Reactor::request_close(shared_ptr<Internalconnection> conn,
                            const string& reason) {
    (void)reason;   // 当前关闭原因只做记录用，后续可对接日志
    if (!conn) return;
    {
        lock_guard<mutex> lock(pending_close_mutex_);
        pending_close_.push_back(conn);
    }
    wakeup();
}

void Reactor::process_pending_close() {
    vector<weak_ptr<Internalconnection>> bucket;
    {
        lock_guard<mutex> lock(pending_close_mutex_);
        bucket.swap(pending_close_);
    }
    for (auto& wk : bucket)
        if (auto c = wk.lock())
            close_client(c);
}

void Reactor::set_batch_handler(Handler_batch* handler) {
    batch_handler_ = handler;
}

void Reactor::handle_read(shared_ptr<Internalconnection> conn) {
    char buffer[1024];
    while (conn->connected && !conn->reading_paused) {
        std::string msg;
        if (!peek_one_message(conn->read_buffer, msg)) {
            ssize_t count = read(conn->sock, buffer, sizeof(buffer));
            if (count == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                close_client(conn);
                return;
            }
            if (count == 0) { close_client(conn); return; }

            conn->read_buffer.append(buffer, count);
            conn->last_active_us = Metrics::now_us();
            continue;
        }

        if (!conn->flow.try_take())
            break;   // 窗口满：消息留在 read_buffer，等后续恢复读取

        if (conn->handler) {
            PushResult r = conn->handler->on_message(conn, msg);
            if (r == PushResult::Full) {
                conn->flow.release_slot();   // 未真正消费，窗口位归还
                break;                       // 消息留在 read_buffer，等 divide 空位
            }
            if (r == PushResult::Closed) {
                close_client(conn);
                return;
            }
        }

        conn->read_buffer.erase(0, 4 + msg.size());
    }
}

void Reactor::try_send(shared_ptr<Internalconnection> conn) {
    if (!conn->connected) return;
    bool write_error = false;
    {
        unique_lock<mutex> lock(conn->send_mutex);
        while (!conn->send_queue.empty()) {
            auto& data = conn->send_queue.front();
            ssize_t n = write(conn->sock, data.data(), data.size());
            if (n > 0) {
                if ((size_t)n == data.size()) conn->send_queue.pop();
                else { data.erase(0, n); break; }
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                write_error = true;
                break;
            }
        }
        if (!write_error && conn->send_queue.empty() && conn->connected)
            mod_event(conn, EPOLLIN | EPOLLET);
    }
    if (write_error) close_client(conn);
}

void Reactor::close_client(shared_ptr<Internalconnection> conn) {
    if (!conn || !conn->connected) return;
    conn->connected = false;
    if (conn->handler) {
        conn->handler->on_disconnect(conn);
        delete conn->handler;
        conn->handler = nullptr;
    }
    del_event(conn);
    close(conn->sock);
    if (log_) log_->info("conn=" + std::to_string(conn->sock) + " closed");
    if (metrics_) metrics_->on_conn_close();
    lock_guard<mutex> lock(conn_mutex_);
    connections_.erase(conn->sock);
}

void Reactor::event_loop() {
    uint64_t last_scan_us = 0;
    while (running_) {
        // 带超时：定期醒来扫描空闲超时连接（心跳检测）。
        int n = epoll_wait(epoll_fd_, events_.data(), events_.size(), 5000);
        if (n < 0) { if (errno == EINTR) continue; break; }

        for (int i = 0; i < n; i++) {
            if (events_[i].data.fd == wake_fd_) {
                uint64_t one = 0;
                read(wake_fd_, &one, sizeof(one));
                if (!running_) break;                 // 停机信号优先，不再处理发送

                process_pending_close();              // 先处理跨线程关闭请求
                process_pending_resume();             // 再恢复因背压暂停的连接

                vector<weak_ptr<Internalconnection>> bucket;
                {
                    lock_guard<mutex> lock(pending_mutex_);
                    bucket.swap(pending_send_);
                }
                for (auto& wk : bucket)
                    if (auto c = wk.lock())
                        try_send(c);
                continue;
            }

            auto* raw = (Internalconnection*)events_[i].data.ptr;
            shared_ptr<Internalconnection> conn;
            {
                lock_guard<mutex> lock(conn_mutex_);
                auto it = connections_.find(raw->sock);
                if (it != connections_.end()) conn = it->second;
            }
            if (!conn) continue;

            if (conn->connected && (events_[i].events & (EPOLLERR | EPOLLHUP))) {
                close_client(conn);
                continue;
            }
            if (conn->connected && (events_[i].events & EPOLLIN))
                handle_read(conn);
            if (conn->connected && (events_[i].events & EPOLLOUT))
                try_send(conn);
        }

        // 每 5 秒节流扫描一次：空闲超过 idle_timeout_us_ 的连接直接关闭。
        uint64_t now = Metrics::now_us();
        if (now - last_scan_us >= 5000000) {
            last_scan_us = now;
            vector<shared_ptr<Internalconnection>> snapshot;
            {
                lock_guard<mutex> lock(conn_mutex_);
                for (auto& [fd, c] : connections_)
                    snapshot.push_back(c);
            }
            for (auto& c : snapshot)
                if (c->connected && now - c->last_active_us > idle_timeout_us_)
                    close_client(c);
        }
    }
}

void Reactor::start() {
    epoll_fd_ = epoll_create1(0);
    wake_fd_ = eventfd(0, EFD_NONBLOCK);

    epoll_event wev{};
    wev.events = EPOLLIN;
    wev.data.fd = wake_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wev);

    running_ = true;
    event_thread_ = thread(&Reactor::event_loop, this);
}

void Reactor::stop() {
    running_ = false;
    wakeup();
    if (event_thread_.joinable()) event_thread_.join();
    for (auto& [fd, conn] : connections_) {
        conn->connected = false;
        if (conn->handler) {
            conn->handler->on_disconnect(conn);
            delete conn->handler;
            conn->handler = nullptr;
        }
        if (conn->sock >= 0) close(conn->sock);
    }
    connections_.clear();
    if (epoll_fd_ >= 0) close(epoll_fd_);
    if (wake_fd_ >= 0) close(wake_fd_);
}
