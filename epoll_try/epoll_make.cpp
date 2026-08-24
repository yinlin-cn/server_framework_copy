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

using namespace std;

struct InternalConnection;

class Handler {
public:
    virtual ~Handler() = default;
    virtual void on_message(InternalConnection* conn, const char* data, int len) = 0;
    virtual void on_connect(InternalConnection* conn) {}
    virtual void on_disconnect(InternalConnection* conn) {}
};

class HandlerFactory {
public:
    virtual Handler* create_handler() = 0;
};

struct InternalConnection : enable_shared_from_this<InternalConnection> {
    int sock;
    string read_buffer;                       // 半包暂存
    bool connected;
    Handler* handler;                         // 本连接专属
    queue<string> send_queue;                 // 发送缓冲
    mutex send_mutex;
    function<void(const string&)> send_function;   // 业务发送入口

    InternalConnection(int fd)
        : sock(fd), connected(true), handler(nullptr) {}
};

class epoll_make {
private:
    int epoll_fd;
    int listen_fd;
    int wake_fd;                    // 修复3：eventfd，用于安全唤醒 epoll_wait
    int port;
    int N;                          // 监听队列长度
    int M;                          // epoll 最大事件数
    atomic<bool> runing{false};     // 修复3：跨线程读写必须原子
    HandlerFactory* factory;
    thread event_thread;
    vector<epoll_event> events;
    vector<shared_ptr<InternalConnection>> connections;

    int set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    void mod_event(shared_ptr<InternalConnection> conn, uint32_t evs) {
        epoll_event ev{};
        ev.events = evs;
        ev.data.ptr = conn.get();
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn->sock, &ev);
    }

    void del_event(shared_ptr<InternalConnection> conn) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->sock, nullptr);
    }

    void handle_accept() {
        while (true) {   // ET：一次接完所有排队连接
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);
            if (client_fd < 0) {
                if (errno == EINTR) continue;
                break;   // EAGAIN 说明接完了
            }
            set_nonblocking(client_fd);

            auto conn = make_shared<InternalConnection>(client_fd);
            conn->handler = factory->create_handler();   // 每连接一个专属handler
            // 修复1：捕获 weak_ptr，避免 conn 自己持有自己形成循环
            conn->send_function = [this, weak = weak_ptr<InternalConnection>(conn)](const string& msg) {
                if (auto c = weak.lock())
                    this->send(c, msg.data(), msg.size());
            };

            connections.push_back(conn);

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;               // 只监听可读
            ev.data.ptr = conn.get();
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

            if (conn->handler) conn->handler->on_connect(conn.get());
        }
    }

    void handle_read(shared_ptr<InternalConnection> conn) {
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
                        conn->handler->on_message(conn.get(),
                                                  message.data(), message.size());
                }
            }
        }
    }

    bool send(shared_ptr<InternalConnection> conn, const char* data, size_t len) {
        string message = send_preview(string(data, len));   // 加长度头
        lock_guard<mutex> lock(conn->send_mutex);
        if (!conn->connected) return false;
        conn->send_queue.push(message);
        mod_event(conn, EPOLLIN | EPOLLET | EPOLLOUT);      // 请求可写通知
        return true;
    }

    void try_send(shared_ptr<InternalConnection> conn) {
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

    void close_client(shared_ptr<InternalConnection> conn) {
        if (!conn || !conn->connected) return;
        conn->connected = false;
        if (conn->handler) {
            conn->handler->on_disconnect(conn.get());
            delete conn->handler;
            conn->handler = nullptr;
        }
        del_event(conn);
        close(conn->sock);
        auto it = find(connections.begin(), connections.end(), conn);
        if (it != connections.end()) connections.erase(it);
    }

    string send_preview(string send_message) {
        size_t len = send_message.size();
        string len_str = to_string(len);
        while (len_str.size() < 4) len_str = "0" + len_str;
        return len_str + send_message;
    }

    vector<string> spilit_message(string& message) {
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

    void event_loop() {
        while (runing) {
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
                auto* raw = (InternalConnection*)events[i].data.ptr;
                // 修复4：本批事件可能已由前面的处理关掉了这条连接，先确认还活着
                auto it = find_if(connections.begin(), connections.end(),
                                  [raw](const shared_ptr<InternalConnection>& c) {
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

public:
    epoll_make(int a = 9001, int b = 128, int c = 64)
        : port(a), N(b), M(c),
          listen_fd(-1), epoll_fd(-1), wake_fd(-1) {
        events.resize(M);
    }

    ~epoll_make() {
        runing = false;
        if (wake_fd >= 0) {
            uint64_t one = 1;
            write(wake_fd, &one, sizeof(one));   // 修复3：唤醒 epoll_wait
        }
        if (event_thread.joinable()) event_thread.join();
        for (auto& conn : connections) {
            conn->connected = false;
            if (conn->handler) {
                conn->handler->on_disconnect(conn.get());
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

    void start(HandlerFactory* f) {
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

        runing = true;
        event_thread = thread(&epoll_make::event_loop, this);
    }
};

// ================= 业务示例（每连接独立状态）=================
class EchoHandler : public Handler {
private:
    int msg_count_ = 0;   // 每连接独立计数（验证状态隔离）
public:
    void on_message(InternalConnection* conn, const char* data, int len) override {
        msg_count_++;
        printf("[连接#%d] 收到: %.*s\n", msg_count_, len, data);
        conn->send_function(std::string(data, len));   // echo回显
    }
};

class EchoFactory : public HandlerFactory {
public:
    Handler* create_handler() override { return new EchoHandler(); }
};

int main() {
    epoll_make server(9001);
    EchoFactory factory;
    server.start(&factory);
    printf("echo 服务器启动，端口 9001\n");
    while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
    return 0;
}