#include "connect_pool.h"

connect_pool::connect_pool(int size,
                           const std::string& host,
                           const std::string& user,
                           const std::string& password,
                           const std::string& database,
                           unsigned int port)
    : host_(host), user_(user), password_(password),
      database_(database), port_(port) {
    for (int i = 0; i < size; i++) {
        DBHandle conn = create_connection();
        if (conn) conns_.push(conn);
    }
}

connect_pool::~connect_pool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
    }
    cv_.notify_all();
    while (!conns_.empty()) {
        close_connection(conns_.front());
        conns_.pop();
    }
}

DBHandle connect_pool::create_connection() {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return nullptr;
    if (!mysql_real_connect(conn, host_.c_str(), user_.c_str(), password_.c_str(),
                            database_.c_str(), port_, nullptr, 0)) {
        mysql_close(conn);
        return nullptr;
    }
    return conn;
}

void connect_pool::close_connection(DBHandle conn) {
    if (conn) mysql_close(conn);
}

DBHandle connect_pool::get() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this]{ return closed_ || !conns_.empty(); });
    if (closed_) return nullptr;
    DBHandle conn = conns_.front();
    conns_.pop();
    return conn;
}

void connect_pool::release(DBHandle conn) {
    if (!conn) return;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        conns_.push(conn);
    }
    cv_.notify_one();
}