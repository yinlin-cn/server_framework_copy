#pragma once
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>

#include <mysql.h>   // 真实版需要 mysql client 库

using DBHandle = MYSQL*;

class connect_pool {
    std::queue<DBHandle> conns_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool closed_ = false;

    std::string host_, user_, password_, database_;
    unsigned int port_;

    DBHandle create_connection();
    void close_connection(DBHandle conn);

public:
    connect_pool(int size,
                 const std::string& host,
                 const std::string& user,
                 const std::string& password,
                 const std::string& database,
                 unsigned int port = 3306);
    ~connect_pool();
    void shutdown(); 
    DBHandle get();          // 池空时阻塞等待
    void release(DBHandle conn);
};