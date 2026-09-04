#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Internalconnection.h"

// 一次连接名册变化
struct net_changer {
    uint64_t virtual_fd = 0;
    int group_name = -1;
    uint64_t version = 0;
    std::string cmd;   // add / remove / rebind / set_group / divide_gp
};

// 连接名册：只登记框架连接，不持有连接生命周期
class connect_book {
public:
    using Conn = std::shared_ptr<Internalconnection>;
    using WeakConn = std::weak_ptr<Internalconnection>;
    static constexpr size_t MAX_CHANGES = 4096;   // 变更缓存上限

    // 新连接，先用 conn->sock 作为临时 virtual_fd
    uint64_t on_connection(Conn conn);

    // 断连，通过 conn->sock 反查并清理
    void dis_connection(Conn conn);

    // 登录/鉴权成功后，把临时 fd 换成业务 virtual_fd
    bool rebind(Conn conn, uint64_t business_virtual_fd,
                int group_name = -1);

    // 修改单个连接的连接组
    bool set_group(uint64_t virtual_fd, int group_name);

    // 把一组 virtual_fd 划入连接组（保留不在列表里的其他组）
    bool assign_group(int group_name, const std::vector<uint64_t>& fds);

    Conn find(uint64_t virtual_fd) const;
    std::vector<Conn> group_snapshot(int group_name) const;
    std::vector<uint64_t> all_virtual_fds() const;

    bool send_to(uint64_t virtual_fd, const std::string& msg);
    void send_group(int group_name, const std::string& msg);

    uint64_t version() const;
    bool wait_version_change(
        uint64_t old_version,
        std::chrono::milliseconds timeout =
            std::chrono::milliseconds(5000));

    std::vector<net_changer> take_changes();
    void shutdown();

private:
    struct Entry {
        WeakConn conn;
        int group = -1;
    };

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped_ = false;
    uint64_t version_ = 0;

    std::unordered_map<uint64_t, Entry> virtual_map_;
    std::unordered_map<int, std::unordered_set<uint64_t>> group_map_;
    std::unordered_map<int, uint64_t> sock_to_virtual_;
    std::deque<net_changer> changes_;

    void notify_change(const std::string& cmd,
                       uint64_t virtual_fd, int group);
    void erase_from_group_locked(int group, uint64_t virtual_fd);
};
