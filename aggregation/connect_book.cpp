#include "connect_book.h"

#include <utility>

using namespace std;

void connect_book::notify_change(const std::string& cmd,
                                 uint64_t virtual_fd, int group) {
    version_++;
    net_changer c;
    c.cmd = cmd;
    c.virtual_fd = virtual_fd;
    c.group_name = group;
    c.version = version_;
    changes_.push_back(std::move(c));
    if (changes_.size() > MAX_CHANGES)
        changes_.pop_front();
    cv_.notify_all();
}

void connect_book::erase_from_group_locked(int group, uint64_t virtual_fd) {
    auto it = group_map_.find(group);
    if (it == group_map_.end())
        return;
    it->second.erase(virtual_fd);
    if (it->second.empty())
        group_map_.erase(it);
}

uint64_t connect_book::on_connection(Conn conn) {
    if (!conn)
        return 0;
    lock_guard<mutex> lock(mutex_);
    if (sock_to_virtual_.count(conn->sock))
        return 0;

    uint64_t vfd = static_cast<uint64_t>(conn->sock);
    virtual_map_[vfd] = Entry{conn, -1};
    sock_to_virtual_[conn->sock] = vfd;
    notify_change("add", vfd, -1);
    return vfd;
}

void connect_book::dis_connection(Conn conn) {
    if (!conn)
        return;
    lock_guard<mutex> lock(mutex_);
    auto sit = sock_to_virtual_.find(conn->sock);
    if (sit == sock_to_virtual_.end())
        return;

    uint64_t vfd = sit->second;
    auto vit = virtual_map_.find(vfd);
    if (vit != virtual_map_.end()) {
        erase_from_group_locked(vit->second.group, vfd);
        virtual_map_.erase(vit);
    }
    sock_to_virtual_.erase(sit);
    notify_change("remove", vfd, -1);
}

bool connect_book::rebind(Conn conn, uint64_t business_virtual_fd,
                          int group_name) {
    if (!conn)
        return false;
    lock_guard<mutex> lock(mutex_);
    auto sit = sock_to_virtual_.find(conn->sock);
    if (sit == sock_to_virtual_.end())
        return false;

    uint64_t old_fd = sit->second;
    auto oit = virtual_map_.find(old_fd);
    if (oit == virtual_map_.end())
        return false;

    if (business_virtual_fd != old_fd) {
        auto nit = virtual_map_.find(business_virtual_fd);
        if (nit != virtual_map_.end())
            return false;   // 业务标识已被其他连接占用

        Entry entry = std::move(oit->second);
        virtual_map_.erase(oit);
        erase_from_group_locked(entry.group, old_fd);
        entry.group = group_name;
        virtual_map_[business_virtual_fd] = std::move(entry);
        sock_to_virtual_[conn->sock] = business_virtual_fd;
        if (group_name >= 0)
            group_map_[group_name].insert(business_virtual_fd);
        notify_change("rebind", business_virtual_fd, group_name);
        return true;
    }

    int old_group = oit->second.group;
    if (old_group != group_name) {
        erase_from_group_locked(old_group, business_virtual_fd);
        oit->second.group = group_name;
        if (group_name >= 0)
            group_map_[group_name].insert(business_virtual_fd);
    }
    notify_change("rebind", business_virtual_fd, group_name);
    return true;
}

bool connect_book::set_group(uint64_t virtual_fd, int group_name) {
    lock_guard<mutex> lock(mutex_);
    auto it = virtual_map_.find(virtual_fd);
    if (it == virtual_map_.end())
        return false;
    if (it->second.group == group_name)
        return true;

    erase_from_group_locked(it->second.group, virtual_fd);
    it->second.group = group_name;
    if (group_name >= 0)
        group_map_[group_name].insert(virtual_fd);
    notify_change("set_group", virtual_fd, group_name);
    return true;
}

bool connect_book::assign_group(int group_name,
                                const std::vector<uint64_t>& fds) {
    lock_guard<mutex> lock(mutex_);
    for (uint64_t fd : group_map_[group_name]) {
        auto it = virtual_map_.find(fd);
        if (it != virtual_map_.end() && it->second.group == group_name)
            it->second.group = -1;
    }
    group_map_[group_name].clear();

    for (uint64_t fd : fds) {
        auto it = virtual_map_.find(fd);
        if (it == virtual_map_.end())
            continue;
        erase_from_group_locked(it->second.group, fd);
        it->second.group = group_name;
        group_map_[group_name].insert(fd);
    }
    notify_change("divide_gp", 0, group_name);
    return true;
}

connect_book::Conn connect_book::find(uint64_t virtual_fd) const {
    lock_guard<mutex> lock(mutex_);
    auto it = virtual_map_.find(virtual_fd);
    if (it == virtual_map_.end())
        return nullptr;
    return it->second.conn.lock();
}

std::vector<connect_book::Conn> connect_book::group_snapshot(
    int group_name) const {
    std::vector<Conn> result;
    lock_guard<mutex> lock(mutex_);
    auto it = group_map_.find(group_name);
    if (it == group_map_.end())
        return result;
    for (uint64_t fd : it->second) {
        auto vit = virtual_map_.find(fd);
        if (vit == virtual_map_.end())
            continue;
        if (auto c = vit->second.conn.lock())
            result.push_back(std::move(c));
    }
    return result;
}

std::vector<uint64_t> connect_book::all_virtual_fds() const {
    std::vector<uint64_t> result;
    lock_guard<mutex> lock(mutex_);
    result.reserve(virtual_map_.size());
    for (auto& [fd, e] : virtual_map_)
        result.push_back(fd);
    return result;
}

bool connect_book::send_to(uint64_t virtual_fd, const std::string& msg) {
    Conn conn;
    {
        lock_guard<mutex> lock(mutex_);
        auto it = virtual_map_.find(virtual_fd);
        if (it == virtual_map_.end())
            return false;
        conn = it->second.conn.lock();
    }
    if (!conn || !conn->send_function)
        return false;
    return conn->send_function(msg);
}

void connect_book::send_group(int group_name, const std::string& msg) {
    auto conns = group_snapshot(group_name);
    for (auto& c : conns) {
        if (c && c->send_function)
            c->send_function(msg);
    }
}

uint64_t connect_book::version() const {
    lock_guard<mutex> lock(mutex_);
    return version_;
}

bool connect_book::wait_version_change(uint64_t old_version,
                                       std::chrono::milliseconds timeout) {
    std::unique_lock<mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] {
        return stopped_ || version_ != old_version;
    });
}

std::vector<net_changer> connect_book::take_changes() {
    std::vector<net_changer> result;
    std::deque<net_changer> drained;
    {
        lock_guard<mutex> lock(mutex_);
        drained.swap(changes_);
    }
    for (auto& c : drained)
        result.push_back(std::move(c));
    return result;
}

void connect_book::shutdown() {
    {
        lock_guard<mutex> lock(mutex_);
        stopped_ = true;
    }
    cv_.notify_all();
}
