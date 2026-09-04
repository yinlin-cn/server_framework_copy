#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "work_pool.h"

struct call_result {
    bool success = false;
    std::string err;
};

class FrameworkCall {
public:
    using Conn = std::shared_ptr<Internalconnection>;
    using Args = std::vector<std::string>;
    using InternalHandler = std::function<call_result(const Args&)>;

    explicit FrameworkCall(std::shared_ptr<work_pool> wp)
        : wp_(std::move(wp)) {}

    void register_internal(const std::string& cmd, InternalHandler handler);
    call_result call(const std::string& cmd, const Args& args);

    static std::string to_arg(const std::string& v) { return v; }
    static std::string to_arg(uint64_t v) { return std::to_string(v); }
    static std::string to_arg(int v) { return std::to_string(v); }

    template <typename... T>
    call_result framework_call(const std::string& cmd, T&&... argv) {
        Args args;
        (args.push_back(to_arg(std::forward<T>(argv))), ...);
        return call(cmd, args);
    }

    // 内置快捷入口
    call_result send_to_sb(uint64_t virtual_fd, const std::string& msg);
    call_result send_to_gp(int group_name, const std::string& msg);
    call_result divide_gp(int group_name, const std::vector<uint64_t>& fds);
    call_result close_conn(uint64_t virtual_fd,
                           const std::string& reason = "");

    // 需要“当前连接”的命令由业务池/会话注入
    void set_conn_provider(std::function<Conn()> provider);
    void set_close_handler(
        std::function<void(Conn, const std::string&)> handler);

    std::shared_ptr<connect_book> connection_book() const;

private:
    std::shared_ptr<work_pool> wp_;
    std::function<Conn()> current_conn_;
    std::function<void(Conn, const std::string&)> close_conn_;

    std::unordered_map<std::string, InternalHandler> handlers_;
    std::mutex handlers_mutex_;
    std::atomic<bool> running_{true};

    call_result handle_builtin(const std::string& cmd, const Args& args);
};
