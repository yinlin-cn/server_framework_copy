#include "FrameworkCall.h"

#include <cstdlib>
#include <sstream>

using namespace std;

static uint64_t parse_u64(const string& s) {
    if (s.empty())
        return 0;
    return static_cast<uint64_t>(strtoull(s.c_str(), nullptr, 10));
}

static vector<uint64_t> parse_fds(const string& s) {
    vector<uint64_t> result;
    stringstream ss(s);
    string part;
    while (getline(ss, part, ',')) {
        if (!part.empty())
            result.push_back(parse_u64(part));
    }
    return result;
}

void FrameworkCall::register_internal(const std::string& cmd,
                                      InternalHandler handler) {
    lock_guard<mutex> lock(handlers_mutex_);
    handlers_[cmd] = std::move(handler);
}

std::shared_ptr<connect_book> FrameworkCall::connection_book() const {
    return wp_ ? wp_->connection_book() : nullptr;
}

void FrameworkCall::set_conn_provider(std::function<Conn()> provider) {
    current_conn_ = std::move(provider);
}

void FrameworkCall::set_close_handler(
    std::function<void(Conn, const std::string&)> handler) {
    close_conn_ = std::move(handler);
}

call_result FrameworkCall::call(const std::string& cmd, const Args& args) {
    if (!running_)
        return {false, "framework stopped"};

    call_result result = handle_builtin(cmd, args);
    if (!result.err.empty() || result.success)
        return result;

    InternalHandler handler;
    {
        lock_guard<mutex> lock(handlers_mutex_);
        auto it = handlers_.find(cmd);
        if (it != handlers_.end())
            handler = it->second;
    }
    if (!handler)
        return {false, "unknown command: " + cmd};
    return handler(args);
}

call_result FrameworkCall::handle_builtin(const std::string& cmd,
                                          const Args& args) {
    auto book = connection_book();
    if (!book)
        return {false, "connect_book not ready"};

    if (cmd == "send_to_sb") {
        if (args.size() < 2)
            return {false, "send_to_sb needs fd,msg"};
        bool ok = book->send_to(parse_u64(args[0]), args[1]);
        return {ok, ok ? "" : "send_to_sb failed"};
    }

    if (cmd == "send_to_gp") {
        if (args.size() < 2)
            return {false, "send_to_gp needs group,msg"};
        book->send_group(static_cast<int>(parse_u64(args[0])), args[1]);
        return {true, ""};
    }

    if (cmd == "divide_gp") {
        if (args.empty())
            return {false, "divide_gp needs group,fds"};
        auto fds = parse_fds(args.size() > 1 ? args[1] : "");
        bool ok = book->assign_group(
            static_cast<int>(parse_u64(args[0])), fds);
        return {ok, ok ? "" : "divide_gp failed"};
    }

    if (cmd == "bind") {
        if (args.size() < 1 || !current_conn_)
            return {false, "bind needs business fd"};
        auto conn = current_conn_();
        if (!conn)
            return {false, "no current connection"};
        int group = args.size() > 1
            ? static_cast<int>(parse_u64(args[1])) : -1;
        bool ok = book->rebind(conn, parse_u64(args[0]), group);
        return {ok, ok ? "" : "bind failed"};
    }

    if (cmd == "set_group") {
        if (args.size() < 2)
            return {false, "set_group needs fd,group"};
        bool ok = book->set_group(parse_u64(args[0]),
                                  static_cast<int>(parse_u64(args[1])));
        return {ok, ok ? "" : "set_group failed"};
    }

    if (cmd == "close_conn") {
        if (args.empty() || !close_conn_)
            return {false, "close handler not ready"};
        auto conn = book->find(parse_u64(args[0]));
        if (!conn)
            return {false, "connection not found"};
        string reason = args.size() > 1 ? args[1] : "";
        close_conn_(conn, reason);
        return {true, ""};
    }

    return {false, ""};   // 不是内置命令
}

call_result FrameworkCall::send_to_sb(uint64_t virtual_fd,
                                      const std::string& msg) {
    return framework_call("send_to_sb", virtual_fd, msg);
}

call_result FrameworkCall::send_to_gp(int group_name,
                                      const std::string& msg) {
    return framework_call("send_to_gp", group_name, msg);
}

call_result FrameworkCall::divide_gp(
    int group_name, const std::vector<uint64_t>& fds) {
    Args args;
    args.push_back(std::to_string(group_name));
    string joined;
    for (size_t i = 0; i < fds.size(); i++) {
        if (i)
            joined += ",";
        joined += std::to_string(fds[i]);
    }
    args.push_back(joined);
    return call("divide_gp", args);
}

call_result FrameworkCall::close_conn(uint64_t virtual_fd,
                                      const std::string& reason) {
    return framework_call("close_conn", virtual_fd, reason);
}
