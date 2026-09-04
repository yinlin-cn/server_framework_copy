#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "FrameworkCall.h"

// 框架外独立长连接线程持有的会话句柄
class ConnectionSession {
public:
    ConnectionSession(std::shared_ptr<FrameworkCall> fc,
                      uint64_t virtual_fd,
                      int group_name = -1)
        : fc_(std::move(fc)),
          virtual_fd_(virtual_fd),
          group_name_(group_name) {}

    bool send(const std::string& msg) const;
    call_result call(const std::string& cmd,
                     const FrameworkCall::Args& args) const;
    bool close(const std::string& reason = "") const;
    bool connected() const;

    uint64_t virtual_fd() const { return virtual_fd_; }
    int group_name() const { return group_name_; }

    uint64_t version() const;
    bool wait_version_change(
        uint64_t old_version,
        std::chrono::milliseconds timeout =
            std::chrono::milliseconds(5000)) const;

private:
    std::shared_ptr<FrameworkCall> fc_;
    uint64_t virtual_fd_;
    int group_name_;
};
