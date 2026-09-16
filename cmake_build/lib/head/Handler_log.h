#pragma once
#include <string>

// 日志接口：Metrics 快照只依赖它，输出可换成 stderr / 文件 / JSON。
class Handler_log {
public:
    virtual ~Handler_log() = default;
    virtual void info(const std::string& msg) = 0;
    virtual void warn(const std::string& msg) = 0;
    virtual void error(const std::string& msg) = 0;
};
