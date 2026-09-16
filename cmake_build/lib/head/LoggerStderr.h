#pragma once
#include <iostream>
#include "Handler_log.h"

// 最简单的日志实现：先输出到 stderr，后续可替换成文件 / JSON。
class LoggerStderr : public Handler_log {
public:
    void info(const std::string& msg) override {
        std::cerr << "[INFO] " << msg << std::endl;
    }
    void warn(const std::string& msg) override {
        std::cerr << "[WARN] " << msg << std::endl;
    }
    void error(const std::string& msg) override {
        std::cerr << "[ERROR] " << msg << std::endl;
    }
};
