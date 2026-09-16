#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include "Handler_log.h"

// 日志级别：Debug < Info < Warn < Error。
enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };

// 日志实现：业务线程只 push 队列，后台线程批量写文件。
class Logger : public Handler_log {
public:
    // file 为空则写 stderr；min_level 过滤低于该级别的日志。
    explicit Logger(const std::string& file = "",
                    LogLevel min_level = LogLevel::Info);
    ~Logger();

    void start();              // 启动后台写线程
    void flush_and_stop();     // 优雅退出：写空队列再停线程

    void info(const std::string& msg) override;
    void warn(const std::string& msg) override;
    void error(const std::string& msg) override;

private:
    std::deque<std::string> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    LogLevel min_level_;
    std::ofstream out_;

    void log(LogLevel lv, const std::string& msg);
    std::string format(LogLevel lv, const std::string& msg);
    static const char* level_name(LogLevel lv);
    void worker_loop();
};
