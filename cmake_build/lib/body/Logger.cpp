#include "Logger.h"

#include <chrono>
#include <ctime>
#include <iostream>

Logger::Logger(const std::string& file, LogLevel min_level)
    : min_level_(min_level) {
    if (!file.empty())
        out_.open(file, std::ios::app);
}

Logger::~Logger() {
    flush_and_stop();
}

const char* Logger::level_name(LogLevel lv) {
    switch (lv) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO";
        case LogLevel::Warn:  return "WARN";
        case LogLevel::Error: return "ERROR";
    }
    return "INFO";
}

std::string Logger::format(LogLevel lv, const std::string& msg) {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    localtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);

    std::string line = "[";
    line += buf;
    line += ".";
    line += std::to_string(ms.count());
    line += "] [";
    line += level_name(lv);
    line += "] ";
    line += msg;
    return line;
}

void Logger::log(LogLevel lv, const std::string& msg) {
    if (lv < min_level_) return;          // 级别过滤
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(format(lv, msg));   // 只 push，不碰磁盘
    }
    cv_.notify_one();
}

void Logger::info(const std::string& msg)  { log(LogLevel::Info, msg); }
void Logger::warn(const std::string& msg)  { log(LogLevel::Warn, msg); }
void Logger::error(const std::string& msg) { log(LogLevel::Error, msg); }

void Logger::worker_loop() {
    while (running_) {
        std::deque<std::string> batch;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(50));
            batch.swap(queue_);
        }
        for (auto& line : batch) {
            if (out_.is_open()) out_ << line << '\n';
            else                std::cerr << line << std::endl;
        }
    }
}

void Logger::start() {
    running_ = true;
    thread_ = std::thread(&Logger::worker_loop, this);
}

void Logger::flush_and_stop() {
    running_ = false;
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();

    // 最后一批：保证队列里的日志不丢。
    std::deque<std::string> last;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        last.swap(queue_);
    }
    for (auto& line : last) {
        if (out_.is_open()) out_ << line << '\n';
        else                std::cerr << line << std::endl;
    }
    if (out_.is_open()) out_.flush();
}
