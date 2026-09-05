#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
using namespace std;
struct Box {
    std::string result;
    std::string err;          // 新增：DB 执行失败时的错误描述
    std::vector<std::vector<std::string>> rows;   // 完整结果：行 × 列，业务层自行取用/限制
    bool ready = false;
    bool cancelled = false;   // 新增：退出/超时时标记，协程恢复后感知
    uint64_t wait_name = 0;
    std::shared_ptr<std::atomic<bool>> wake_guard;  // 当前业务任务是否还在占用协程
};
