#pragma once
#include <cstdint>
#include<string>
using namespace std;
struct Box {
    std::string result;
    std::string err;          // 新增：DB 执行失败时的错误描述
    bool ready = false;
    bool cancelled = false;   // 新增：退出/超时时标记，协程恢复后感知
    uint64_t wait_name = 0;
};