#pragma once
#include <iostream>
#include <thread>
#include <string>
#include <memory>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include"Internalconnection.h"
#include"Handler_divide.h"
using namespace std;
struct divide_task {
    std::function<std::function<void()>()> back_funtion;                    // 解析函数
    std::shared_ptr<Internalconnection> connection;                            // 这条任务属于哪个连接
    std::shared_ptr<Handler_divide> handler=nullptr;                 // 业务分发器
    divide_task(std::function<std::function<void()>()> b, std::shared_ptr<Internalconnection> d, std::shared_ptr<Handler_divide> h)
        : back_funtion(b), connection(d), handler(h) {}
    divide_task() = default;
};