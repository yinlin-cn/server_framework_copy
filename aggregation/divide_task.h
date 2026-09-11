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
#include <utility>
#include"Internalconnection.h"
#include"Handler_divide.h"
using namespace std;
class DB_credit_token;

struct divide_task {
    std::function<std::function<void()>()> back_funtion;                    // 解析函数
    std::shared_ptr<Internalconnection> connection;                            // 这条任务属于哪个连接
    std::shared_ptr<Handler_divide> handler=nullptr;                 // 业务分发器
    std::shared_ptr<DB_credit_token> db_credit_token;                 // 该消息已占用的 DB 准入额度（db 任务才有）
    divide_task(std::function<std::function<void()>()> b, std::shared_ptr<Internalconnection> d,
                std::shared_ptr<Handler_divide> h,
                std::shared_ptr<DB_credit_token> credit = nullptr)
        : back_funtion(b), connection(d), handler(h), db_credit_token(std::move(credit)) {}
    divide_task() = default;
};
