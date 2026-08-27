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
#include "divide_task.h"
#include "ErrorHandler.h"
#include"Handler_divide.h"
using namespace std;
class divide_pool {
private:
    queue<divide_task> tasks;
    mutex for_task;
    bool stop = false;
    vector<thread> pool;
    condition_variable cv;
    ErrorHandler error_handler_;   // 业务层可选注入，默认空
public:
    divide_pool(int N=8);
    ~divide_pool();
    void worker();
    void add_task(divide_task funtion);
    void set_error_handler(ErrorHandler h) { error_handler_ = std::move(h); }
};