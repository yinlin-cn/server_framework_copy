#include"divide_pool.h"
#include"divide_task.h"
#include <iostream>
#include <thread>
#include <string>
#include <memory>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
using namespace std;
divide_pool::divide_pool(int N) {
        for (int i = 0; i < N; i++)
            pool.emplace_back(&divide_pool::worker, this);
    }
divide_pool::~divide_pool() {
        {
            lock_guard<mutex> lock(for_task);
            stop = true;
        }
        cv.notify_all();
        for (auto& t : pool) t.join();
    }

void divide_pool::worker() {
        while (true) {
            divide_task funtion;
            {
                unique_lock<mutex> lock(for_task);
                cv.wait(lock, [this]() { return stop || !tasks.empty(); });
                if (stop && tasks.empty()) return;
                funtion = move(tasks.front());
                tasks.pop();
            }
            // 1. 执行解析函数，得到真正的业务任务
            std::function<void()> work = funtion.back_funtion();
            // 2. 交给业务分发器，解析层不关心业务层内部结构
            if (funtion.handler)
                funtion.handler->on_work(funtion.connection, work);
        }
    }

void divide_pool::add_task(divide_task funtion) {
        lock_guard<mutex> lock(for_task);
        tasks.push(move(funtion));
        cv.notify_one();
    }