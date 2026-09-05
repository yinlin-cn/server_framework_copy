#include"divide_pool.h"
#include"divide_task.h"
#include "Metrics.h"
#include <iostream>
#include <thread>
#include <string>
#include <memory>
#include <vector>
#include <functional>
using namespace std;
divide_pool::divide_pool(int N)
    : tasks_(static_cast<size_t>(N * 32)) {
        for (int i = 0; i < N; i++)
            pool.emplace_back(&divide_pool::worker, this);
    }
divide_pool::~divide_pool() {
        tasks_.close();
        for (auto& t : pool)
            if (t.joinable()) t.join();
    }

void divide_pool::worker() {
        while (true) {
            divide_task funtion;
            if (!tasks_.pop(funtion))
                return;
            // 1. 执行解析函数，得到真正的业务任务
            if (metrics_) metrics_->on_task_dequeued(PoolId::Divide);
            uint64_t start_us = Metrics::now_us();
            active_++;  
             try {
                std::function<void()> work = funtion.back_funtion();
                // 2. 交给业务分发器，解析层不关心业务层内部结构
                if (funtion.handler)
                    funtion.handler->on_work(funtion.connection, work);
            } catch (const std::exception& e) {
                if (error_handler_) error_handler_(funtion.connection, "divide", e.what());
                if (log_) log_->error("divide failed: " + std::string(e.what()));
                if (metrics_) metrics_->on_error(ErrorStage::Divide);
            } catch (...) {
                if (error_handler_) error_handler_(funtion.connection, "divide", "unknown error");
                if (log_) log_->error("divide failed: unknown error");
                if (metrics_) metrics_->on_error(ErrorStage::Divide);
            }
            if (metrics_) metrics_->on_module_task_done(PoolId::Divide, Metrics::now_us() - start_us);
            if (active_ == 0) idle_cv_.notify_all();
        }
    }

void divide_pool::add_task(divide_task funtion) {
        tasks_.push(std::move(funtion));
        if (metrics_) metrics_->on_task_enqueued(PoolId::Divide);
    }

PushResult divide_pool::try_add_task(divide_task funtion) {
    PushResult r = tasks_.try_push(std::move(funtion));
    if (r == PushResult::Ok && metrics_)
        metrics_->on_task_enqueued(PoolId::Divide);
    return r;
}

void divide_pool::shutdown() {
    tasks_.close();
}

bool divide_pool::wait_idle(const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(idle_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this]{ return active_ == 0; });
}
