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
        tasks_.set_low_water_callback([this] {
            if (low_water_callback_)
                low_water_callback_();
        });
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
             try {
                std::function<void()> work = funtion.back_funtion();
                // 2. 交给业务分发器，解析层不关心业务层内部结构
                if (funtion.handler)
                    funtion.handler->on_work(funtion.connection, work,
                                             funtion.db_credit_token);
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
            finish_task();
        }
    }

push_result divide_pool::add_task(divide_task funtion) {
        {
            std::lock_guard<std::mutex> lock(idle_mutex_);
            ++unfinished_task_count_;
        }

        push_result r = tasks_.push(std::move(funtion));
        if (r != push_result::Ok) {
            std::lock_guard<std::mutex> lock(idle_mutex_);
            --unfinished_task_count_;
            return r;
        }
        if (metrics_) metrics_->on_task_enqueued(PoolId::Divide);
        return push_result::Ok;
    }

push_result divide_pool::try_add_task(divide_task funtion) {
    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        ++unfinished_task_count_;
    }

    push_result r = tasks_.try_push(std::move(funtion));
    if (r == push_result::Ok) {
        if (metrics_) metrics_->on_task_enqueued(PoolId::Divide);
        return r;
    }

    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        --unfinished_task_count_;
    }
    return r;
}

void divide_pool::shutdown() {
    tasks_.close();
}

void divide_pool::set_low_water_callback(std::function<void()> callback) {
    low_water_callback_ = std::move(callback);
}

bool divide_pool::wait_idle(const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(idle_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this]{ return unfinished_task_count_ == 0; });
}

void divide_pool::finish_task() {
    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        if (unfinished_task_count_ > 0)
            --unfinished_task_count_;
    }
    idle_cv_.notify_all();
}
