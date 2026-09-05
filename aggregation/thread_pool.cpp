#include"thread_pool.h"
#include "Metrics.h"
#include "EventTask.h"
#include "Reactor.h"

using namespace std;
thread_pool::thread_pool(int N)
    : tasks_(static_cast<size_t>(N * 32)) {
        for (int i = 0; i < N; i++)
            pool.emplace_back(&thread_pool::worker, this);
    }
thread_pool::~thread_pool() {
        tasks_.close();
        for (auto& t : pool)
            if (t.joinable()) t.join();
    }

void thread_pool::worker() {
        while (true) {
            work_task f;
            if (!tasks_.pop(f))
                return;
            tls_current_conn = f.conn;
            g_coroutine_suspended = false;
            auto active = std::make_shared<std::atomic<bool>>(true);
            g_current_task_active = active;
            if (metrics_) metrics_->on_task_dequeued(PoolId::Work);
            bool business = f.is_business;   // 业务请求任务才计入请求级 QPS
            if (metrics_ && business) metrics_->on_request_started();
            uint64_t start_us = Metrics::now_us();
            active_++;            // 取到任务后
            try {
                f.fn();
            } catch (const std::exception& e) {
                if (error_handler_) error_handler_(f.conn, "work", e.what());
                if (log_) log_->error("work failed: " + std::string(e.what()));
                if (metrics_ && business) metrics_->on_error(ErrorStage::Work);
            } catch (...) {
                if (error_handler_) error_handler_(f.conn, "work", "unknown error");
                if (log_) log_->error("work failed: unknown error");
                if (metrics_ && business) metrics_->on_error(ErrorStage::Work);
            }
            if (active)
                active->store(false);   // 当前 fn 已返回，DB 可以唤醒协程
            g_current_task_active.reset();
            // 没有挂起说明任务已经跑到头：归还窗口。
            if (!g_coroutine_suspended && f.conn) {
                bool need_resume = f.conn->flow.finish_one();
                if (need_resume && !f.conn->reading_paused.load() &&
                    f.conn->owner_reactor) {
                    f.conn->owner_reactor->schedule_resume(f.conn);
                }
            }
            g_coroutine_suspended = false;
            uint64_t done_us = Metrics::now_us() - start_us;
            if (metrics_) {
                metrics_->on_module_task_done(PoolId::Work, done_us);   // work 模块
                if (business) metrics_->on_request_done(done_us);   // 业务请求级
            }
            tls_current_conn.reset();
            active_--;            // 跑完（无论成败）
            if (active_ == 0) idle_cv_.notify_all();
        }
    }

void thread_pool::add_task(work_task f) {
        tasks_.push(std::move(f));
        if (metrics_) metrics_->on_task_enqueued(PoolId::Work);
    }

void thread_pool::shutdown() {
    tasks_.close();
}

bool thread_pool::wait_idle(const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(idle_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this]{ return active_ == 0; });
}
