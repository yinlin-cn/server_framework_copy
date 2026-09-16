#include"thread_pool.h"
#include "Metrics.h"
#include "EventTask.h"
#include "Box.h"
#include "backpressure.h"
#include "Reactor.h"
#include <exception>
#include <utility>

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
            auto active = std::make_shared<coroutine_suspend_guard>();
            g_current_task_suspend_guard = active;
            g_current_task_db_credit_token = f.db_credit_token;   // await_suspend 需要知道额度归属
            if (metrics_) metrics_->on_task_dequeued(PoolId::Work);
            bool business = f.is_business;   // 业务请求任务才计入请求级 QPS
            if (metrics_ && business) metrics_->on_request_started();
            uint64_t start_us = Metrics::now_us();
            tls_coroutine_exception = nullptr;
            try {
                f.fn();
                if (tls_coroutine_exception) {
                    auto e = std::exchange(tls_coroutine_exception, nullptr);
                    std::rethrow_exception(e);
                }
            } catch (const std::exception& e) {
                if (error_handler_) error_handler_(f.conn, "work", e.what());
                if (log_) log_->error("work failed: " + std::string(e.what()));
                if (metrics_ && business) metrics_->on_error(ErrorStage::Work);
            } catch (...) {
                if (error_handler_) error_handler_(f.conn, "work", "unknown error");
                if (log_) log_->error("work failed: unknown error");
                if (metrics_ && business) metrics_->on_error(ErrorStage::Work);
            }
            active->finish();   // 当前 fn 已返回，DB 可以唤醒协程
            g_current_task_suspend_guard.reset();
            // DB 额度按“业务流程”持有：任务没再挂起，说明整个流程已结束，才归还。
            if (f.db_credit_token && !g_coroutine_suspended)
                f.db_credit_token->release();
            g_current_task_db_credit_token.reset();
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
            finish_task();
        }
    }

push_result thread_pool::add_task(work_task f) {
        {
            std::lock_guard<std::mutex> lock(idle_mutex_);
            ++unfinished_task_count_;
        }

        push_result r = tasks_.push(std::move(f));
        if (r != push_result::Ok) {
            std::lock_guard<std::mutex> lock(idle_mutex_);
            --unfinished_task_count_;
            return r;
        }
        if (metrics_) metrics_->on_task_enqueued(PoolId::Work);
        return push_result::Ok;
    }

void thread_pool::shutdown() {
    tasks_.close();
}

bool thread_pool::wait_idle(const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(idle_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this]{ return unfinished_task_count_ == 0; });
}

void thread_pool::finish_task() {
    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        if (unfinished_task_count_ > 0)
            --unfinished_task_count_;
    }
    idle_cv_.notify_all();
}
