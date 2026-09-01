#include"thread_pool.h"
#include "Metrics.h"

using namespace std;
thread_pool::thread_pool(int N) {
        for (int i = 0; i < N; i++)
            pool.emplace_back(&thread_pool::worker, this);
    }
thread_pool::~thread_pool() {
        { lock_guard<mutex> lock(for_task); stop = true; }
        cv.notify_all();
        for (auto& t : pool)
            if (t.joinable()) t.join();
    }

void thread_pool::worker() {
        while (true) {
            work_task f;
            {
                unique_lock<mutex> lock(for_task);
                cv.wait(lock, [this]{ return stop || !tasks.empty(); });
                if (stop && tasks.empty()) return;
                f = move(tasks.front());
                tasks.pop();
            }
            tls_current_conn = f.conn;
            if (metrics_) metrics_->on_task_dequeued(PoolId::Work);
            bool business = f.is_business;   // 业务请求任务才计入请求级 QPS
            if (metrics_ && business) metrics_->on_request_started();
            uint64_t start_us = Metrics::now_us();
            active_++;            // 取到任务后
            try {
                f.fn();
            } catch (const std::exception& e) {
                if (error_handler_) error_handler_(f.conn, "work", e.what());
                if (metrics_ && business) metrics_->on_error(ErrorStage::Work);
            } catch (...) {
                if (error_handler_) error_handler_(f.conn, "work", "unknown error");
                if (metrics_ && business) metrics_->on_error(ErrorStage::Work);
            }
            uint64_t done_us = Metrics::now_us() - start_us;
            if (metrics_) {
                metrics_->on_work_task_done(done_us);   // 框架任务级
                if (business) metrics_->on_request_done(done_us);   // 业务请求级
            }
            tls_current_conn.reset();
            active_--;            // 跑完（无论成败）
            if (active_ == 0) idle_cv_.notify_all();
        }
    }

void thread_pool::add_task(work_task f) {
        lock_guard<mutex> lock(for_task);
        tasks.push(move(f));
        if (metrics_) metrics_->on_task_enqueued(PoolId::Work);
        cv.notify_one();
    }

void thread_pool::shutdown() {
    { lock_guard<mutex> lock(for_task); stop = true; }
    cv.notify_all();
}

bool thread_pool::wait_idle(const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(idle_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this]{ return active_ == 0; });
}
