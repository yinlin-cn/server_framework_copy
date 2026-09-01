#include "DB_pool.h"
#include "Metrics.h"

DB_pool::DB_pool(int conns, int workers, work_pool* business_pool,
                 const std::string& host, const std::string& user,
                 const std::string& password, const std::string& database,
                 unsigned int port)
    : conn_pool_(conns, host, user, password, database, port),
      business_pool_(business_pool), worker_count_(workers) {
    for (int i = 0; i < worker_count_; i++)
        workers_.emplace_back(&DB_pool::worker_loop, this);
}

DB_pool::~DB_pool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_) return;              // 已停过就直接返回，避免二次 join
        stopped_ = true;
    }
    conn_pool_.shutdown();                 // 先唤醒卡在 get() 的 worker，避免 join 死锁
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}

void DB_pool::submit(DBTask task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
    }
    if (metrics_) metrics_->on_task_enqueued(PoolId::DB);
    cv_.notify_one();
}

void DB_pool::worker_loop() {
    while (true) {
        DBTask job;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]{ return stop_ || !tasks_.empty(); });
            if (stop_ && tasks_.empty()) return;
            job = std::move(tasks_.front());
            tasks_.pop();
        }
        if (metrics_) metrics_->on_task_dequeued(PoolId::DB);

        DBHandle conn = conn_pool_.get();            // 真实版是 MYSQL*
        if (!conn) {
            if (stop_) return;   // 停止中且拿不到连接，直接退出，避免空转
            continue;
        }

        uint64_t db_start_us = Metrics::now_us();
        try {
            if (mysql_query(conn, job.sql.c_str()) == 0) {
                MYSQL_RES* res = mysql_store_result(conn);
                if (res) {
                    MYSQL_ROW row = mysql_fetch_row(res);
                    if (row && row[0])
                        job.box->result = row[0];             // 正常结果
                    mysql_free_result(res);
                } else {
                    job.box->result = "OK";                   // 非 SELECT
                }
            } else {
                job.box->err = mysql_error(conn);             // 错误单独写 err
            }
        } catch (const std::exception& e) {
            job.box->err = e.what();
            if (metrics_) metrics_->on_error(ErrorStage::DB);
        } catch (...) {
            job.box->err = "unknown";
            if (metrics_) metrics_->on_error(ErrorStage::DB);
        }
        if (metrics_) metrics_->on_db_query_done(Metrics::now_us() - db_start_us);
        job.box->ready = true;
        conn_pool_.release(conn);

        uint64_t key = job.wait_name;
        auto* pool = business_pool_;
        pool->add_task([pool, key]{ pool->on_event(key); });
    }
}

void DB_pool::shutdown() {
     {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_) return;
        stopped_ = true;
    }
    conn_pool_.shutdown();                 // 唤醒卡在 get() 的 worker
    { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
    cv_.notify_all();
        for (auto& t : workers_)
        if (t.joinable()) t.join();
}
