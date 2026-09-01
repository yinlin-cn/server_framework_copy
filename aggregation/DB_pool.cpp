#include "DB_pool.h"
#include "Metrics.h"
#include <cstring>
#include <vector>

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
            MYSQL_STMT* stmt = mysql_stmt_init(conn);
            if (!stmt) {
                job.box->err = "stmt init failed";
            } else if (mysql_stmt_prepare(stmt, job.sql.c_str(), job.sql.size()) != 0) {
                job.box->err = mysql_stmt_error(stmt);
                if (log_) log_->error("db prepare failed: " + job.box->err);
                mysql_stmt_close(stmt);
            } else {
                // 绑定参数：全部按字符串处理，MySQL 自动转换
                std::vector<MYSQL_BIND> binds(job.params.size());
                std::vector<std::string> data = job.params;   // 保证 buffer 存活到执行结束
                for (size_t i = 0; i < job.params.size(); i++) {
                    binds[i].buffer_type = MYSQL_TYPE_STRING;
                    binds[i].buffer = (void*)data[i].data();
                    binds[i].buffer_length = data[i].size();
                }
                if (mysql_stmt_bind_param(stmt, binds.data()) != 0) {
                    job.box->err = mysql_stmt_error(stmt);
                    if (log_) log_->error("db bind failed: " + job.box->err);
                } else if (mysql_stmt_execute(stmt) != 0) {
                    job.box->err = mysql_stmt_error(stmt);
                    if (log_) log_->error("db execute failed: " + job.box->err);
                } else {
                    MYSQL_RES* res = mysql_stmt_result_metadata(stmt);
                    if (res) {
                        unsigned int cols = mysql_num_fields(res);
                        std::vector<MYSQL_BIND> rb(cols);
                        std::vector<std::vector<char>> bufs(
                            cols, std::vector<char>(256));
                        std::vector<unsigned long> lengths(cols);
                        std::vector<my_bool> is_null(cols);
                        for (unsigned int c = 0; c < cols; c++) {
                            rb[c].buffer_type = MYSQL_TYPE_STRING;
                            rb[c].buffer = bufs[c].data();
                            rb[c].buffer_length = bufs[c].size();
                            rb[c].length = &lengths[c];
                            rb[c].is_null = &is_null[c];
                        }
                        mysql_stmt_store_result(stmt);
                        mysql_stmt_bind_result(stmt, rb.data());
                        while (mysql_stmt_fetch(stmt) == 0) {
                            std::vector<std::string> line;
                            for (unsigned int c = 0; c < cols; c++)
                                line.push_back(std::string(bufs[c].data(), lengths[c]));
                            job.box->rows.push_back(std::move(line));
                        }
                        if (!job.box->rows.empty() && !job.box->rows[0].empty())
                            job.box->result = job.box->rows[0][0];
                        mysql_free_result(res);
                    } else {
                        job.box->result = "OK";               // 非 SELECT
                    }
                }
                mysql_stmt_close(stmt);
            }
        } catch (const std::exception& e) {
            job.box->err = e.what();
            if (metrics_) metrics_->on_error(ErrorStage::DB);
        } catch (...) {
            job.box->err = "unknown";
            if (metrics_) metrics_->on_error(ErrorStage::DB);
        }
        if (metrics_) metrics_->on_module_task_done(PoolId::DB, Metrics::now_us() - db_start_us);
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
