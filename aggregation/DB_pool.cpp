#include "DB_pool.h"
#include "Metrics.h"
#include "backpressure.h"
#include <cstring>
#include <vector>

DB_pool::DB_pool(int conns, int workers, work_pool* business_pool,
                 const std::string& host, const std::string& user,
                 const std::string& password, const std::string& database,
                 unsigned int port)
    : conn_pool_(conns, host, user, password, database, port),
      business_pool_(business_pool), worker_count_(workers),
      tasks_(static_cast<size_t>(workers * 32)) {
    for (int i = 0; i < worker_count_; i++)
        workers_.emplace_back(&DB_pool::worker_loop, this);
}

DB_pool::~DB_pool() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true))
        return;
    conn_pool_.shutdown();                 // 先唤醒卡在 get() 的 worker，避免 join 死锁
    tasks_.close();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}

void DB_pool::submit(DBTask task) {
    tasks_.push(std::move(task));
    if (metrics_) metrics_->on_task_enqueued(PoolId::DB);
}

void DB_pool::worker_loop() {
    while (true) {
        DBTask job;
        if (!tasks_.pop(job))
            return;
        if (metrics_) metrics_->on_task_dequeued(PoolId::DB);
        db_active_++;

        DBHandle conn = conn_pool_.get();            // 真实版是 MYSQL*
        if (!conn) {
            db_active_--;
            if (stopped_.load()) return;   // 停止中且拿不到连接，直接退出
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
        db_active_--;
        if (db_gate_) db_gate_->release();   // 释放一个 DB 准入额度，唤醒等待者

        // 等待发起协程的业务任务返回后再 resume，避免同一协程被两个线程访问。
        if (job.box && job.box->wake_guard) {
            auto guard = job.box->wake_guard;
            int spins = 0;
            while (guard->load() && spins++ < 1000000)
                std::this_thread::yield();
        }

        uint64_t key = job.wait_name;
        auto* pool = business_pool_;
        pool->add_task([pool, key]{ pool->on_event(key); });
    }
}

void DB_pool::shutdown() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true))
        return;
    conn_pool_.shutdown();                 // 唤醒卡在 get() 的 worker
    tasks_.close();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}
