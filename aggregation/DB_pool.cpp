#include "DB_pool.h"

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
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) t.join();
}

void DB_pool::submit(DBTask task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
    }
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

        DBHandle conn = conn_pool_.get();            // 真实版是 MYSQL*
        if (!conn) continue;

        if (mysql_query(conn, job.sql.c_str()) == 0) {
            MYSQL_RES* res = mysql_store_result(conn);
            if (res) {
                MYSQL_ROW row = mysql_fetch_row(res);
                if (row && row[0])
                    job.box->result = row[0];        // 原型先取第一行第一列
                mysql_free_result(res);
            } else {
                job.box->result = "OK";              // 非 SELECT
            }
        } else {
            job.box->result = std::string("ERR: ") + mysql_error(conn);
        }
        job.box->ready = true;
        conn_pool_.release(conn);

        uint64_t key = job.wait_name;
        auto* pool = business_pool_;
        pool->add_task([pool, key]{ pool->on_event(key); });
    }
}