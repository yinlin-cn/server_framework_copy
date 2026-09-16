#include "DB_pool.h"
#include "Metrics.h"
#include <cstring>
#include <vector>

namespace {

// 读取 prepared statement 的完整结果集。列缓冲从 256 字节起步，
// 遇到 MYSQL_DATA_TRUNCATED 时按实际长度扩容，再取回完整列数据。
bool read_all_rows(MYSQL_STMT* stmt, MYSQL_RES* res,
                   std::vector<std::vector<std::string>>& rows,
                   std::string& err) {
    const unsigned int cols = mysql_num_fields(res);
    if (cols == 0)
        return true;

    if (mysql_stmt_store_result(stmt) != 0) {
        err = mysql_stmt_error(stmt);
        return false;
    }

    constexpr size_t init_column_bytes = 256;
    std::vector<MYSQL_BIND> rb(cols);
    std::vector<std::vector<char>> bufs(cols);
    std::vector<unsigned long> lengths(cols, 0);
    std::vector<my_bool> is_null(cols, 0);

    for (unsigned int c = 0; c < cols; c++) {
        bufs[c].assign(init_column_bytes, '\0');
        rb[c].buffer_type = MYSQL_TYPE_STRING;
        rb[c].buffer = bufs[c].data();
        rb[c].buffer_length = bufs[c].size();
        rb[c].length = &lengths[c];
        rb[c].is_null = &is_null[c];
    }
    if (mysql_stmt_bind_result(stmt, rb.data()) != 0) {
        err = mysql_stmt_error(stmt);
        return false;
    }

    while (true) {
        int rc = mysql_stmt_fetch(stmt);
        if (rc == MYSQL_NO_DATA)
            break;
        if (rc != 0 && rc != MYSQL_DATA_TRUNCATED) {
            err = mysql_stmt_error(stmt);
            if (err.empty()) err = "mysql_stmt_fetch failed";
            return false;
        }

        if (rc == MYSQL_DATA_TRUNCATED) {
            std::vector<unsigned char> refetch(cols, 0);
            for (unsigned int c = 0; c < cols; c++) {
                if (is_null[c])
                    continue;
                unsigned long need = lengths[c];
                if (need >= bufs[c].size()) {
                    bufs[c].resize(need + 1);
                    rb[c].buffer = bufs[c].data();
                    rb[c].buffer_length = static_cast<unsigned long>(bufs[c].size());
                    refetch[c] = 1;
                }
            }
            for (unsigned int c = 0; c < cols; c++) {
                if (!refetch[c])
                    continue;
                if (mysql_stmt_fetch_column(stmt, &rb[c], c, 0) != 0) {
                    err = mysql_stmt_error(stmt);
                    return false;
                }
            }
        }

        std::vector<std::string> line;
        line.reserve(cols);
        for (unsigned int c = 0; c < cols; c++)
            line.push_back(std::string(bufs[c].data(), lengths[c]));
        rows.push_back(std::move(line));
    }
    return true;
}

}  // namespace

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
    shutdown();
}

push_result DB_pool::submit(DBTask task) {
    record_task_enqueued();
    push_result result = tasks_.push(std::move(task));
    if (result != push_result::Ok) {
        finish_task();
        return result;
    }
    if (metrics_) metrics_->on_task_enqueued(PoolId::DB);
    return push_result::Ok;
}

void DB_pool::record_task_enqueued() {
    std::lock_guard<std::mutex> lock(idle_mutex_);
    ++unfinished_task_count_;
}

void DB_pool::finish_task() {
    {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        if (unfinished_task_count_ > 0)
            --unfinished_task_count_;
    }
    idle_cv_.notify_all();
}

void DB_pool::worker_loop() {
    while (true) {
        DBTask job;
        if (!tasks_.pop(job))
            return;
        if (metrics_) metrics_->on_task_dequeued(PoolId::DB);
        active_query_count_++;

        DBHandle conn = conn_pool_.get();            // 真实版是 MYSQL*
        if (!conn) {
            if (job.box) {
                job.box->err = "database pool stopped";
                job.box->ready = true;
            }
            if (business_pool_ && job.box) {
                auto* pool = business_pool_;
                push_result result = pool->add_task(
                    [pool, key = job.wait_name] { pool->on_event(key); });
                if (result == push_result::Closed && log_)
                    log_->error("db resume drop: work pool closed");
            }
            active_query_count_--;
            finish_task();
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
                        if (!read_all_rows(stmt, res, job.box->rows, job.box->err))
                            if (log_) log_->error("db fetch failed: " + job.box->err);
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
        active_query_count_--;

        // 等待发起协程的业务任务返回后再 resume，避免同一协程被两个线程访问。
        if (job.box && job.box->suspend_guard)
            job.box->suspend_guard->wait_finished();

        uint64_t key = job.wait_name;
        auto* pool = business_pool_;
        push_result result = pool->add_task([pool, key]{ pool->on_event(key); });
        if (result == push_result::Closed && log_)
            log_->error("db resume drop: work pool closed");
        finish_task();
    }
}

void DB_pool::shutdown() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true))
        return;
    tasks_.close();
    conn_pool_.shutdown();                 // 唤醒可能卡在 get() 的 worker
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}

bool DB_pool::wait_idle(const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(idle_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this] {
        return unfinished_task_count_ == 0;
    });
}
