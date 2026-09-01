#include "Metrics.h"
#include "Handler_log.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <thread>

Metrics::Metrics(const MetricsConfig& cfg, Handler_log* log)
    : cfg_(cfg), log_(log) {
    for (auto& b : lat_buckets_) b = 0;
    for (auto& e : errors_) e = 0;
    for (auto& q : queue_depth_) q = 0;
}

Metrics::~Metrics() {
    stop_sampler();
}

uint64_t Metrics::now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// ============ 埋点：只做原子累加 ============

void Metrics::on_request_started() {
    inflight_++;
}

void Metrics::on_request_done(uint64_t latency_us) {
    if (cfg_.qps)     total_requests_++;
    if (cfg_.latency) total_latency_us_ += latency_us;
    if (cfg_.p99)     lat_buckets_[bucket_index(latency_us)]++;
    inflight_--;
}

void Metrics::on_work_task_done(uint64_t latency_us) {
    if (cfg_.qps)     total_work_tasks_++;
    if (cfg_.latency) total_work_latency_us_ += latency_us;
}

void Metrics::on_error(ErrorStage stage) {
    if (cfg_.errors)
        errors_[static_cast<int>(stage)]++;
}

void Metrics::on_conn_open()  { if (cfg_.conns) conns_++; }
void Metrics::on_conn_close() { if (cfg_.conns) conns_--; }

void Metrics::on_task_enqueued(PoolId pool) {
    if (cfg_.queue_depth)
        queue_depth_[static_cast<int>(pool)]++;
}

void Metrics::on_task_dequeued(PoolId pool) {
    if (cfg_.queue_depth)
        queue_depth_[static_cast<int>(pool)]--;
}

void Metrics::on_db_query_done(uint64_t latency_us) {
    if (cfg_.db_latency) {
        db_query_count_++;
        total_db_latency_us_ += latency_us;
    }
}

// ============ 直方图 / P99 ============

int Metrics::bucket_index(uint64_t us) {
    if (us < 1000) return 0;       // <1ms
    if (us < 5000) return 1;       // <5ms
    if (us < 10000) return 2;      // <10ms
    if (us < 50000) return 3;      // <50ms
    if (us < 100000) return 4;     // <100ms
    if (us < 500000) return 5;     // <500ms
    if (us < 1000000) return 6;    // <1s
    return 7;                      // >=1s
}

uint64_t Metrics::calc_window_p99() {
    static const uint64_t upper[BUCKET_COUNT] = {
        1000, 5000, 10000, 50000, 100000, 500000, 1000000, 1000000,
    };

    uint64_t total = 0;
    for (int i = 0; i < BUCKET_COUNT; i++)
        total += lat_buckets_[i].load() - last_buckets_[i];   // 窗口差值
    if (total == 0) return 0;

    uint64_t target = total * 99 / 100;
    uint64_t acc = 0;
    for (int i = 0; i < BUCKET_COUNT; i++) {
        acc += lat_buckets_[i].load() - last_buckets_[i];
        if (acc >= target) return upper[i];
    }
    return upper[BUCKET_COUNT - 1];
}

// ============ 系统指标（Linux） ============

void Metrics::read_system(uint64_t& cpu_percent, uint64_t& rss_kb) {
    // RSS：读 /proc/self/statm，第 2 个字段是 resident pages。
    rss_kb = 0;
    std::ifstream statm("/proc/self/statm");
    if (statm) {
        uint64_t size = 0, resident = 0;
        statm >> size >> resident;
        rss_kb = resident * 4;   // 按 4KB 页估算
    }

    // CPU：进程 CPU 时间差值 / 墙钟窗口。
    timespec ts{};
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    uint64_t now_cpu_ns =
        static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;

    if (last_cpu_ns_ == 0) {
        cpu_percent = 0;
    } else {
        double interval_s = interval_ms_ / 1000.0;
        double cpu_used_s = (now_cpu_ns - last_cpu_ns_) / 1e9;
        cpu_percent = static_cast<uint64_t>(cpu_used_s / interval_s * 100.0);
    }
    last_cpu_ns_ = now_cpu_ns;
}

// ============ 快照 ============

void Metrics::snapshot() {
    auto req = total_requests_.load();
    auto lat = total_latency_us_.load();
    auto wt = total_work_tasks_.load();
    auto wl = total_work_latency_us_.load();
    auto db_count = db_query_count_.load();
    auto db_lat = total_db_latency_us_.load();

    double req_qps = (req - last_requests_) / (double)(interval_ms_ / 1000.0);
    double req_avg = (req > last_requests_)
        ? (lat - last_latency_us_) / (double)(req - last_requests_)
        : 0;

    // 滚动窗口：记录最近 60 秒的每秒请求数，算平均。
    recent_req_qps_[window_index_] = static_cast<uint64_t>(req_qps);
    window_index_ = (window_index_ + 1) % WINDOW_SIZE;
    if (window_count_ < WINDOW_SIZE) window_count_++;
    uint64_t sum60 = 0;
    for (int i = 0; i < window_count_; i++)
        sum60 += recent_req_qps_[i];
    double req_avg60 = window_count_ ? (double)sum60 / window_count_ : 0;

    double task_qps = (wt - last_work_tasks_) / (double)(interval_ms_ / 1000.0);
    double task_avg = (wt > last_work_tasks_)
        ? (wl - last_work_latency_us_) / (double)(wt - last_work_tasks_)
        : 0;

    uint64_t cpu = 0, rss = 0;
    if (cfg_.system) read_system(cpu, rss);

    if (log_) {
        std::string msg = "req_qps=" + std::to_string(static_cast<int>(req_qps))
            + " req_avg60=" + std::to_string(static_cast<int>(req_avg60))
            + " req_avg=" + std::to_string(req_avg / 1000.0) + "ms"
            + " task_qps=" + std::to_string(static_cast<int>(task_qps))
            + " task_avg=" + std::to_string(task_avg / 1000.0) + "ms"
            + " p99=" + std::to_string(calc_window_p99() / 1000.0) + "ms"
            + " inflight=" + std::to_string(inflight_.load())
            + " conns=" + std::to_string(conns_.load());

        if (cfg_.queue_depth)
            msg += " queue(d/w/db)=" + std::to_string(queue_depth_[0].load())
                 + "/" + std::to_string(queue_depth_[1].load())
                 + "/" + std::to_string(queue_depth_[2].load());

        if (cfg_.errors)
            msg += " err(d/w/db)=" + std::to_string(errors_[0].load())
                 + "/" + std::to_string(errors_[1].load())
                 + "/" + std::to_string(errors_[2].load());

        if (cfg_.db_latency && db_count > last_db_count_)
            msg += " db_avg=" + std::to_string(
                (db_lat - last_db_latency_us_) /
                (double)(db_count - last_db_count_) / 1000.0) + "ms";

        if (cfg_.system)
            msg += " cpu=" + std::to_string(cpu) + "% rss="
                 + std::to_string(rss) + "KB";

        log_->info(msg);
    }

    last_requests_ = req;
    last_latency_us_ = lat;
    last_work_tasks_ = wt;
    last_work_latency_us_ = wl;
    for (int i = 0; i < STAGE_COUNT; i++)
        last_errors_[i] = errors_[i].load();
    for (int i = 0; i < BUCKET_COUNT; i++)
        last_buckets_[i] = lat_buckets_[i].load();
    last_db_count_ = db_count;
    last_db_latency_us_ = db_lat;
}

// ============ 采样线程 ============

void Metrics::start_sampler(int interval_ms) {
    interval_ms_ = interval_ms;
    running_ = true;
    sampler_thread_ = std::thread([this] {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
            snapshot();
        }
    });
}

void Metrics::stop_sampler() {
    running_ = false;
    if (sampler_thread_.joinable()) sampler_thread_.join();
}
