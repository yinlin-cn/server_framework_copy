#pragma once
#include <array>
#include <atomic>
#include <thread>
#include "Handler_metrics.h"
#include "MetricsConfig.h"

class Handler_log;

// 指标实现：模块埋点 + 原子计数器 + 独立采样线程。
class Metrics : public Handler_metrics {
public:
    static constexpr int BUCKET_COUNT = 8;
    static constexpr int POOL_COUNT = static_cast<int>(PoolId::Count);
    static constexpr int STAGE_COUNT = static_cast<int>(ErrorStage::Count);
    static constexpr int WINDOW_SIZE = 60;   // 最近 60 秒滚动平均窗口

    Metrics(const MetricsConfig& cfg, Handler_log* log);
    ~Metrics();

    void start_sampler(int interval_ms = 1000);
    void stop_sampler();

    // 埋点实现
    void on_request_started() override;
    void on_request_done(uint64_t latency_us) override;
    void on_module_task_done(PoolId pool, uint64_t latency_us) override;
    void on_error(ErrorStage stage) override;
    void on_conn_open() override;
    void on_conn_close() override;
    void on_task_enqueued(PoolId pool) override;
    void on_task_dequeued(PoolId pool) override;

    static uint64_t now_us();   // 模块埋点计时用

private:
    MetricsConfig cfg_;
    Handler_log* log_;
    int interval_ms_ = 1000;
    std::atomic<bool> running_{false};
    std::thread sampler_thread_;

    // 请求 / 延迟
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> total_latency_us_{0};
    std::atomic<int64_t> inflight_{0};
    std::array<std::atomic<uint64_t>, BUCKET_COUNT> lat_buckets_{};

    // 模块级任务统计（divide / work / db 各自）
    std::array<std::atomic<uint64_t>, POOL_COUNT> module_done_{};
    std::array<std::atomic<uint64_t>, POOL_COUNT> module_latency_us_{};
    std::array<std::array<std::atomic<uint64_t>, BUCKET_COUNT>, POOL_COUNT> module_buckets_{};

    // 错误（按阶段）
    std::array<std::atomic<uint64_t>, STAGE_COUNT> errors_{};

    // 连接 / 队列（队列按池）
    std::atomic<int> conns_{0};
    std::array<std::atomic<int64_t>, POOL_COUNT> queue_depth_{};

    // 窗口采样需要的"上次"值
    uint64_t last_requests_ = 0;
    uint64_t last_latency_us_ = 0;
    std::array<uint64_t, STAGE_COUNT> last_errors_{};
    std::array<uint64_t, BUCKET_COUNT> last_buckets_{};
    std::array<uint64_t, POOL_COUNT> last_module_done_{};
    std::array<uint64_t, POOL_COUNT> last_module_latency_us_{};
    std::array<std::array<uint64_t, BUCKET_COUNT>, POOL_COUNT> last_module_buckets_{};

    // 系统指标
    uint64_t last_cpu_ns_ = 0;

    // 最近 N 秒业务请求数（环形缓冲，用于滚动平均）
    std::array<uint64_t, WINDOW_SIZE> recent_req_qps_{};
    int window_index_ = 0;
    int window_count_ = 0;

    void snapshot();
    static int bucket_index(uint64_t us);
    uint64_t calc_window_p99();
    uint64_t calc_module_p99(int pool);
    void read_system(uint64_t& cpu_percent, uint64_t& rss_kb);
};
