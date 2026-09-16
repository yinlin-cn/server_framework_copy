#pragma once

// 指标配置：关闭的指标连计数器都不维护。
struct MetricsConfig {
    bool qps = true;          // QPS
    bool latency = true;      // 平均延迟
    bool p99 = true;          // P99（窗口直方图）
    bool errors = true;       // 错误率（按阶段）
    bool queue_depth = true;  // 队列深度（按池）
    bool conns = true;        // 连接数
    bool db_latency = true;   // DB 查询延迟
    bool system = true;       // CPU / 内存
};
