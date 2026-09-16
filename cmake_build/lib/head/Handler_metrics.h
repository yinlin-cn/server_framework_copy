#pragma once
#include <cstdint>

// 池类型：divide_pool / work_pool / DB_pool。
// 新增池时插在 Count 前面，Count 自动等于个数。
enum class PoolId : int {
    Divide = 0,
    Work = 1,
    DB = 2,
    Count = 3,
};

// 错误阶段：定位错误出在解析 / 业务 / DB。
enum class ErrorStage : int {
    Divide = 0,
    Work = 1,
    DB = 2,
    Count = 3,
};

// 指标埋点接口：所有模块只依赖它，不接触 Metrics 实现。
class Handler_metrics {
public:
    virtual ~Handler_metrics() = default;

    virtual void on_request_started() = 0;                  // 业务任务开始
    virtual void on_request_done(uint64_t latency_us) = 0;  // 业务任务完成
    virtual void on_module_task_done(PoolId pool, uint64_t latency_us) = 0;  // 模块任务完成（divide/work/db 各自统计）
    virtual void on_error(ErrorStage stage) = 0;            // 出错（分阶段）
    virtual void on_conn_open() = 0;                        // 连接建立
    virtual void on_conn_close() = 0;                       // 连接断开
    virtual void on_task_enqueued(PoolId pool) = 0;         // 某池入队
    virtual void on_task_dequeued(PoolId pool) = 0;         // 某池出队
};
