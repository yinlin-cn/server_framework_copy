#include "work_pool.h"
#include "thread_context.h"

using namespace std;

blockingqueue& work_pool::get_queue() { return queue_; }

void work_pool::add_task(function<void()> task) {
    auto conn = tls_current_conn;          // 提交线程当前连接
    pool.add_task(work_task{
        [task] {
            task();                        // 白板已由 worker 设置好
        },
        conn
    });
}

void work_pool::add_task(work_task task) {
    pool.add_task(move(task));
}

void work_pool::on_event(uint64_t key) {
    auto ts = queue_.take(key);
    for (auto& t : ts)
        pool.add_task(move(t.funtion));
}

void work_pool::add_blockingtask(blockedtask b) {
    queue_.insert(move(b));
}