#include"EventAwaiter.h"
#include "EventTask.h"
#include "backpressure.h"

using namespace std;

thread_local bool g_coroutine_suspended = false;
thread_local std::shared_ptr<coroutine_suspend_guard> g_current_task_suspend_guard;
thread_local std::shared_ptr<DB_credit_token> g_current_task_db_credit_token;

bool EventAwaiter::await_ready() { return box && box->cancelled; }
void EventAwaiter::await_suspend(coroutine_handle<> h) {
        handle = h;
        g_coroutine_suspended = true;   // 本任务挂起，业务池先不归还窗口
        if (box)
            box->suspend_guard = g_current_task_suspend_guard;   // 防止 DB 提前 resume
        work_task resume{ [this]{ handle.resume(); }, tls_current_conn };
        resume.db_credit_token = g_current_task_db_credit_token;   // 业务流程的额度跟着 resume 走
        queue->insert(blockedtask{
        wait_key,
        std::move(resume),
        box
        });

        if (db_handler && box) {
            box->wait_name = wait_key;
            db_handler->submit(wait_key, box, message, params);
        }
    }
DBResult EventAwaiter::await_resume() {
    if (box && box->cancelled)
        return {false, true, "cancelled", "", {}};    // 取消
    if (box && !box->err.empty())
        return {false, false, box->err, "", {}};      // 错误
    if (box)
        return {true, false, "", box->result, box->rows};   // 成功，带完整结果
    return {false, false, "no box", "", {}};          // 理论不可达，兜底
    }
