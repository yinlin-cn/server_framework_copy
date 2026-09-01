#include"EventAwaiter.h"

using namespace std;

bool EventAwaiter::await_ready() { return box && box->cancelled; }
void EventAwaiter::await_suspend(coroutine_handle<> h) {
        handle = h;
        queue->insert(blockedtask{
        wait_key,
        work_task{ [this]{ handle.resume(); }, tls_current_conn },
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
