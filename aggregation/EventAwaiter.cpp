#include"EventAwaiter.h"

using namespace std;

bool EventAwaiter::await_ready() { return false; }
void EventAwaiter::await_suspend(coroutine_handle<> h) {
        handle = h;
        queue->insert(blockedtask{
            wait_key,
            work_task{ [this]{ handle.resume(); }, tls_current_conn }
        });

        if (db_handler && box) {
            box->wait_name = wait_key;
            db_handler->submit(wait_key, box, message);
        }
    }
string EventAwaiter::await_resume() { return box ? box->result : message; }