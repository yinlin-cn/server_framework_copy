#include"Handler_epoll_make.h"
#include "backpressure.h"
using namespace std;

void Handler_epoll_make::on_connect(std::shared_ptr<Internalconnection> conn) {
    if (book_) book_->on_connection(conn);
}

void Handler_epoll_make::on_disconnect(std::shared_ptr<Internalconnection> conn) {
    if (book_) book_->dis_connection(conn);
}

push_result Handler_epoll_make::on_message(
    std::shared_ptr<Internalconnection> conn, const std::string& msg) {
    work_type message_type = route_ ? route_->classify(msg) : work_type::Fast;

    std::shared_ptr<DB_credit_token> credit;
    if (message_type == work_type::DB && db_credit_gate_) {
        if (!db_credit_gate_->try_acquire()) {
            // DB 额度不足：消息先进等待队列，暂停这条连接的读取。
            if (!db_waiting_queue_ || !db_waiting_queue_->add_waiting_task(conn, msg))
                return push_result::Full;   // 无法登记时保留消息，等待重试
            if (reactor_control_)
                reactor_control_->pause_reading(conn);
            return push_result::Ok;
        }
        // 额度随整个业务流程走，由业务 worker 在流程结束时归还。
        credit = std::make_shared<DB_credit_token>(db_credit_gate_);
    }

    auto parse = [this, msg]() -> std::function<void()> {
        return divide_work(msg);
    };
    push_result r = divide_pool_->try_add_task(
        divide_task{parse, conn, divide_handler_, credit});

    // Full 时不阻塞 Reactor：消息留在 read_buffer，由低水位回调触发重试。
    if (r == push_result::Full)
        return r;
    if (r == push_result::Closed && credit)
        credit->release();   // 池已关闭，任务没投进去，额度立即还回
    return r;
}

Handler_epoll* Handler_epoll_Factory_make::create_handler() {
    return new Handler_epoll_make(
        divide_work, divide_pool_, divide_handler_, book_,
        route_, db_credit_gate_, db_waiting_queue_, reactor_control_);
}
