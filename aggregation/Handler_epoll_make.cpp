#include"Handler_epoll_make.h"
#include "backpressure.h"
using namespace std;

void Handler_epoll_make::on_connect(std::shared_ptr<Internalconnection> conn) {
    if (book_) book_->on_connection(conn);
}

void Handler_epoll_make::on_disconnect(std::shared_ptr<Internalconnection> conn) {
    if (book_) book_->dis_connection(conn);
}

PushResult Handler_epoll_make::on_message(
    std::shared_ptr<Internalconnection> conn, const std::string& msg) {
    WorkClass cls = route_ ? route_->classify(msg) : WorkClass::Fast;

    if (cls == WorkClass::Db && db_gate_ && !db_gate_->try_acquire()) {
        // DB 额度不足：消息先进等待队列，暂停这条连接的读取。
        if (waiting_)
            waiting_->add(conn, msg);
        if (reactor_control_)
            reactor_control_->pause_reading(conn);
        return PushResult::Ok;
    }

    auto parse = [this, msg]() -> std::function<void()> {
        return divide_work(msg);
    };
    PushResult r = divide_pool_->try_add_task(
        divide_task{parse, conn, divide_handler_});

    // Full 时退回阻塞投递：不丢消息，当前线程等 divide 空位。
    if (r == PushResult::Full) {
        divide_pool_->add_task(
            divide_task{parse, conn, divide_handler_});
        return PushResult::Ok;
    }
    return r;
}

Handler_epoll* Handler_epoll_Factory_make::create_handler() {
    return new Handler_epoll_make(
        divide_work, divide_pool_, divide_handler_, book_,
        route_, db_gate_, waiting_, reactor_control_);
}
