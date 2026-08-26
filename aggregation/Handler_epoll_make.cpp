#include"Handler_epoll_make.h"
using namespace std;
void Handler_epoll_make::on_message(std::shared_ptr<Internalconnection> conn,
                                    const std::string& msg) {
    auto parse = [this, msg]() -> std::function<void()> {
        return divide_work(msg);
    };
    divide_pool_->add_task(divide_task{parse, conn, divide_handler_});
}

Handler_epoll* Handler_epoll_Factory_make::create_handler() {
    return new Handler_epoll_make(divide_work, divide_pool_, divide_handler_);
}