#pragma once
#include <memory>
#include <string>
#include "bounded_task_queue.h"
#include "Internalconnection.h"

using namespace std;
class Handler_epoll {
public:
    virtual ~Handler_epoll() = default;
    virtual PushResult on_message(std::shared_ptr<Internalconnection> conn,
                                  const std::string& msg) = 0;
    virtual void on_connect(std::shared_ptr<Internalconnection> conn) {}
    virtual void on_disconnect(std::shared_ptr<Internalconnection> conn) {}
};

class Handler_epoll_Factory {
public:
    virtual Handler_epoll* create_handler() = 0;
};
