#pragma once
#include "Handler_batch.h"

class BatchSender;

// make 实现：只负责把 Reactor 的通知转发给 BatchSender。
class Handler_batch_make : public Handler_batch {
public:
    explicit Handler_batch_make(BatchSender* sender);
    void on_need_send(Reactor* reactor) override;

private:
    BatchSender* sender_;
};

// make 工厂实现：创建时注入 BatchSender。
class Handler_batch_Factory_make : public Handler_batch_Factory {
public:
    explicit Handler_batch_Factory_make(BatchSender* sender);
    Handler_batch* create_handler() override;

private:
    BatchSender* sender_;
};
