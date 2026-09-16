#include "Handler_batch_make.h"
#include "BatchSender.h"

Handler_batch_make::Handler_batch_make(BatchSender* sender) : sender_(sender) {}

void Handler_batch_make::on_need_send(Reactor* reactor) {
    sender_->mark_reactor(reactor);
}

Handler_batch_Factory_make::Handler_batch_Factory_make(BatchSender* sender)
    : sender_(sender) {}

Handler_batch* Handler_batch_Factory_make::create_handler() {
    return new Handler_batch_make(sender_);
}
