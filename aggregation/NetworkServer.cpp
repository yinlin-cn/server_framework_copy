#include "NetworkServer.h"

NetworkServer::NetworkServer(int port, int reactor_count, int max_events,
                             int backlog, Handler_epoll_Factory* factory)
    : port_(port), reactor_count_(reactor_count),
      max_events_(max_events), backlog_(backlog), factory_(factory) {}

NetworkServer::~NetworkServer() {
    stop();
}

bool NetworkServer::start() {
    if (started_) return false;

    for (int i = 0; i < reactor_count_; i++) {
        auto r = std::make_shared<Reactor>(max_events_, factory_);
        r->set_batch_handler(batch_handler_);
        r->set_metrics(metrics_);
        r->start();
        reactors_.push_back(r);
    }

    acceptor_ = std::make_unique<Acceptor>(
        port_, reactors_, factory_, backlog_);
    acceptor_->start();

    started_ = true;
    return true;
}

void NetworkServer::set_batch_handler(Handler_batch* handler) {
    batch_handler_ = handler;
}

void NetworkServer::stop_accept() {
    if (acceptor_) acceptor_->stop();
}

void NetworkServer::stop() {
    stop_accept();
    for (auto& r : reactors_) r->stop();
    reactors_.clear();
    acceptor_.reset();
    started_ = false;
}
