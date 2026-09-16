#include "ConnectionSession.h"

using namespace std;

bool ConnectionSession::send(const std::string& msg) const {
    return fc_ && fc_->send_to_sb(virtual_fd_, msg).success;
}

call_result ConnectionSession::call(
    const std::string& cmd, const FrameworkCall::Args& args) const {
    if (!fc_)
        return {false, "framework not ready"};
    return fc_->call(cmd, args);
}

bool ConnectionSession::close(const std::string& reason) const {
    return fc_ && fc_->close_conn(virtual_fd_, reason).success;
}

bool ConnectionSession::connected() const {
    if (!fc_)
        return false;
    auto book = fc_->connection_book();
    if (!book)
        return false;
    auto conn = book->find(virtual_fd_);
    return conn && conn->connected;
}

uint64_t ConnectionSession::version() const {
    auto book = fc_ ? fc_->connection_book() : nullptr;
    return book ? book->version() : 0;
}

bool ConnectionSession::wait_version_change(
    uint64_t old_version, std::chrono::milliseconds timeout) const {
    auto book = fc_ ? fc_->connection_book() : nullptr;
    return book && book->wait_version_change(old_version, timeout);
}
