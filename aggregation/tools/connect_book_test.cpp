#include <cassert>
#include <memory>
#include <string>

#include "connect_book.h"

int main() {
    auto c1 = std::make_shared<Internalconnection>(100);
    auto c2 = std::make_shared<Internalconnection>(101);
    bool sent = false;
    c1->send_function = [&](const std::string& msg) {
        sent = (msg == "hi");
        return true;
    };
    c2->send_function = [](const std::string&) { return true; };

    connect_book book;
    assert(book.on_connection(c1) == 100);
    assert(book.on_connection(c2) == 101);

    assert(book.rebind(c1, 1001, 7));
    assert(book.send_to(1001, "hi") && sent);

    uint64_t virtual_fd = 0;
    int group_name = -1;
    assert(book.connection_info(c1, virtual_fd, group_name));
    assert(virtual_fd == 1001 && group_name == 7);

    assert(book.set_group(101, 9));
    auto g9 = book.group_snapshot(9);
    assert(g9.size() == 1);
    auto g9_fds = book.group_virtual_fds(9);
    assert(g9_fds.size() == 1 && g9_fds[0] == 101);

    assert(book.assign_group(7, {1001, 101}));
    assert(book.group_snapshot(7).size() == 2);
    assert(book.group_virtual_fds(7).size() == 2);

    uint64_t old_version = book.version();
    assert(old_version > 0);
    assert(book.set_group(1001, 8));   // 制造一次新变更
    assert(book.group_virtual_fds(7).size() == 1);
    assert(book.group_virtual_fds(8).size() == 1);
    assert(book.wait_version_change(old_version, std::chrono::milliseconds(1)));

    auto changes = book.take_changes();
    assert(!changes.empty());

    book.dis_connection(c1);
    assert(!book.find(1001));
    assert(!book.connection_info(c1, virtual_fd, group_name));
    assert(book.group_snapshot(7).size() == 1);

    book.shutdown();
    return 0;
}
