#include "ReactorControl.h"

#include "Reactor.h"

void ReactorControl::pause_reading(
    std::shared_ptr<Internalconnection> conn) {
    if (conn && conn->owner_reactor)
        conn->owner_reactor->pause_reading(conn);
}

void ReactorControl::schedule_resume(
    std::shared_ptr<Internalconnection> conn) {
    if (conn && conn->owner_reactor)
        conn->owner_reactor->schedule_resume(conn);
}
