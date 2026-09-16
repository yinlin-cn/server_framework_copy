#pragma once

#include <memory>

#include "backpressure.h"
#include "Internalconnection.h"

class ReactorControl : public IReactorControl {
public:
    void pause_reading(
        std::shared_ptr<Internalconnection> conn) override;
    void schedule_resume(
        std::shared_ptr<Internalconnection> conn) override;
};
