#pragma once
#include"work_task.h"
#include <cstdint>
#include <utility>
using namespace std;
struct blockedtask {
    uint64_t wait_name;
    work_task funtion;
    blockedtask(uint64_t a, work_task b) : wait_name(a), funtion(std::move(b)) {}
};