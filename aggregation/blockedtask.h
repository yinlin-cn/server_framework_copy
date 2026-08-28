#pragma once
#include"work_task.h"
#include"Box.h"
#include <cstdint>
#include <utility>
#include<memory>
using namespace std;
struct blockedtask {
    uint64_t wait_name;
    work_task funtion;
    std::shared_ptr<Box> box; 
    blockedtask(uint64_t a, work_task b, std::shared_ptr<Box> c = nullptr)
        : wait_name(a), funtion(std::move(b)), box(std::move(c)) {}
};