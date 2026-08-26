#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include "Box.h"

struct DBTask {
    uint64_t wait_name;
    std::shared_ptr<Box> box;
    std::string sql;   // 设计补漏：任务要带 SQL
};