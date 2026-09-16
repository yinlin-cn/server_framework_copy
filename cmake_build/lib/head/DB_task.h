#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "Box.h"

struct DBTask {
    uint64_t wait_name;
    std::shared_ptr<Box> box;
    std::string sql;                       // SQL 模板，含 ? 占位符
    std::vector<std::string> params;       // 参数，按顺序对应 ?
};
