#pragma once
#include <string>

// 数据库查询结果：业务层必须显式判断，避免把取消/错误当正常结果。
struct DBResult {
    bool ok = false;          // 是否成功拿到结果
    bool cancelled = false;   // 是否被取消（退出/超时）
    std::string err;          // 错误描述（!ok 且未取消时有效）
    std::string data;         // 正常结果（ok 时有效）
};