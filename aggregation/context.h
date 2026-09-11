#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "thread_context.h"
#include "EventAwaiter.h"
#include "EventTask.h"
using namespace std;

struct virtual_conn_info {
    bool valid = false;
    uint64_t virtual_fd = 0;
    int group_name = -1;
};

// 业务层唯一接口
bool send(const std::string& data);   // 返回是否真正入队（连接已断开时返回 false）
// 框架调用：cmd + 字符串参数，返回调用是否成功
bool framework_call(const std::string& cmd,
                    const std::vector<std::string>& args);
// 查询当前业务连接在名册中的 virtual_fd 与连接组
virtual_conn_info get_current_virtual_conn();
// 查询指定连接组内当前有效的 virtual_fd 列表
std::vector<uint64_t> get_group_info(int group_name);
// 参数化查询：sql 为模板（? 占位符），params 按顺序对应。
EventAwaiter query_db(const std::string& sql, std::vector<std::string> params = {});
