#pragma once
#include <string>
#include <vector>
#include "thread_context.h"
#include "EventAwaiter.h"
#include "EventTask.h"
using namespace std;
// 业务层唯一接口
bool send(const std::string& data);   // 返回是否真正入队（连接已断开时返回 false）
// 参数化查询：sql 为模板（? 占位符），params 按顺序对应。
EventAwaiter query_db(const std::string& sql, std::vector<std::string> params = {});
