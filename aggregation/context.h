#pragma once
#include <string>
#include "thread_context.h"
#include "EventAwaiter.h"
#include "EventTask.h"
using namespace std;
// 业务层唯一接口
void send(const std::string& data);
EventAwaiter query_db(const std::string& sql);