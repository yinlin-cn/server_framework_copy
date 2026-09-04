#pragma once
#include <memory>
#include "Internalconnection.h"
using namespace std;
class work_pool;
class Handler_DB;
class FrameworkCall;

extern thread_local std::shared_ptr<Internalconnection> tls_current_conn;   // 真正的白板
extern work_pool* g_work_pool;       // 全局单例
extern Handler_DB* g_db_handler;     // 全局单例
extern FrameworkCall* g_framework_call;   // 框架调用全局入口
