#pragma once
#include <memory>
#include <string>
#include "internalconnection.h"   // 网络层连接（已 include）
#include "work_pool.h"            // blockingqueue<>
#include "event_awaiter.h"        // EventAwaiter / Box / DBHandler

// 当前工作线程正在处理的连接（白板）
extern thread_local std::shared_ptr<internalconnection> tls_current_conn;

// 业务层唯一发送接口
bool send(const std::string& data);

// 异步数据库查询：co_await 后拿到结果
EventAwaiter<blockingqueue<>> query_db(const std::string& sql);