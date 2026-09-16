#pragma once
#include <functional>
#include<memory>
#include"Internalconnection.h"
using namespace std;
class DB_credit_token;

struct work_task {
    function<void()> fn;
    shared_ptr<Internalconnection> conn;
    bool is_business = false;   // 是否业务请求任务（区分 on_event 恢复任务）
    shared_ptr<DB_credit_token> db_credit_token;  // 业务任务占用的 DB 准入额度；fast/恢复链路沿用同一令牌
};
