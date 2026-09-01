#pragma once
#include <functional>
#include<memory>
#include"Internalconnection.h"
using namespace std;
struct work_task {
    function<void()> fn;
    shared_ptr<Internalconnection> conn;
    bool is_business = false;   // 是否业务请求任务（区分 on_event 恢复任务）
};
