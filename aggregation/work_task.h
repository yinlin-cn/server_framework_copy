#pragma once
#include <functional>
#include<memory>
#include"Internalconnection.h"
using namespace std;
struct work_task {
    function<void()> fn;
    shared_ptr<Internalconnection> conn;
};