#pragma once
#include <memory>
#include <functional>
#include"Internalconnection.h"
#include"work_task.h"
using namespace std;
class DbCreditToken;

class Handler_divide{
public:
    virtual ~Handler_divide() = default;
    virtual void on_work(std::shared_ptr<Internalconnection> conn,
                         std::function<void()> work,
                         std::shared_ptr<DbCreditToken> db_credit = nullptr) = 0;
};

class Handler_divide_Factory {
public:
    virtual Handler_divide* create_handler() = 0;
};
