#pragma once
#include"Handler_divide.h"
#include"work_task.h"
#include"work_pool.h"
using namespace std;
class Handler_divide_make:public Handler_divide{
    private:
        std::shared_ptr<work_pool> wp;
    public:
        Handler_divide_make(std::shared_ptr<work_pool> a):wp(a){};
        void on_work(std::shared_ptr<Internalconnection> conn,std::function<void()> work) override;
};

class Handler_divide_Factory_make:public Handler_divide_Factory {
    private:
        std::shared_ptr<work_pool> wp;
    public:
        Handler_divide_Factory_make(std::shared_ptr<work_pool> a):wp(a){};
        Handler_divide_make* create_handler() override;
};