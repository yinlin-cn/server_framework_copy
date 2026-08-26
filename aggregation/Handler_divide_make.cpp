#include"Handler_divide_make.h"
#include<functional>
#include<memory>
using namespace std;
void Handler_divide_make::on_work(std::shared_ptr<Internalconnection> conn,std::function<void()> work){
        wp->add_task(work_task{work, conn});
}

Handler_divide_make* Handler_divide_Factory_make::create_handler(){
        return new Handler_divide_make(wp);
}