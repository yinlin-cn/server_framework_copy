#include"Handler_divide_make.h"
#include "Reactor.h"
#include<functional>
#include<memory>
using namespace std;
void Handler_divide_make::on_work(std::shared_ptr<Internalconnection> conn,
                                  std::function<void()> work,
                                  std::shared_ptr<DbCreditToken> db_credit){
        work_task task{work, conn, true};
        task.db_credit = std::move(db_credit);   // 解析产出的业务请求沿用准入令牌
        wp->add_task(std::move(task));
}

Handler_divide_make* Handler_divide_Factory_make::create_handler(){
        return new Handler_divide_make(wp);
}
