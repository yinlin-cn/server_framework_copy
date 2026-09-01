#include "Handler_DB_make.h"

void Handler_DB_make::submit(uint64_t wait_key, std::shared_ptr<Box> box,
                             const std::string& sql,
                             const std::vector<std::string>& params) {
    db_pool_->submit(DBTask{wait_key, box, sql, params});   // DBTask 在 make 里生成并推入
}
