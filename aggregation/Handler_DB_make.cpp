#include "Handler_DB_make.h"

void Handler_DB_make::submit(uint64_t wait_key, std::shared_ptr<Box> box,
                             const std::string& message) {
    db_pool_->submit(DBTask{wait_key, box, message});   // DBTask 在 make 里生成并推入
}