#pragma once
#include <memory>
#include <string>
#include "Handler_DB.h"
#include "DB_pool.h"
#include "DB_task.h"

class Handler_DB_make : public Handler_DB {
    std::shared_ptr<DB_pool> db_pool_;

public:
    explicit Handler_DB_make(std::shared_ptr<DB_pool> p) : db_pool_(p) {}

    void submit(uint64_t wait_key, std::shared_ptr<Box> box,
                const std::string& sql,
                const std::vector<std::string>& params) override;
};

class Handler_DB_Factory_make : public Handler_DB_Factory {
    std::shared_ptr<DB_pool> db_pool_;
public:
    explicit Handler_DB_Factory_make(std::shared_ptr<DB_pool> p) : db_pool_(p) {}
    Handler_DB* create_handler() override {
        return new Handler_DB_make(db_pool_);
    }
};
