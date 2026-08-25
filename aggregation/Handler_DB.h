#pragma once
#include"Box.h"
#include"DB_task.h"
#include<string>
#include <cstdint>
#include<memory>
using namespace std;
class Handler_DB {
public:
    virtual ~Handler_DB() = default;
    virtual void submit(uint64_t wait_key,std::shared_ptr<Box> box,const std::string& message) = 0;
};

class Handler_DB_Factory {
public:
    virtual Handler_DB* create_handler() = 0;
};