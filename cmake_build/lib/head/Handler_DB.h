#pragma once
#include"Box.h"
#include<string>
#include<vector>
#include <cstdint>
#include<memory>
using namespace std;
class Handler_DB {
public:
    virtual ~Handler_DB() = default;
    virtual void submit(uint64_t wait_key, std::shared_ptr<Box> box,
                        const std::string& sql,
                        const std::vector<std::string>& params) = 0;
};

class Handler_DB_Factory {
public:
    virtual Handler_DB* create_handler() = 0;
};
