#pragma once
#include <cstdint>
#include<string>
using namespace std;
struct Box {
    std::string result;
    bool ready = false;
    uint64_t wait_name = 0;
};