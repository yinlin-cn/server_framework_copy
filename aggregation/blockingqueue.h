#pragma once
#include"blockedtask.h"
#include<unordered_map>
#include<mutex>
#include<vector>
using namespace std;
class blockingqueue {
private:
    std::unordered_map<uint64_t, vector<blockedtask>> task_queue;
    std::mutex for_queue;
public:
    using key_type   = uint64_t;
    using block_type = blockedtask;
    blockingqueue();
    void insert(blockedtask task);
    std::vector<blockedtask> take(uint64_t& name);
};