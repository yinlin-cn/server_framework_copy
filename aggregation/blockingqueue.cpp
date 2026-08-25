#include"blockingqueue.h"

using namespace std;

blockingqueue::blockingqueue() {}
void blockingqueue::insert(blockedtask task) {
        lock_guard<mutex> lock(for_queue);
        task_queue[task.wait_name].push_back(task);
    }
vector<blockedtask> blockingqueue::take(uint64_t& name) {
        lock_guard<mutex> lock(for_queue);
        auto it = task_queue.find(name);
        if (it == task_queue.end())
            return {};
        vector<blockedtask> task = std::move(it->second);
        task_queue.erase(it);
        return task;
    }