#include <thread>
#include <memory>
#include <vector>
#include <functional>
#include <mutex>
#include<queue>
#include <condition_variable>
#include <atomic>
#include <cstdint>
#include "thread_context.h"
#include"work_task.h"

using namespace std;
class thread_pool {
private:
    queue<work_task> tasks;
    mutex for_task;
    bool stop = false;
    vector<thread> pool;
    condition_variable cv;
public:
    thread_pool(int N=8);
    ~thread_pool();
    void worker();
    void add_task(work_task f);
};