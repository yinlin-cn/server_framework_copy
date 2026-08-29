#include <iostream>
#include <thread>
#include <string>
#include <memory>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <coroutine>
#include <condition_variable>
#include <unordered_map>
#include <atomic>
#include <cstdint>
#include"context.h"
using namespace std;

class DBHandler;


struct PoolTask {
    function<void()> fn;
    shared_ptr<internalconnection> conn;
};

template<int N = 8>
class thread_pool {
    queue<PoolTask> tasks;
    mutex for_task;
    bool stop = false;
    vector<thread> pool;
    condition_variable cv;

public:
    thread_pool() {
        for (int i = 0; i < N; i++)
            pool.emplace_back(&thread_pool::worker, this);
    }
    ~thread_pool() {
        { lock_guard<mutex> lock(for_task); stop = true; }
        cv.notify_all();
        for (auto& t : pool) t.join();
    }

    void worker() {
        while (true) {
            PoolTask f;
            {
                unique_lock<mutex> lock(for_task);
                cv.wait(lock, [this]{ return stop || !tasks.empty(); });
                if (stop && tasks.empty()) return;
                f = move(tasks.front());
                tasks.pop();
            }
            tls_current_conn = f.conn;
            f.fn();
            tls_current_conn.reset();
        }
    }

    void add_task(PoolTask f) {
        lock_guard<mutex> lock(for_task);
        tasks.push(move(f));
        cv.notify_one();
    }
};

template<typename Task, typename U = uint64_t>
struct blockedtask {
    U wait_name;
    Task funtion;

    blockedtask(U a, Task b) : wait_name(a), funtion(std::move(b)) {}
};

template<typename U = uint64_t, typename block = blockedtask<PoolTask, U>>
class blockingqueue {
private:
    unordered_map<U, vector<block>> task_queue;
    mutex for_queue;

public:
    using key_type   = U;
    using block_type = block;

    blockingqueue() {}

    void insert(block task) {
        lock_guard<mutex> lock(for_queue);
        task_queue[task.wait_name].push_back(task);
    }

    vector<block> take(U& name) {
        lock_guard<mutex> lock(for_queue);
        auto it = task_queue.find(name);
        if (it == task_queue.end())
            return {};
        vector<block> task = std::move(it->second);
        task_queue.erase(it);
        return task;
    }
};

class work_pool {
    thread_pool<8> pool;
    blockingqueue<> queue_;

public:
    using queue_type = blockingqueue<>;

    blockingqueue<>& get_queue() { return queue_; }

    void add_task(function<void()> task) {
        pool.add_task(PoolTask{task, tls_current_conn});
    }
    void add_task(PoolTask task) {
        pool.add_task(move(task));
    }

    void on_event(uint64_t key) {
        auto ts = queue_.take(key);
        for (auto& t : ts)
            pool.add_task(move(t.funtion));
    }

    void add_blockingtask(blockedtask<PoolTask, uint64_t> b) {
        queue_.insert(move(b));
    }
};



struct Box {
    string result;
    bool ready = false;
    uint64_t wait_name = 0;
};


class DBHandler {
public:
    virtual ~DBHandler() = default;
    virtual void submit(uint64_t wait_key,Box box,std::string message) = 0;
};

struct Task {
    struct promise_type {
        Task get_return_object() { return {}; }
        suspend_never initial_suspend() { return {}; }
        suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { terminate(); }
    };
};

template<typename Queue>
struct EventAwaiter {
    typename Queue::key_type wait_key;
    Queue* queue;
    shared_ptr<Box> box;
    string message;
    DBHandler* db_handler;
    coroutine_handle<> handle;

    bool await_ready() { return false; }

    void await_suspend(coroutine_handle<> h) {
        handle = h;
        queue->insert(typename Queue::block_type{
            wait_key,
            PoolTask{ [this]{ handle.resume(); }, tls_current_conn }
        });

        if (db_handler && box) {
            box->wait_name = wait_key;
            db_handler->submit(wait_key, box, message);
        }
    }

    string await_resume() { return box ? box->result : message; }
};





