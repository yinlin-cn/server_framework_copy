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
using namespace std;

class DBHandler;

struct internalconnection {
    int sock = -1;
    function<bool(const string&)> send;
};

thread_local shared_ptr<internalconnection> tls_current_conn;

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

bool send(const string& data) {
    if (!tls_current_conn) return false;
    return tls_current_conn->send(data);
}

struct Box {
    string result;
    bool ready = false;
    uint64_t wait_name = 0;
};

struct DBTask {
    uint64_t wait_name;
    shared_ptr<Box> box;
    string sql;
};

class DBHandler {
public:
    virtual ~DBHandler() = default;
    virtual void submit(DBTask task) = 0;
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
            db_handler->submit(DBTask{ wait_key, box, message });
        }
    }

    string await_resume() { return box ? box->result : message; }
};

template<typename WorkPool>
class business {
    shared_ptr<WorkPool> wp;
    DBHandler* db_handler;
    atomic<uint64_t> key_alloc{0};

public:
    using Queue = typename WorkPool::queue_type;

    business(shared_ptr<WorkPool> p, DBHandler* db) : wp(p), db_handler(db) {}

    EventAwaiter<Queue> query_db(const string& sql) {
        uint64_t key = key_alloc.fetch_add(1);
        auto box = make_shared<Box>();
        box->wait_name = key;
        return EventAwaiter<Queue>{ key, &wp->get_queue(), box, sql, db_handler };
    }

    Task flow(int order_id) {
        cout << order_id << " 前段：开始查询...\n";
        string res = co_await query_db("select * from orders");
        cout << order_id << " 后段：拿到 " << res << "\n";
    }
};

class FakeDBHandler : public DBHandler {
    work_pool* bp;

public:
    explicit FakeDBHandler(work_pool* p) : bp(p) {}

    void submit(DBTask task) override {
        task.box->result = "查询结果@" + task.sql;
        task.box->ready = true;

        uint64_t key = task.wait_name;
        auto* pool = bp;
        pool->add_task([pool, key]{ pool->on_event(key); });
    }
};

int main() {
    auto pool = make_shared<work_pool>();
    FakeDBHandler db(pool.get());
    auto b = make_shared<business<work_pool>>(pool, &db);

    auto conn = make_shared<internalconnection>();
    conn->send = [](const string& s){ cout << "[send] " << s << endl; return true; };

    pool->add_task(PoolTask{ [b]{ b->flow(1); }, conn });
    pool->add_task(PoolTask{ [b]{ b->flow(2); }, conn });

    this_thread::sleep_for(chrono::seconds(1));
    return 0;
}