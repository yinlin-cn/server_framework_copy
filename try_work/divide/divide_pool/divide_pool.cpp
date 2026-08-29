#include <iostream>
#include <thread>
#include <string>
#include <memory>
#include <vector>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
using namespace std;

using Work = std::function<void()>;          // 业务任务
using ParseFn = std::function<Work()>;       // 解析函数：解析完返回业务任务


template<typename T>
class Handler {
public:
    virtual ~Handler() = default;
    virtual void on_work(T connection, Work work) = 0;
};

template<typename T>
class HandlerFactory {
public:
    virtual Handler<T>* create_handler() = 0;
};


template<typename P>
struct task {
    ParseFn back_funtion;                    // 解析函数
    P connection;                            // 这条任务属于哪个连接
    Handler<P>* handler;                     // 业务分发器

    task(ParseFn b, P d, Handler<P>* h)
        : back_funtion(b), connection(d), handler(h) {}
    task() = default;
};


template<typename P = int, int N = 8>
class thread_pool {
private:
    queue<task<P>> tasks;
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
        {
            lock_guard<mutex> lock(for_task);
            stop = true;
        }
        cv.notify_all();
        for (auto& t : pool) t.join();
    }

    void worker() {
        while (true) {
            task<P> funtion;
            {
                unique_lock<mutex> lock(for_task);
                cv.wait(lock, [this]() { return stop || !tasks.empty(); });
                if (stop && tasks.empty()) return;
                funtion = move(tasks.front());
                tasks.pop();
            }
            // 1. 执行解析函数，得到真正的业务任务
            Work work = funtion.back_funtion();
            // 2. 交给业务分发器，解析层不关心业务层内部结构
            if (funtion.handler)
                funtion.handler->on_work(funtion.connection, work);
        }
    }

    void add_task(task<P> funtion) {
        lock_guard<mutex> lock(for_task);
        tasks.push(move(funtion));
        cv.notify_one();
    }
};


