#include"thread_pool.h"

using namespace std;
thread_pool::thread_pool(int N) {
        for (int i = 0; i < N; i++)
            pool.emplace_back(&thread_pool::worker, this);
    }
thread_pool::~thread_pool() {
        { lock_guard<mutex> lock(for_task); stop = true; }
        cv.notify_all();
        for (auto& t : pool) t.join();
    }

void thread_pool::worker() {
        while (true) {
            work_task f;
            {
                unique_lock<mutex> lock(for_task);
                cv.wait(lock, [this]{ return stop || !tasks.empty(); });
                if (stop && tasks.empty()) return;
                f = move(tasks.front());
                tasks.pop();
            }
            tls_current_conn = f.conn;
            try {
                f.fn();
            } catch (const std::exception& e) {
                if (error_handler_) error_handler_(f.conn, "work", e.what());
            } catch (...) {
                if (error_handler_) error_handler_(f.conn, "work", "unknown error");
            }
            tls_current_conn.reset();
        }
    }

void thread_pool::add_task(work_task f) {
        lock_guard<mutex> lock(for_task);
        tasks.push(move(f));
        cv.notify_one();
    }