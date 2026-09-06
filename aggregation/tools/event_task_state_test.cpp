#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdio>
#include <memory>
#include <vector>

// 最小实验：验证 EventTask 显式携带 shared RequestState 时，
// final_suspend 能否可靠把 active 归零。

struct RequestState {
    std::atomic<int> active{1};
    std::atomic<int> released{0};
};

thread_local std::shared_ptr<RequestState> g_pending_state;

struct ManualAwaiter {
    std::coroutine_handle<> handle;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        handle = h;
    }

    void await_resume() {}
};

struct TestTask {
    struct promise_type {
        std::shared_ptr<RequestState> state;

        promise_type()
            : state(std::move(g_pending_state)) {}

        TestTask get_return_object() {
            return TestTask{state};
        }

        std::suspend_never initial_suspend() { return {}; }

        std::suspend_never final_suspend() noexcept {
            if (state) {
                state->active--;
                state->released++;
            }
            return {};
        }

        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };

    std::shared_ptr<RequestState> state;
};

TestTask run(std::shared_ptr<RequestState> state,
             ManualAwaiter* awaiter) {
    co_await *awaiter;
    (void)state;
}

int main() {
    constexpr int N = 1000;
    std::vector<TestTask> tasks;
    std::vector<ManualAwaiter> awaiters(N);
    std::vector<std::shared_ptr<RequestState>> states;

    tasks.reserve(N);
    for (int i = 0; i < N; i++) {
        auto state = std::make_shared<RequestState>();
        states.push_back(state);
        g_pending_state = state;                     // promise 构造时取走
        tasks.emplace_back(run(state, &awaiters[i]));
        g_pending_state.reset();                     // 防止泄漏到下一条
        assert(awaiters[i].handle != nullptr);   // 全部已挂起
    }

    for (int i = 0; i < N; i++) {
        assert(states[i]->active.load() == 1);
        awaiters[i].handle.resume();
    }

    for (int i = 0; i < N; i++) {
        assert(states[i]->active.load() == 0);
        assert(states[i]->released.load() == 1);
    }

    std::printf("EVENT_TASK_STATE_TEST-OK n=%d\n", N);
    return 0;
}
