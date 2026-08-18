#include<iostream>
#include<thread>
#include<string>
#include<memory>
#include<vector>
#include<queue>
#include<tuple>
#include<functional>
#include<mutex>
#include <coroutine>
#include <condition_variable>
#include<unordered_map>
using namespace std;

void show(int n)
{
	for (int i = 0; i < n; i++)
		cout << "[" << this_thread::get_id() << "]" << i << " ";
	cout << endl;
}


template<typename T,typename U,typename... Arg>
struct task {
	T back_funtion;
	U id;
	std::tuple<Arg...> args;
	task(U a, T b, Arg... c) :id(a), back_funtion(b), args(c...) {};
	~task() = default;
	task() = default;
	void operator()() {
		std::apply(back_funtion, args);
	}
};



template<int N=8,typename T= std::function<void()>>
class thread_pool
{
private:
	queue<T> tasks;
	mutex for_task;
	bool stop = false;
	vector<thread> pool;
	std::condition_variable cv;
public:
	thread_pool() {
		for (int i = 0; i < N; i++)
			pool.emplace_back(&thread_pool::worker, this);
	};
	~thread_pool(){ 
		{
			std::lock_guard<std::mutex> lock(for_task);
			stop = true;              // 1. 加锁置停止标志
		}
		cv.notify_all();              // 2. 唤醒所有线程
		for (auto& t : pool)
			t.join();                 // 3. 等所有线程退出
	};
	void worker()
	{
		while (true)
		{
			T funtion;
			{
				std::unique_lock<mutex> lock(for_task);
				cv.wait(lock, [this]() {return stop || !tasks.empty(); });

					if (stop && tasks.empty()) {
						return;
					}
				funtion = move(tasks.front());
				tasks.pop();
			}
			funtion();
		}
	};
	void add_task(T funtion)
	{
		std::lock_guard<mutex> lock(for_task);
		tasks.push(funtion);
		cv.notify_one();
	}
};

template<typename U= std::string, typename T = std::function<void()>,typename P=int>
struct blockedtask {
	P host;//归属标识
	U wait_name;
	T funtion;
	blockedtask(U a, T b, P c = 0) :wait_name(a), funtion(b), host(c) {};
};

template<typename U= std::string,typename block= blockedtask<U, std::function<void()>>>//此处U的类型应该与block的wait_name类型一致
class blockingqueue {
private:
	unordered_map<U, vector<block>>task_queue;
	mutex for_queue;
public:
	using key_type = U;
	using block_type = block;
	blockingqueue() {};
	void insert(block task)
	{
		lock_guard<mutex> lock(for_queue);
		task_queue[task.wait_name].push_back(task);
	};
	vector<block> take(U& name)
	{
		lock_guard<mutex> lock(for_queue);
		auto it = task_queue.find(name);
		if (it == task_queue.end())
			return {};
		vector<block> task = std::move(it->second);
		task_queue.erase(it);
		return task;
	};
};

template<int N=8,typename T= blockingqueue<>>
class work_pool 
{
private:
	thread_pool<N> pool;
	T queue_;
public:
	work_pool() {};
	T& get_queue() { return queue_; }
	void add_task(std::function<void()> task)
	{
		pool.add_task(task);
	}
	template<typename U>
	void on_event(U wait_key) {
		auto tasks = queue_.take(wait_key);   // 找到在等的
		for (auto& t : tasks)
			pool.add_task(std::move(t.funtion));    // 重新入队
	}
	template<typename Block>
	void add_blockingtask(Block block)
	{
		queue_.insert(block);
	}
};


struct Task {
	struct promise_type {
		Task get_return_object() { return {}; }
		std::suspend_never initial_suspend() { return {}; }          // 调用即开始跑
		std::suspend_never final_suspend() noexcept { return {}; }   // 结束自动销毁帧
		void return_void() {}
		void unhandled_exception() { std::terminate(); }
	};
};

template<typename Queue>
struct EventAwaiter {
	typename Queue::key_type wait_key;
	Queue* queue;
	std::coroutine_handle<> handle;

	bool await_ready() { return false; }   // 总是先挂起

	void await_suspend(std::coroutine_handle<> h) {
		handle = h;
		// 把"恢复自己"封装成阻塞任务插入队列
		// this 指向协程帧里的 awaiter，协程挂起期间帧存活 → 安全
		queue->insert(typename Queue::block_type{
			wait_key,
			[this] { handle.resume(); }
			});
	}

	void await_resume() {}   // 恢复后 co_await 求值为这里
};



template<typename Work_pool>
class work
{
private:
	shared_ptr<Work_pool> wp;
	int wei = 0;
public:
	work(shared_ptr<Work_pool> a) :wp(a) {};
	void show_(int n)
	{
		string s;
		for (int h = 0; h < n; h++) s += std::to_string(h) + " ";
		cout << s << endl;   // 一次输出
		blockedtask<int>ha(wei, [=, this]() {show(n); });
		wp->add_blockingtask(ha);
		wei++;
	}
	using Queue = blockingqueue<int, blockedtask<int, std::function<void()>>>;
	Task business_flow(int order_id) {
		std::cout <<order_id<< " 前段：下单完成，等待商家接单...\n";

		// 挂起点：恢复后，下面这行自动执行
		co_await EventAwaiter<Queue>{
			order_id,
				&wp->get_queue()
		};

		std::cout << order_id << " 后段：商家接单了，继续处理订单 " <<  "\n";
	}
	/*template<typename T, typename... Arg>
	void add(T task,Arg... args)
	{
		std::tuple<Arg...>arg(args...);
		std::function<void()>a = [=, this]() {
			blockedtask<int>ha(wei, [=, this]() {std::apply(task, arg); });
			std::apply(task, arg);
			wp->add_blockingtask(ha);
			};
		wp->add_task(a);
		wei++;
	}
	*/
};




int main()
{
	/*
	thread_pool<5, task<void(*)(int),int,int>> ceshi;
	task renwu(1,&show,10);
	for (int h = 0; h < 10; h++)
	{
		ceshi.add_task(renwu);
	}
	*/
	/*
	thread_pool<5> ceshi;
	std::function<void()> renwu = [=]() {show(5); };
	for (int i = 0; i < 10; i++)
		ceshi.add_task(renwu);
	return 0;
	*/
	/*
	using wpl=work_pool<5, blockingqueue<int, blockedtask<int>>>;
	shared_ptr<wpl>pool = make_shared<wpl>();
	auto liu = make_shared<work<wpl>>(pool);
	std::function<void()> a = [liu]() {liu->show_(5); };
	pool->add_task(a);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	pool->on_event(0);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	*/
	using wpl = work_pool<5, blockingqueue<int, blockedtask<int>>>;
	auto pool = std::make_shared<wpl>();
	auto liu = make_shared<work<wpl>>(pool);
	// 提交：协程启动 → 跑到 co_await 挂起 → 任务函数返回 → 线程归还
	for(int h=1001;h<1200;h++)
	{
		pool->add_task([liu,h] {
			liu->business_flow(h);   // 返回的 Task 丢弃即可
			});
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
	for (int h = 1001; h < 1200; h++)
	pool->on_event(h); // 唤醒 → 协程恢复
	std::this_thread::sleep_for(std::chrono::seconds(1));
	while (true);
	return 0;
}