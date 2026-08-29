#pragma once

class Reactor;

// 批处理接线接口：Reactor 只依赖它，不接触 BatchSender 内部。
class Handler_batch {
public:
    virtual ~Handler_batch() = default;
    virtual void on_need_send(Reactor* reactor) = 0;   // 该 Reactor 有待发数据
};

// 批处理 Handler 工厂接口。
class Handler_batch_Factory {
public:
    virtual ~Handler_batch_Factory() = default;
    virtual Handler_batch* create_handler() = 0;
};
