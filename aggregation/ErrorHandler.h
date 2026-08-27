#pragma once
#include <functional>
#include <memory>
#include <string>

#include "Internalconnection.h"

// 业务层可注入的错误回调。
// conn：发生错误时所属的连接（可能为 null）。
// stage：出错阶段，如 "divide"（解析）/"work"（业务）。
// err：框架整理好的错误描述，具体怎么记录/响应由业务层决定。
using ErrorHandler = std::function<void(std::shared_ptr<Internalconnection> conn,
                                        const std::string& stage,
                                        const std::string& err)>;
