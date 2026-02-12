# FastStream + Redis Stream 最佳范式示例

本目录提供面向工程实现的示例代码，目标是把本仓库现有的任务处理经验沉淀为可复用范式，而非业务功能代码。

对应示例代码目录：`src/`

---

## 为什么选 FastStream（相对 arq / taskiq 的简要对比）

> 这里聚焦你们当前场景：Redis Stream + 手动 ACK + 可恢复消费。

### 1) RPC 能力

- **FastStream**：内建 `request/reply` 与消息抽象，既可 fire-and-forget，也可做 RPC。
- **arq**：更偏“任务队列”模型，RPC 语义不是核心设计点。
- **taskiq**：偏通用异步任务框架，插件生态可做较多适配，但 RPC 与 Stream 的一体化心智不如 FastStream 直接。

### 2) Redis Stream 一等支持

- **FastStream**：对 Redis Stream/Consumer Group 语义贴合高，可直接表达 `group/consumer/min_idle_time/ack`。
- **arq**：传统上更偏 Redis 列表/延迟任务心智，Stream 语义表达不是主路径。
- **taskiq**：取决于 broker 插件能力，Stream 细节可控性通常不如直接面向 FastStream Redis 适配层。

### 3) ACK 与恢复路径

- **FastStream**：可手动 `msg.ack(...)`，结合 claiming subscriber 构建超时恢复路径。
- **arq/taskiq**：可以实现重试/恢复，但以 Redis Stream pending/claim 为中心的范式通常需要更多自定义胶水层。

结论：在“Redis Stream 原生语义 + 主路径/恢复路径 + 状态追踪”这个组合下，FastStream 与当前项目需求匹配度高。

---

## 核心范式 1：一个 Stream 两条消费路径（Main + Claiming）

### 设计

每个 stream 至少两个 subscriber：

1. **Main worker（fast path）**：消费新消息。
2. **Claiming worker（recovery path）**：消费 `min_idle_time` 超时仍未 ACK 的 pending 消息。

### 价值

- 主路径吞吐与恢复路径隔离，职责清晰。
- 进程异常退出、网络抖动、长耗时中断时，任务可被恢复处理。

参考实现：
- `src/workers.py`
- `src/stream_config.py`

---

## 核心范式 2：自定义 Broker，publish 返回业务 UUID

### 问题背景

Redis Stream message id（如 `1767854208631-0`）是 stream 局部语义，不适合作为跨 stream 全局 task_id。

### 方案

1. `publish` 时生成 UUID。
2. 写入 header：`x-message-uuid`。
3. 业务层返回该 UUID 作为任务追踪主键。

### 价值

- 跨 stream、跨模块统一查询任务状态。
- 与前端/API 层交互时可直接暴露稳定 task_id。

参考实现：
- `src/custom_broker.py`

---

## 核心范式 3：任务状态中间件（四态状态机）

### 状态流转

```text
publish -> pending
consumer receive -> processing
business success -> completed
business exception -> failed
```

### 关键点

- 状态 key：`task:status:{x-message-uuid}`。
- 过期策略：建议 TTL（示例为 3 天）。
- 失败状态包含 error type/message/traceback，便于排障。

参考实现：
- `src/task_status_middleware.py`
- `src/publisher.py`（状态查询示例）

---

## 核心范式 4：启动清理僵尸消费者

### 问题背景

多进程/滚动发布场景下，consumer name 通常应唯一。历史消费者若长期残留，会导致组内噪音与运维负担。

### 清理策略

- 条件：`pending <= 阈值` 且 `idle > 阈值`。
- 保护：跳过当前在线消费者（main + claiming）。

### 价值

- 降低 consumer group 污染。
- 提升可观测性，减少“幽灵消费者”误判。

参考实现：
- `src/cleanup_consumers.py`
- `src/workers.py` 中 `on_startup` 调用

补充讨论参考：Redis 社区关于消费者管理实践（https://github.com/redis/redis/discussions/14681）

---

## 目录结构

```text
faststream_redis_stream_best_practice/
├── README.md
└── src/
    ├── README.md
    ├── stream_config.py
    ├── custom_broker.py
    ├── task_status_middleware.py
    ├── cleanup_consumers.py
    ├── workers.py
    └── publisher.py
```

---

## 最小接入思路（不要求立即运行）

1. 复制 `src/` 中的 6 个 Python 文件到你的任务模块。
2. 根据业务拆分 stream config（例如 `asr/kb/export`）。
3. 统一通过自定义 broker 发布消息，获得业务 UUID。
4. 将任务查询接口统一基于 `task:status:{uuid}` 读取状态。
5. 在应用启动钩子挂载消费者清理。

最小运行命令见：`src/README.md`。
