# Example：FastStream + Redis Stream 最佳范式

本目录提供一个可读性优先、可直接复制改造的最小示例，实现以下目标：

1. 一个 stream 两条消费路径：Main + Claiming。
2. 自定义 broker：`publish` 返回业务 UUID，而非 Redis message id。
3. 中间件驱动任务状态机：`pending -> processing -> completed/failed`。
4. 启动时清理僵尸消费者，避免 consumer group 长期污染。

---

## 文件说明

- `stream_config.py`：统一定义 stream/group/consumer 配置，并为当前进程生成唯一 consumer 名。
- `custom_broker.py`：重写 `publish` 注入 `x-message-uuid`，并返回业务 UUID。
- `task_status_middleware.py`：在 publish/consume 生命周期写入四态状态。
- `cleanup_consumers.py`：按 `idle + pending` 条件清理历史消费者，且跳过当前消费者。
- `workers.py`：Main + Claiming 双订阅路径示例。
- `publisher.py`：发布任务并按 UUID 查询状态。

---

## 运行前提

确保已安装依赖并准备 Redis：

```bash
uv sync
# 或 pip install -r requirements.txt
```

环境变量（最小）：

```bash
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_DB=0
# 若有密码：
# export REDIS_PASSWORD=your_password
```

---

## 最小运行步骤

### 1) 启动 Worker

在仓库根目录执行：

```bash
faststream run src.workers:app
```

### 2) 发布任务

新开一个终端执行：

```bash
python src.publisher.py
```

你将看到：

1. 发布返回的 `task uuid`（业务唯一标识）。
2. Redis 中按 `task:status:{uuid}` 查询到的状态快照。

---

## 关键行为说明

1. `publish` 返回 UUID：
   - 业务层不依赖 Redis message id（它只在单 stream 局部有意义）。
   - 跨 stream / 跨模块统一使用 task UUID 追踪。

2. Main + Claiming：
   - Main 处理新消息。
   - Claiming 处理超过 `min_idle_time` 且未 ACK 的 pending 消息。

3. 手动 ACK：
   - 示例中只在业务逻辑真正成功后调用 `msg.ack(...)`。
   - 若处理异常不 ACK，消息可由 Claiming worker 接管。

4. 启动清理消费者：
   - 清理条件：`pending <= threshold` 且 `idle > threshold`。
   - 跳过当前进程 main/claiming 名，避免误删在线消费者。
