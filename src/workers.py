"""Worker 示例：Main + Claiming 双路径消费。"""

from __future__ import annotations

import os
from typing import Any

from faststream import FastStream
from faststream.redis import Redis, RedisRouter, RedisStreamMessage, StreamSub
from loguru import logger
from pydantic import BaseModel, Field

from src.cleanup_consumers import (
    startup_cleanup_consumers,
)
from src.custom_broker import build_broker
from src.stream_config import DEMO_CONFIG


class DemoTask(BaseModel):
    """演示任务模型。"""

    task_name: str = Field(..., description="任务名称")
    payload: dict[str, Any] = Field(default_factory=dict, description="任务参数")


REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

if REDIS_PASSWORD:
    REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
else:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"


broker = build_broker(REDIS_URL)
app = FastStream(broker)
router = RedisRouter()
broker.include_router(router)


async def _handle_demo_task(body: DemoTask, msg: RedisStreamMessage, redis: Redis) -> None:
    """统一任务处理函数。"""

    logger.info(
        "开始处理任务: task_name={}, payload={}",
        body.task_name,
        body.payload,
    )

    # 业务逻辑（示例）
    # 这里可以调用数据库、HTTP、LLM 或其他内部服务。

    # 手动 ACK：只在真正处理成功后确认消息。
    await msg.ack(redis, group=DEMO_CONFIG.group_name)
    logger.info("任务处理成功并已 ACK: task_name={}", body.task_name)


@router.subscriber(
    stream=StreamSub(
        DEMO_CONFIG.stream_name,
        group=DEMO_CONFIG.group_name,
        consumer=DEMO_CONFIG.main_consumer_name,
        maxlen=200,
    )
)
async def main_worker(body: DemoTask, msg: RedisStreamMessage, redis: Redis) -> None:
    """主路径消费者：处理新到消息（fast path）。"""

    try:
        await _handle_demo_task(body, msg, redis)
    except Exception:
        logger.exception("Main worker 处理失败")
        raise


@router.subscriber(
    stream=StreamSub(
        DEMO_CONFIG.stream_name,
        group=DEMO_CONFIG.group_name,
        consumer=DEMO_CONFIG.claiming_consumer_name,
        min_idle_time=DEMO_CONFIG.claiming_min_idle_time_ms,
        maxlen=200,
    )
)
async def claiming_worker(body: DemoTask, msg: RedisStreamMessage, redis: Redis) -> None:
    """恢复路径消费者：处理超时未 ACK 消息（recovery path）。"""

    logger.warning("Claiming worker 接管超时任务: task_name={}", body.task_name)
    try:
        await _handle_demo_task(body, msg, redis)
    except Exception:
        logger.exception("Claiming worker 恢复处理失败")
        raise


@app.on_startup
async def _on_startup_cleanup() -> None:
    """启动时清理历史僵尸消费者。"""

    cleaned = await startup_cleanup_consumers()
    logger.info("启动清理完成，删除消费者数量: {}", cleaned)
