"""清理僵尸消费者（教学示例）。

背景：
- Redis Stream Consumer Group 中，历史进程退出后可能残留消费者名。
- 如果消费者长期 idle 且没有 pending 消息，可视为可回收对象。
- 多进程部署时，当前在线消费者不能被误删。
"""

from __future__ import annotations

import os
from typing import Iterable

import redis.asyncio as redis

from src.stream_config import STREAM_CONFIGS


async def cleanup_idle_consumers(
    redis_client: redis.Redis,
    *,
    stream_name: str,
    group_name: str,
    current_consumer_names: Iterable[str],
    idle_threshold_ms: int,
    pending_threshold: int = 0,
) -> int:
    """按 idle + pending 规则清理消费者，并跳过当前消费者。"""

    current_set = set(current_consumer_names)
    cleaned_count = 0

    consumers_info = await redis_client.xinfo_consumers(stream_name, group_name)
    for consumer in consumers_info:
        name = consumer.get("name")
        pending = int(consumer.get("pending", 0))
        idle = int(consumer.get("idle", 0))

        # 跳过当前进程正在使用的 main/claiming 消费者。
        if name in current_set:
            continue

        # 删除条件：
        # 1) pending 不超过阈值（默认必须为 0）
        # 2) idle 超过阈值
        if pending <= pending_threshold and idle > idle_threshold_ms:
            await redis_client.xgroup_delconsumer(stream_name, group_name, name)
            cleaned_count += 1

    return cleaned_count


async def startup_cleanup_consumers() -> int:
    """应用启动时批量清理所有 stream 的历史僵尸消费者。"""

    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_password = os.getenv("REDIS_PASSWORD")

    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        decode_responses=True,
    )

    total_cleaned = 0
    for config in STREAM_CONFIGS.values():
        cleaned = await cleanup_idle_consumers(
            redis_client,
            stream_name=config.stream_name,
            group_name=config.group_name,
            current_consumer_names=[
                config.main_consumer_name,
                config.claiming_consumer_name,
            ],
            idle_threshold_ms=3 * 24 * 60 * 60 * 1000,  # 3 天
            pending_threshold=0,
        )
        total_cleaned += cleaned

    await redis_client.aclose()
    return total_cleaned
