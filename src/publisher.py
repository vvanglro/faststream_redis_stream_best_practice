"""Publisher 示例：发布任务并拿到业务 UUID。"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import redis.asyncio as redis
from pydantic import BaseModel, Field

from src.custom_broker import build_broker
from src.stream_config import DEMO_CONFIG


class DemoTask(BaseModel):
    """演示任务模型（与 workers.py 保持一致）。"""

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


async def publish_demo_task() -> str:
    """发布一条任务，返回业务 task UUID。"""

    broker = build_broker(REDIS_URL)
    await broker.start()
    try:
        task = DemoTask(task_name="generate-summary", payload={"doc_id": "doc-001"})

        task_uuid = await broker.publish(
            message=task.model_dump(),
            stream=DEMO_CONFIG.stream_name,
        )
        print(f"published task uuid: {task_uuid}")
        return task_uuid
    finally:
        await broker.stop()


async def query_task_status(task_uuid: str) -> dict[str, Any] | None:
    """按 UUID 查询状态中间件写入的任务状态。"""

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )
    try:
        raw = await redis_client.get(f"task:status:{task_uuid}")
        return json.loads(raw) if raw else None
    finally:
        await redis_client.aclose()


async def main() -> None:
    task_uuid = await publish_demo_task()

    # 演示：等待 worker 处理后查询状态。
    await asyncio.sleep(1)

    status = await query_task_status(task_uuid)
    print("task status:", status)


if __name__ == "__main__":
    asyncio.run(main())
