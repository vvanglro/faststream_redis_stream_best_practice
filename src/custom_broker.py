"""自定义 Redis Broker（教学示例）。

核心点：
1. 重写 publish，在业务侧返回 UUID，而不是 Redis Stream message id。
2. 将 UUID 写入 header: x-message-uuid，供消费者和中间件统一追踪。
3. 允许多 stream 并行时，使用统一的业务 task_id 做全局状态查询。
"""

from __future__ import annotations

import uuid
from typing import Any

from faststream.redis import RedisBroker

from src.task_status_middleware import (
    TaskStatusMiddleware,
)


class CustomRedisBroker(RedisBroker):
    """返回业务 UUID 的 Redis Broker。"""

    async def publish(
        self,
        message: Any = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        pipeline: Any | None = None,
    ) -> str:
        """发布消息并返回业务 UUID。"""

        message_uuid = str(uuid.uuid4())

        normalized_headers = dict(headers or {})
        normalized_headers["x-message-uuid"] = message_uuid

        if correlation_id is None:
            correlation_id = message_uuid

        # 父类实际返回 Redis Stream message id（例如 1767854208631-0）。
        # 这里我们故意忽略它，向业务层暴露统一 UUID。
        await super().publish(
            message=message,
            channel=channel,
            reply_to=reply_to,
            headers=normalized_headers,
            correlation_id=correlation_id,
            list=list,
            stream=stream,
            maxlen=maxlen,
            pipeline=pipeline,
        )
        return message_uuid


def build_broker(redis_url: str) -> CustomRedisBroker:
    """构造带任务状态中间件的 broker。"""

    return CustomRedisBroker(redis_url, middlewares=[TaskStatusMiddleware])
