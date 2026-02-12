"""任务状态中间件（教学示例）。

状态机：
- pending: 消息发布成功，进入队列。
- processing: 消费者收到消息，开始处理。
- completed: 业务处理成功并完成。
- failed: 业务处理抛出异常。

说明：
- 任务 ID 使用 x-message-uuid（由 CustomRedisBroker 注入 header）。
- 状态写入 Redis，key 形如 task:status:{uuid}。
"""

from __future__ import annotations

import json
import os
import traceback
from datetime import datetime
from types import TracebackType
from typing import Any, Awaitable, Callable

import redis.asyncio as redis
from faststream import BaseMiddleware, PublishCommand, StreamMessage
from faststream.redis import BinaryMessageFormatV1
from faststream.redis.parser import RedisStreamParser


class _ParserConfig:
    message_format = BinaryMessageFormatV1


STREAM_PARSER = RedisStreamParser(_ParserConfig)


def _to_json_safe(obj: Any) -> Any:
    """递归将 bytes 转为字符串，避免 JSON 序列化失败。"""

    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return obj.decode("latin-1")
    if isinstance(obj, dict):
        return {
            (_to_json_safe(k) if isinstance(k, bytes) else k): _to_json_safe(v)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple, set)):
        return [_to_json_safe(item) for item in obj]
    return obj


def _read_message_uuid_from_headers(headers: dict[str, Any] | None) -> str | None:
    if not headers:
        return None
    value = headers.get("x-message-uuid")
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value) if value else None


class TaskStatusMiddleware(BaseMiddleware):
    """基于 Redis 的任务状态追踪中间件。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_db = int(os.getenv("REDIS_DB", "0"))
        redis_password = os.getenv("REDIS_PASSWORD")

        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
        )
        self.status_key_prefix = "task:status:"
        self.status_ttl_seconds = 3 * 24 * 60 * 60

    # Use this if you want to customize outgoing messages before they are sent,
    # such as adding encryption, compression, or custom headers.
    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any:
        """发布成功后写入 pending。"""

        message_uuid = _read_message_uuid_from_headers(cmd.headers)
        msg_id = await call_next(cmd)
        if message_uuid:
            await self._update_status(message_uuid, "pending", {})
        return msg_id

    # Use this if you want to add logic when a message is received for the first time,
    # such as logging incoming messages, validating headers, or setting up the context.
    async def on_receive(self) -> Any:
        """消费者首次收到消息时写入 processing。"""
        # {'type': 'stream', 'channel': 'demo-stream', 'message_ids': [b'1767854208631-0'], 'data': {b'__data__': b'\x89BIN\r\n\x1a\n\x00\x01\x00\x00\x00\x12\x00\x00\x00j\x00\x02\x00\x0ecorrelation_id\x00$a590d6fb-cacd-4ce2-bf74-969b323e3406\x00\x0ccontent-type\x00\x10application/json{"task_id":"task-001","content":"async task"}'}}
        parsed = await STREAM_PARSER.parse_message(self.msg)
        message_uuid = _read_message_uuid_from_headers(parsed.headers)
        if message_uuid:
            await self._update_status(
                message_uuid,
                "processing",
                {"raw_stream_message": self.msg},
            )
        return await super().on_receive()

    # Use this if you want to wrap the entire message processing process,
    # such as implementing retry logic, circuit breakers, rate limiting, or authentication.
    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        """业务处理成功后写入 completed。"""

        message_uuid = _read_message_uuid_from_headers(msg.headers)
        try:
            result = await call_next(msg)
            if message_uuid:
                await self._update_status(message_uuid, "completed", {"result": result})
            return result
        except Exception:
            # failed 状态由 after_processed 统一落库。
            raise

    # Use this if you want to perform post-processing tasks after message handling has completed,
    # such as cleaning up, logging errors, collecting metrics, or committing transactions.
    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool | None:
        """业务处理异常后写入 failed。"""

        if exc_val is not None:
            parsed = await STREAM_PARSER.parse_message(self.msg)
            message_uuid = _read_message_uuid_from_headers(parsed.headers)
            if message_uuid:
                await self._update_status(
                    message_uuid,
                    "failed",
                    {
                        "error": {
                            "type": exc_type.__name__ if exc_type else None,
                            "message": str(exc_val),
                            "traceback": traceback.format_exc(),
                        }
                    },
                )

        return await super().after_processed(exc_type, exc_val, exc_tb)

    async def _update_status(self, task_id: str, status: str, details: dict[str, Any]) -> None:
        status_data = {
            "status": status,
            "updated_at": datetime.now().isoformat(),
            **details,
        }
        await self.redis_client.setex(
            f"{self.status_key_prefix}{task_id}",
            self.status_ttl_seconds,
            json.dumps(_to_json_safe(status_data), ensure_ascii=False),
        )
