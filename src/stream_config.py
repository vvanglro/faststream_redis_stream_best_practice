"""Redis Stream 配置（教学示例）。

设计目标：
1. 每个进程启动时生成唯一 consumer 前缀，避免多进程同名冲突。
2. 每个 stream 固定两条消费路径：main + claiming。
3. claiming 路径通过 min idle time 触发超时任务恢复。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass


PROCESS_CONSUMER_UUID = str(uuid.uuid4())


@dataclass(frozen=True)
class StreamConfig:
    """单个 Redis Stream 的消费配置。"""

    stream_name: str
    group_name: str
    main_consumer_name: str
    claiming_consumer_name: str
    claiming_min_idle_time_ms: int


def build_stream_config(
    stream_name: str,
    group_name: str,
    *,
    claiming_min_idle_time_ms: int,
) -> StreamConfig:
    """构造带有唯一消费者名的 Stream 配置。"""

    consumer_base = f"{group_name}-{PROCESS_CONSUMER_UUID}"
    return StreamConfig(
        stream_name=stream_name,
        group_name=group_name,
        main_consumer_name=consumer_base,
        claiming_consumer_name=f"{consumer_base}-claiming",
        claiming_min_idle_time_ms=claiming_min_idle_time_ms,
    )


STREAM_CONFIGS: dict[str, StreamConfig] = {
    "demo": build_stream_config(
        stream_name="demo-best-practice-stream",
        group_name="demo-best-practice-group",
        claiming_min_idle_time_ms=60_000,
    )
}


DEMO_CONFIG = STREAM_CONFIGS["demo"]
