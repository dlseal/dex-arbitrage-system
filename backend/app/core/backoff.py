import asyncio
import time
import logging
from typing import Dict, Optional, Callable

logger = logging.getLogger("Backoff")


class ErrorBackoffController:
    """
    指数退避控制器
    职责：管理交易所 API 错误频率，触发熔断等待，防止 IP 被 ban。
    """

    def __init__(self):
        self._error_counts: Dict[str, int] = {}
        self._next_available_time: Dict[str, float] = {}
        self._reset_timers: Dict[str, asyncio.TimerHandle] = {}

        # 配置
        self.initial_backoff = 1.0  # 初始等待 1秒
        self.max_backoff = 60.0  # 最大等待 60秒
        self.reset_interval = 300.0  # 5分钟无报错则重置计数

    def can_request(self, exchange_name: str) -> bool:
        """检查当前是否允许发送请求"""
        return time.time() >= self._next_available_time.get(exchange_name, 0)

    async def wait_permission(self, exchange_name: str):
        """
        [阻塞] 等待直到获得请求权限
        如果在冷却期内，会 sleep 到冷却结束。
        """
        wait_time = self._next_available_time.get(exchange_name, 0) - time.time()
        if wait_time > 0:
            logger.warning(f"⏳ {exchange_name} 处于冷却期，等待 {wait_time:.2f}s...")
            await asyncio.sleep(wait_time)

    def record_error(self, exchange_name: str):
        """
        记录一次错误，触发指数退避
        """
        count = self._error_counts.get(exchange_name, 0) + 1
        self._error_counts[exchange_name] = count

        # 指数退避算法: 1, 2, 4, 8, ... max 60
        backoff_time = min(self.initial_backoff * (2 ** (count - 1)), self.max_backoff)
        self._next_available_time[exchange_name] = time.time() + backoff_time

        logger.error(f"💥 {exchange_name} 错误计数: {count} | 冷却: {backoff_time}s")

        # 刷新重置定时器
        self._schedule_reset(exchange_name)

    def record_success(self, exchange_name: str):
        """
        记录一次成功（可选：用于快速恢复，这里采用定时自动恢复）
        """
        pass

    def _schedule_reset(self, exchange_name: str):
        """重置错误计数的定时任务"""
        loop = asyncio.get_running_loop()
        if exchange_name in self._reset_timers:
            self._reset_timers[exchange_name].cancel()

        self._reset_timers[exchange_name] = loop.call_later(
            self.reset_interval,
            self._reset_counter,
            exchange_name
        )

    def _reset_counter(self, exchange_name: str):
        if exchange_name in self._error_counts:
            logger.info(f"♻️ {exchange_name} 错误计数器已重置")
            del self._error_counts[exchange_name]
            del self._next_available_time[exchange_name]
            del self._reset_timers[exchange_name]