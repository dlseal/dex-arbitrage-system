# backend/app/core/engine.py
import asyncio
import logging
import time
from typing import List
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class RingQueue(asyncio.Queue):
    """
    环形队列：用于 Tick 数据，满了自动丢弃旧数据，永远不会阻塞或报错。
    """

    def put_nowait(self, item):
        if self.full():
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            super().put_nowait(item)
        except asyncio.QueueFull:
            pass


class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy

        # 1. Tick 队列: 允许丢弃旧数据 (RingQueue)，容量 1000
        self.tick_queue = RingQueue(maxsize=1000)

        # 2. Event 队列 (成交/订单状态): 绝不能丢弃
        self.event_queue = asyncio.Queue(maxsize=100000)

        self.running = False
        self._tick_count = 0

    async def start(self):
        self.running = True
        strategy_name = self.strategy.name if self.strategy and hasattr(self.strategy, 'name') else "Unknown"
        logger.info(f"🚀 Engine Starting | Strategy: {strategy_name}")

        tasks = []
        # 启动所有交易所适配器
        for ex in self.exchanges:
            tasks.append(asyncio.create_task(self._safe_adapter_run(ex)))

        # 启动消费者
        tasks.append(asyncio.create_task(self._tick_consumer()))
        tasks.append(asyncio.create_task(self._event_consumer()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Engine Stopped")

    async def _safe_adapter_run(self, adapter):
        """保护性运行 Adapter，崩溃自动重启"""
        retry_count = 0
        while self.running:
            try:
                # 传入队列供 Adapter 写入
                await adapter.listen_websocket(self.tick_queue, self.event_queue)
            except asyncio.CancelledError:
                break
            except Exception as e:
                retry_count += 1
                wait_time = min(retry_count * 2, 60)
                logger.error(f"💥 Adapter {adapter.name} CRASHED: {e}. Restarting in {wait_time}s...")

                if hasattr(adapter, 'close'):
                    try:
                        await adapter.close()
                    except:
                        pass
                await asyncio.sleep(wait_time)
                try:
                    await adapter.initialize()
                    logger.info(f"♻️ Adapter {adapter.name} Re-initialized.")
                except Exception:
                    pass

    async def _tick_consumer(self):
        """处理高频行情数据"""
        logger.info("🌊 Tick Consumer Started (Waiting for data...)")
        last_log_ts = time.time()

        while self.running:
            try:
                # 增加 timeout 使得 loop 有机会检查 self.running
                try:
                    tick = await asyncio.wait_for(self.tick_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                self._tick_count += 1

                # [DEBUG] 每 50 个 Tick 或 10秒 打印一次日志，证明活着
                now = time.time()
                if self._tick_count == 1:
                    logger.info(f"⚡ FIRST TICK RECEIVED: {tick.get('symbol')} @ {tick.get('bid')}/{tick.get('ask')}")
                elif self._tick_count % 50 == 0 or (now - last_log_ts > 10):
                    logger.info(f"🌊 Processing Ticks... (Total: {self._tick_count} | Last: {tick.get('symbol')})")
                    last_log_ts = now

                if self.strategy:
                    # 使用 Task 分发
                    asyncio.create_task(self._safe_strategy_tick(tick))
            except Exception as e:
                logger.error(f"Tick Consumer Error: {e}", exc_info=True)

    async def _event_consumer(self):
        """处理关键交易事件"""
        logger.info("🛡️ Event Consumer (High Priority) Started")
        while self.running:
            try:
                try:
                    event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if self.strategy:
                    await self._safe_strategy_tick(event)
            except Exception as e:
                logger.critical(f"❌ Event Consumer Error: {e}", exc_info=True)

    async def _safe_strategy_tick(self, event):
        try:
            if hasattr(self.strategy, 'on_tick'):
                await self.strategy.on_tick(event)
        except Exception as e:
            logger.error(f"Strategy on_tick Exception: {e}", exc_info=True)