# backend/app/core/engine.py
import asyncio
import logging
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

        # 2. Event 队列 (成交/订单状态): 绝不能丢弃，容量设为极大 (10万)
        # 在高频场景下，如果消费者处理慢，这里充当缓冲区。
        # 如果 10万 还满了，说明系统设计有严重瓶颈， crash 是合理的。
        self.event_queue = asyncio.Queue(maxsize=100000)

        self.running = False

    async def start(self):
        self.running = True
        if self.strategy:
            logger.info(f"🚀 Engine Starting | Strategy: {self.strategy.name}")
        else:
            logger.warning("⚠️ Engine Starting WITHOUT Strategy!")

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
                wait_time = min(retry_count * 2, 60)  # 指数退避，最大 60s
                logger.error(f"💥 Adapter {adapter.name} CRASHED: {e}. Restarting in {wait_time}s...")

                # 尝试清理资源
                if hasattr(adapter, 'close'):
                    try:
                        await adapter.close()
                    except:
                        pass

                await asyncio.sleep(wait_time)

                # 尝试重连
                try:
                    await adapter.initialize()
                    logger.info(f"♻️ Adapter {adapter.name} Re-initialized.")
                except Exception as init_e:
                    logger.error(f"❌ Re-init failed: {init_e}")

    async def _tick_consumer(self):
        """处理高频行情数据"""
        while self.running:
            try:
                tick = await self.tick_queue.get()
                if self.strategy:
                    # 使用 Task 分发，防止单个 tick 处理阻塞后续 tick
                    # 注意：如果策略逻辑很重，这里会产生大量 Task，需注意
                    asyncio.create_task(self._safe_strategy_tick(tick))
            except Exception as e:
                logger.error(f"Tick Consumer Error: {e}", exc_info=True)

    async def _event_consumer(self):
        """处理关键交易事件 (成交回报) - 单线程顺序处理以保证状态一致性"""
        logger.info("🛡️ Event Consumer (High Priority) Started")
        while self.running:
            try:
                event = await self.event_queue.get()
                if self.strategy:
                    # 关键事件必须 await，确保顺序执行 (例如：先成交 -> 再补单)
                    await self._safe_strategy_tick(event)
            except Exception as e:
                logger.critical(f"❌ Event Consumer Error: {e}", exc_info=True)

    async def _safe_strategy_tick(self, event):
        try:
            if hasattr(self.strategy, 'on_tick'):
                # 这里的 on_tick 内部应该是非阻塞的
                await self.strategy.on_tick(event)
        except Exception as e:
            logger.error(f"Strategy on_tick Exception: {e}", exc_info=True)