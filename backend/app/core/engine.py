import asyncio
import logging
from typing import List
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")

class RingQueue(asyncio.Queue):
    """环形队列：仅用于高频行情 (Ticks)，满时丢弃旧数据"""
    def put_nowait(self, item):
        if self.full():
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                pass
        super().put_nowait(item)

class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy
        self.tick_queue = RingQueue(maxsize=100)
        self.event_queue = asyncio.Queue() # 无限容量，严禁丢包
        self.running = False

    async def start(self):
        self.running = True
        logger.info(f"🚀 Engine Starting | Strategy: {self.strategy.name}")

        tasks = []
        # 1. 启动所有适配器
        for ex in self.exchanges:
            tasks.append(asyncio.create_task(
                self._safe_adapter_run(ex)
            ))

        # 2. 启动数据处理 (拆分为两个独立任务)
        tasks.append(asyncio.create_task(self._tick_consumer()))
        tasks.append(asyncio.create_task(self._event_consumer()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Engine Stopped")

    async def _safe_adapter_run(self, adapter):
        """保护性运行适配器，断线自动重启"""
        while self.running:
            try:
                await adapter.listen_websocket(self.tick_queue, self.event_queue)
            except Exception as e:
                logger.error(f"💥 Adapter {adapter.name} crashed: {e}. Restarting in 5s...")
                await asyncio.sleep(5)
                try:
                    await adapter.initialize() # 尝试重新初始化
                except:
                    pass

    async def _tick_consumer(self):
        """消费者 A: 仅处理高频行情"""
        while self.running:
            try:
                tick = await self.tick_queue.get()
                if self.strategy:
                    await self.strategy.on_tick(tick)
            except Exception as e:
                logger.error(f"Tick Error: {e}")

    async def _event_consumer(self):
        """消费者 B: 仅处理核心事件 (成交、状态)"""
        logger.info("🛡️ Event Consumer (High Priority) Started")
        while self.running:
            try:
                event = await self.event_queue.get()
                # ✅ 修复点：成交回报优先级最高，且绝不因为行情繁忙而被 Cancel
                logger.info(f"📨 Processing Event: {event.get('type')} from {event.get('exchange')}")
                if self.strategy:
                    await self.strategy.on_tick(event) # 调用策略处理
            except Exception as e:
                logger.critical(f"❌ Event Error: {e}", exc_info=True)