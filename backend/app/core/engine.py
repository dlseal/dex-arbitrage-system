import asyncio
import logging
from typing import List
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")

class RingQueue(asyncio.Queue):
    """环形队列：仅用于高频行情 (Ticks)，满时丢弃旧数据"""
    def put_nowait(self, item):
        try:
            super().put_nowait(item)
        except asyncio.QueueFull:
            # 队列满时，弹出一个旧的，压入一个新的
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                pass
            # 再次尝试，如果还是满（极低概率并发），则放弃本次 tick
            try:
                super().put_nowait(item)
            except asyncio.QueueFull:
                pass

class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy
        # 适当增大队列深度，避免微小的处理抖动导致丢包
        self.tick_queue = RingQueue(maxsize=200)
        self.event_queue = asyncio.Queue()
        self.running = False

    async def start(self):
        self.running = True
        logger.info(f"🚀 Engine Starting | Strategy: {self.strategy.name}")

        tasks = []
        for ex in self.exchanges:
            tasks.append(asyncio.create_task(self._safe_adapter_run(ex)))

        tasks.append(asyncio.create_task(self._tick_consumer()))
        tasks.append(asyncio.create_task(self._event_consumer()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Engine Stopped")

    async def _safe_adapter_run(self, adapter):
        while self.running:
            try:
                await adapter.listen_websocket(self.tick_queue, self.event_queue)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"💥 Adapter {adapter.name} crashed: {e}. Restarting in 5s...")
                if hasattr(adapter, 'close'):
                    try:
                        await adapter.close()
                    except:
                        pass
                await asyncio.sleep(5)
                # 重新初始化逻辑...

    async def _tick_consumer(self):
        """消费者 A: 仅处理高频行情"""
        while self.running:
            try:
                tick = await self.tick_queue.get()
                if self.strategy:
                    # 🔴 优化：移除 wait_for。
                    # 策略的 on_tick 必须是非阻塞的（只做计算和发起 async 任务，不 await IO）。
                    # 如果策略在这里阻塞，整个引擎变慢是符合预期的（背压）。
                    # 强行 timeout cancel 会导致策略内部状态（如 active_orders）损坏。
                    await self.strategy.on_tick(tick)
            except Exception as e:
                logger.error(f"Tick Error: {e}", exc_info=False)

    async def _event_consumer(self):
        """消费者 B: 仅处理核心事件 (成交、状态)"""
        logger.info("🛡️ Event Consumer (High Priority) Started")
        while self.running:
            try:
                event = await self.event_queue.get()
                # 核心事件必须记录日志
                # logger.info(f"📨 Processing Event: {event.get('type')} from {event.get('exchange')}")
                if self.strategy:
                    await self.strategy.on_tick(event)
            except Exception as e:
                logger.critical(f"❌ Event Error: {e}", exc_info=True)