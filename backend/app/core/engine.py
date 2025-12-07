import asyncio
import logging
from typing import List
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class RingQueue(asyncio.Queue):
    """环形队列：仅用于高频行情 (Ticks)，满时丢弃旧数据"""

    def put_nowait(self, item):
        # 优化：不依赖 self.full() 的非原子性判断，直接尝试 put，捕获 Full 异常
        try:
            super().put_nowait(item)
        except asyncio.QueueFull:
            try:
                self.get_nowait()  # 移除最旧的一个
            except asyncio.QueueEmpty:
                pass  # 极低概率竞态，忽略

            # 再次尝试推入，如果还满则丢弃本次 tick (保护内存)
            try:
                super().put_nowait(item)
            except asyncio.QueueFull:
                pass


class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy
        self.tick_queue = RingQueue(maxsize=100)
        self.event_queue = asyncio.Queue()  # 无限容量，严禁丢包
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

        # 2. 启动数据处理
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
            except asyncio.CancelledError:
                # 外部停止信号，优雅退出
                break
            except Exception as e:
                logger.error(f"💥 Adapter {adapter.name} crashed: {e}. Restarting in 5s...")

                # ✅ 新增：显式调用关闭/清理方法，防止 socket 泄漏
                if hasattr(adapter, 'close'):
                    try:
                        await adapter.close()
                    except:
                        pass

                await asyncio.sleep(5)
                try:
                    await adapter.initialize()  # 尝试重新初始化
                except Exception as init_e:
                    logger.error(f"❌ Adapter {adapter.name} re-init failed: {init_e}")

    async def _tick_consumer(self):
        """消费者 A: 仅处理高频行情"""
        while self.running:
            try:
                tick = await self.tick_queue.get()
                if self.strategy:
                    # 增加超时保护，防止策略逻辑阻塞行情处理
                    await asyncio.wait_for(self.strategy.on_tick(tick), timeout=0.5)
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Strategy tick processing too slow: {tick.get('symbol')}")
            except Exception as e:
                logger.error(f"Tick Error: {e}", exc_info=False)  # 减少刷屏

    async def _event_consumer(self):
        """消费者 B: 仅处理核心事件 (成交、状态)"""
        logger.info("🛡️ Event Consumer (High Priority) Started")
        while self.running:
            try:
                event = await self.event_queue.get()
                logger.info(f"📨 Processing Event: {event.get('type')} from {event.get('exchange')}")
                if self.strategy:
                    await self.strategy.on_tick(event)
            except Exception as e:
                logger.critical(f"❌ Event Error: {e}", exc_info=True)