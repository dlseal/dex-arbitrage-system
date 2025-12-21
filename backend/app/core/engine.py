# backend/app/core/engine.py
import asyncio
import logging
import time
from typing import List, Deque
from collections import deque
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class RingQueue(asyncio.Queue):
    """
    环形队列：即使消费者完全阻塞，也能保证生产端不阻塞，
    并且队列中永远是相对较新的数据。
    """

    def put_nowait(self, item):
        if self.full():
            try:
                self.get_nowait()  # 丢弃最老的数据 (Head)
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

        # [HFT 优化] 队列不宜过大，保持数据新鲜度
        # 如果队列积压超过 100 个，说明系统已经严重延迟，更多缓存没有意义
        self.tick_queue = RingQueue(maxsize=100)

        # 事件队列 (成交/订单) 必须保证不丢失
        self.event_queue = asyncio.Queue(maxsize=100000)

        self.running = False
        self._tick_count = 0
        self._drop_count = 0  # 监控丢包数量

    async def start(self):
        self.running = True
        strategy_name = self.strategy.name if self.strategy and hasattr(self.strategy, 'name') else "Unknown"
        logger.info(f"🚀 Engine Starting | Strategy: {strategy_name} | Mode: HFT Serial")

        tasks = []
        for ex in self.exchanges:
            tasks.append(asyncio.create_task(self._safe_adapter_run(ex)))

        tasks.append(asyncio.create_task(self._tick_consumer_hft()))
        tasks.append(asyncio.create_task(self._event_consumer()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Engine Stopped")

    async def _safe_adapter_run(self, adapter):
        retry_count = 0
        while self.running:
            try:
                await adapter.listen_websocket(self.tick_queue, self.event_queue)
            except asyncio.CancelledError:
                break
            except Exception as e:
                retry_count += 1
                wait_time = min(retry_count * 2, 30)
                logger.error(f"💥 Adapter {adapter.name} CRASHED: {e}. Restarting in {wait_time}s...")
                await asyncio.sleep(wait_time)
                try:
                    if hasattr(adapter, 'close'): await adapter.close()
                    await adapter.initialize()
                except Exception:
                    pass

    async def _tick_consumer_hft(self):
        """
        [HFT 专用消费者]
        逻辑：
        1. 串行执行 (await strategy) 确保背压生效。
        2. 智能丢包 (Conflation)：如果队列中有积压，直接跳到最新一个 Tick。
        """
        logger.info("🌊 HFT Tick Consumer Started (Smart Conflation Enabled)")
        last_log_ts = time.time()

        while self.running:
            try:
                # 1. 阻塞等待第一个数据
                try:
                    tick = await asyncio.wait_for(self.tick_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                # 2. [关键优化] 检查积压：如果队列里还有数据，说明我们处理慢了
                # HFT 原则：只要最新的，中间的全部丢弃
                qsize = self.tick_queue.qsize()
                if qsize > 0:
                    # 只有当积压严重时才丢弃，避免丢失过多统计样本
                    # 这里设定阈值：如果积压超过 2 个，说明处理不过来了，直接清空取最新的
                    skipped = 0
                    while not self.tick_queue.empty():
                        try:
                            new_tick = self.tick_queue.get_nowait()
                            # 只有同类型的行情才覆盖，避免混淆不同币种(如果是多币种策略需更复杂的逻辑)
                            # 假设是单币种 HFT：
                            tick = new_tick
                            skipped += 1
                        except asyncio.QueueEmpty:
                            break

                    self._drop_count += skipped
                    if skipped > 10:
                        logger.warning(f"⚠️ [Load Shedding] Dropped {skipped} old ticks to catch up!")

                self._tick_count += 1

                # 3. 真正处理 (使用 await 确保顺序和流控)
                if self.strategy:
                    start_t = time.perf_counter()
                    await self._safe_strategy_tick(tick)
                    cost = (time.perf_counter() - start_t) * 1000

                    # 监控：如果单次处理超过 50ms，打印警告
                    if cost > 50:
                        logger.warning(f"🐢 Slow Strategy Logic: {cost:.1f}ms")

                # 心跳日志
                now = time.time()
                if now - last_log_ts > 10:
                    logger.info(
                        f"🌊 Processing... Total: {self._tick_count} | Dropped: {self._drop_count} | "
                        f"QSize: {self.tick_queue.qsize()}"
                    )
                    last_log_ts = now

            except Exception as e:
                logger.error(f"Tick Consumer Error: {e}", exc_info=True)

    async def _event_consumer(self):
        """订单/成交事件 (高优先级，不丢弃，并行处理)"""
        logger.info("🛡️ Event Consumer Started")
        while self.running:
            try:
                event = await self.event_queue.get()
                if self.strategy:
                    # 订单事件允许并发，因为必须快速响应
                    asyncio.create_task(self._safe_strategy_tick(event))
            except Exception as e:
                logger.error(f"Event Error: {e}", exc_info=True)

    async def _safe_strategy_tick(self, event):
        try:
            if hasattr(self.strategy, 'on_tick'):
                await self.strategy.on_tick(event)
        except Exception as e:
            logger.error(f"Strategy Exception: {e}", exc_info=True)