import asyncio
import logging
from typing import List, Optional
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class RingQueue(asyncio.Queue):
    """
    环形队列：当队列满时，自动丢弃最早的数据，腾出空间给新数据。
    适用于高频行情处理，确保策略永远只处理最新的 Tick，而不是处理堆积的旧数据。
    """

    def put_nowait(self, item):
        if self.full():
            try:
                # 扔掉最旧的一个数据
                _ = self.get_nowait()
                # logger.debug("⚠️ [Engine] 队列已满，丢弃了一条旧行情数据")
            except asyncio.QueueEmpty:
                pass
        super().put_nowait(item)


class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy

        # 使用自定义的环形队列，容量设为 100
        # 意味着如果策略处理不过来，最多只堆积 100 个最新 tick，更早的直接丢弃
        self.market_data_queue = RingQueue(maxsize=100)

        self.running = False

    async def start(self):
        """启动引擎"""
        self.running = True
        logger.info(f"🚀 引擎启动，绑定策略: {self.strategy.name if self.strategy else '无'}")
        logger.info(f"   - 队列模式: RingQueue (Max 100), 溢出自动丢弃旧数据")

        # 1. 启动所有适配器的 WS 监听
        tasks = []
        for ex in self.exchanges:
            # 适配器内部调用的 put_nowait 会自动触发 RingQueue 的丢弃逻辑
            tasks.append(asyncio.create_task(ex.listen_websocket(self.market_data_queue)))

        # 2. 启动数据消费者
        tasks.append(asyncio.create_task(self._data_consumer()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("引擎停止")

    async def _data_consumer(self):
        """消费者：将数据喂给策略"""
        logger.info("🧠 策略大脑已上线，正在等待数据流入...")

        msg_count = 0

        while self.running:
            # 等待数据
            tick = await self.market_data_queue.get()
            msg_count += 1

            if msg_count <= 5 or msg_count % 100 == 0:
                logger.debug(f"Tick received: {tick.get('symbol')} from {tick.get('exchange')}")

            # 推送给策略
            if self.strategy:
                try:
                    await self.strategy.on_tick(tick)
                except Exception as e:
                    logger.error(f"❌ 策略执行报错: {e}", exc_info=True)

            self.market_data_queue.task_done()