import asyncio
import logging
from typing import List, Optional
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy
        self.market_data_queue = asyncio.Queue()
        self.running = False

    async def start(self):
        """启动引擎"""
        self.running = True
        logger.info(f"🚀 引擎启动，绑定策略: {self.strategy.name if self.strategy else '无'}")

        # 1. 启动所有适配器的 WS 监听
        tasks = []
        for ex in self.exchanges:
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

        # 计数器
        msg_count = 0

        while self.running:
            # 等待数据
            tick = await self.market_data_queue.get()
            msg_count += 1

            if msg_count <= 20 or msg_count % 50 == 0:
                print(
                    f"🕵️ [DEBUG] 引擎收到数据: Exchange={tick['exchange']} | Symbol={tick['symbol']} | Bid={tick['bid']}")

            # 推送给策略
            if self.strategy:
                await self.strategy.on_tick(tick)

            self.market_data_queue.task_done()