import asyncio
import logging
from typing import List, Optional
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class EventEngine:
    def __init__(self, exchanges: List[BaseExchange], strategy=None):
        self.exchanges = exchanges
        self.strategy = strategy  # 接收策略实例
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

        # 等待运行
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("引擎停止")

    async def _data_consumer(self):
        """消费者：将数据喂给策略"""
        logger.info("🧠 策略大脑已上线，正在扫描市场...")

        while self.running:
            tick = await self.market_data_queue.get()

            # --- 简单的控制台心跳 (防止觉得程序死了) ---
            # 只打印 BTC 的心跳，减少刷屏，或者您可以注释掉这行
            if tick['symbol'] in ['BTC', 'BTC-USDT'] and int(tick['ts']) % 10 == 0:
                print(f".", end="", flush=True)

            # --- 核心：推送给策略 ---
            if self.strategy:
                await self.strategy.on_tick(tick)

            self.market_data_queue.task_done()