import asyncio
import logging
from typing import List
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class EventEngine:
    def __init__(self, exchanges: List[BaseExchange]):
        self.exchanges = exchanges
        self.market_data_queue = asyncio.Queue()
        self.running = False

    async def start(self):
        """启动主循环"""
        self.running = True

        # 1. 创建 WebSocket 监听任务
        tasks = []
        for ex in self.exchanges:
            # 启动每个交易所的 WS 监听，将数据推送到 queue
            task = asyncio.create_task(ex.listen_websocket(self.market_data_queue))
            tasks.append(task)

        # 2. 创建消费者任务 (这里暂时只做打印，未来接入 Strategy)
        consumer_task = asyncio.create_task(self._data_consumer())
        tasks.append(consumer_task)

        logger.info(f"🚀 引擎已启动，监控 {len(self.exchanges)} 个交易所...")

        try:
            # 等待所有任务 (通常它们是无限循环)
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("引擎任务被取消")

    async def _data_consumer(self):
        """
        消费者：处理接收到的行情数据
        """
        logger.info("👀 消费者线程启动: 等待行情数据...")

        while self.running:
            # 从队列获取数据
            tick = await self.market_data_queue.get()

            # --- 这里是策略入口 ---
            # 简单打印：证明数据流是通的
            # 格式: [GRVT] BTC-USDT Bid:65000 Ask:65001
            print(f"⚡ [{tick['exchange']}] {tick['symbol']} \t| Bid: {tick['bid']} \t| Ask: {tick['ask']}")

            # 标记队列任务完成
            self.market_data_queue.task_done()