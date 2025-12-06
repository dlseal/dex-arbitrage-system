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

        # 1. 行情队列 (Tick Queue): RingQueue, Max 100, 允许丢包
        self.tick_queue = RingQueue(maxsize=100)

        # 2. 事件队列 (Event Queue): 无限容量, 严禁丢包 (用于成交回报、订单状态)
        self.event_queue = asyncio.Queue()

        self.running = False

    async def start(self):
        self.running = True
        logger.info(f"🚀 引擎启动 | 策略: {self.strategy.name if self.strategy else '无'}")
        logger.info("   - 队列架构: TickQueue(Ring) + EventQueue(Infinite)")

        tasks = []
        # 启动适配器，传入两个队列
        for ex in self.exchanges:
            tasks.append(asyncio.create_task(
                ex.listen_websocket(self.tick_queue, self.event_queue)
            ))

        # 启动消费者
        tasks.append(asyncio.create_task(self._data_consumer()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("引擎停止")

    async def _data_consumer(self):
        logger.info("🧠 策略大脑已上线，双通道监听中...")

        while self.running:
            # 优先处理事件队列 (成交回报)
            # 使用 wait 机制，优先服务 event_queue，闲时服务 tick_queue

            # 创建读取任务
            task_event = asyncio.create_task(self.event_queue.get())
            task_tick = asyncio.create_task(self.tick_queue.get())

            done, pending = await asyncio.wait(
                [task_event, task_tick],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in done:
                data = task.result()

                # 如果是 Tick 任务完成
                if task == task_tick:
                    # 如果此时 Event 队列也有数据，优先取 Event (虽然 wait 已返回，但防止积压)
                    # 这里简化处理，直接透传
                    pass
                else:
                    # 如果是 Event 任务完成，取消 Tick 等待 (因为 Event 优先级高，处理完立即进入下一轮)
                    task_tick.cancel()

                if self.strategy:
                    try:
                        await self.strategy.on_tick(data)
                    except Exception as e:
                        logger.error(f"❌ 策略执行报错: {e}", exc_info=True)

            # 清理未完成的任务 (Tick 任务可能被取消)
            for task in pending:
                task.cancel()