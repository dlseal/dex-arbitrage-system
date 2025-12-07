import asyncio
import logging
from typing import List
from app.adapters.base import BaseExchange

logger = logging.getLogger("Engine")


class RingQueue(asyncio.Queue):
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
        self.tick_queue = RingQueue(maxsize=1000)
        self.event_queue = asyncio.Queue(maxsize=10000)
        self.running = False

    async def start(self):
        self.running = True
        if self.strategy:
            logger.info(f"🚀 Engine Starting | Strategy: {self.strategy.name}")
        else:
            logger.warning("⚠️ Engine Starting WITHOUT Strategy!")

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
        retry_count = 0
        while self.running:
            try:
                await adapter.listen_websocket(self.tick_queue, self.event_queue)
            except asyncio.CancelledError:
                break
            except Exception as e:
                retry_count += 1
                wait_time = min(retry_count * 2, 30)
                logger.error(f"💥 Adapter {adapter.name} crashed: {e}. Restarting in {wait_time}s...")

                if hasattr(adapter, 'close'):
                    try:
                        await adapter.close()
                    except:
                        pass

                await asyncio.sleep(wait_time)
                try:
                    await adapter.initialize()
                except Exception as init_e:
                    logger.error(f"❌ Re-init failed: {init_e}")

    async def _tick_consumer(self):
        while self.running:
            try:
                tick = await self.tick_queue.get()
                if self.strategy:
                    asyncio.create_task(self._safe_strategy_tick(tick))
            except Exception as e:
                logger.error(f"Tick Consumer Error: {e}", exc_info=True)

    async def _event_consumer(self):
        logger.info("🛡️ Event Consumer (High Priority) Started")
        while self.running:
            try:
                event = await self.event_queue.get()
                if self.strategy:
                    await self._safe_strategy_tick(event)
            except Exception as e:
                logger.critical(f"❌ Event Consumer Error: {e}", exc_info=True)

    async def _safe_strategy_tick(self, event):
        try:
            await self.strategy.on_tick(event)
        except Exception as e:
            logger.error(f"Strategy on_tick Exception: {e}", exc_info=True)