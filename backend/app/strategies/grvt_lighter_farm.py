import asyncio
import logging
import time
from typing import Dict, Any, List
from app.config import Config

logger = logging.getLogger("SmartFarm_GL")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_Pro_v3_Fixed"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 状态管理: symbol -> {client_order_id: price}
        self.active_orders: Dict[str, Dict[str, float]] = {}

        # 频率限制
        self.last_quote_time: Dict[str, float] = {}

        # 重置锁
        self.resetting_symbols = set()

        # 对冲队列
        self.hedge_queue = asyncio.Queue()

        # 配置
        self.spread = Config.SPREAD_THRESHOLD
        self.requote_threshold = getattr(Config, 'REQUOTE_THRESHOLD', 0.0005)

        asyncio.create_task(self._hedge_consumer())

        logger.info(f"🛡️ Strategy Ready | Spread: {self.spread * 100:.2f}%")

    async def on_tick(self, event: dict):
        try:
            event_type = event.get('type', 'tick')
            if event_type == 'trade':
                await self._process_trade_fill(event)
            elif event_type == 'tick':
                await self._process_tick(event)
        except Exception as e:
            logger.error(f"Strategy Error: {e}", exc_info=True)

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if symbol in self.resetting_symbols: return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            # 1. 检查数据新鲜度 (5秒内)
            t1 = self.tickers[symbol]['Lighter']['ts']
            t2 = self.tickers[symbol]['GRVT']['ts']
            if abs(t1 - t2) > 5000: return

            # 2. 限制频率 (1秒)
            now = time.time()
            if now - self.last_quote_time.get(symbol, 0) > 1.0:
                self.last_quote_time[symbol] = now
                await self._manage_maker_orders(symbol)

    async def _process_trade_fill(self, trade: dict):
        if trade['exchange'] != 'GRVT': return

        symbol = trade['symbol']
        side = trade['side']
        size = float(trade['size'])
        price = float(trade['price'])
        order_id = str(trade.get('order_id', 'unknown'))

        logger.info(f"⚡️ [FILLED] GRVT {side} {size} @ {price} (ID: {order_id})")

        # 1. 对冲
        await self.hedge_queue.put({
            'symbol': symbol,
            'side': 'SELL' if side == 'BUY' else 'BUY',
            'size': size,
            'reason': f"Hedge for GRVT {side} @ {price}"
        })

        # 2. 状态重置：触发清理逻辑（必须清理旧单）
        self.resetting_symbols.add(symbol)

        # ⚠️ 修复：不要立即简单 pop，而是交给 cleanup 函数去处理撤单
        # self.active_orders.pop(symbol, None)

        asyncio.create_task(self._cleanup_after_fill(symbol))

    async def _cleanup_after_fill(self, symbol: str):
        """成交后清理：撤销所有剩余挂单并重置状态"""
        try:
            logger.info(f"🧹 [Cleanup] Fill detected for {symbol}. Cancelling remaining orders...")

            # 1. 强制撤销该币种所有本地记录的订单
            if symbol in self.active_orders and self.active_orders[symbol]:
                await self._cancel_orders(symbol, list(self.active_orders[symbol].keys()))

            # 2. 清空本地状态
            self.active_orders[symbol] = {}

        except Exception as e:
            logger.error(f"❌ Cleanup Error: {e}")
        finally:
            # 3. 暂停一会再恢复挂单
            await asyncio.sleep(2.0)
            self.resetting_symbols.discard(symbol)
            logger.info(f"▶️ [Resume] Resuming quotes for {symbol}")

    async def _manage_maker_orders(self, symbol: str):
        lighter_tick = self.tickers[symbol]['Lighter']
        ref_bid = lighter_tick['bid']
        ref_ask = lighter_tick['ask']
        if ref_bid <= 0 or ref_ask <= 0: return

        farm_side = Config.FARM_SIDE.upper()
        target_orders = []

        # 计算目标价
        if farm_side in ['BUY', 'BOTH']:
            target_orders.append(('BUY', ref_bid * (1 - self.spread)))
        if farm_side in ['SELL', 'BOTH']:
            target_orders.append(('SELL', ref_ask * (1 + self.spread)))

        current_orders = self.active_orders.get(symbol, {})

        # 如果没有挂单，直接挂
        if not current_orders:
            if target_orders:
                await self._place_orders(symbol, target_orders)
            return

        # 检查是否需要撤单重挂
        should_cancel = False
        if len(current_orders) != len(target_orders):
            should_cancel = True
        else:
            current_prices = list(current_orders.values())
            # 简单的价格对比逻辑
            for _, target_p in target_orders:
                # 检查是否存在价格接近的订单
                if not any(abs(cp - target_p) / target_p < self.requote_threshold for cp in current_prices):
                    should_cancel = True
                    break

        if should_cancel:
            ids = list(current_orders.keys())
            if ids:
                logger.info(f"♻️ [Requote] Deviation detected. Cancelling {len(ids)} orders...")
                # ⚠️ 修复：先执行撤单，再清空状态，再挂新单
                await self._cancel_orders(symbol, ids)

            # 无论撤单成功与否（Adapter可能吞异常），我们都清除本地状态以避免死锁，
            # 并重新挂单。如果撤单真的失败，这里确实会造成双挂，
            # 但至少我们现在尝试了显式撤单。
            self.active_orders[symbol] = {}
            await self._place_orders(symbol, target_orders)

    async def _cancel_orders(self, symbol: str, order_ids: List[str]):
        """封装撤单逻辑"""
        if not order_ids: return
        try:
            tasks = [self.adapters['GRVT'].cancel_order(oid) for oid in order_ids]
            # 等待所有撤单请求发送完成
            await asyncio.gather(*tasks, return_exceptions=True)
            # logger.info(f"🗑️ Sent cancel for {order_ids}")
        except Exception as e:
            logger.error(f"❌ Cancel Failed: {e}")

    async def _place_orders(self, symbol, targets):
        adapter = self.adapters['GRVT']
        qty = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        tasks = []
        prices = []
        for side, price in targets:
            tasks.append(adapter.create_order(f"{symbol}-USDT", side, qty, price))
            prices.append(price)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        if symbol not in self.active_orders: self.active_orders[symbol] = {}

        success = 0
        for res, price in zip(results, prices):
            if isinstance(res, str) and res:
                self.active_orders[symbol][res] = price
                success += 1
            else:
                logger.warning(f"⚠️ Order placement failed or returned None: {res}")

        if success > 0:
            logger.info(f"🌊 [Quote] {symbol} Placed {success} orders near {prices[0]:.2f}")

    async def _hedge_consumer(self):
        logger.info("🚀 Hedge Consumer Started")
        while True:
            if self.hedge_queue.empty():
                await asyncio.sleep(0.01)
                continue

            item = await self.hedge_queue.get()
            symbol = item['symbol']
            side = item['side']
            size = item['size']

            try:
                # 简单市价对冲
                tick = self.tickers.get(symbol, {}).get('Lighter')
                if not tick:
                    logger.warning(f"⚠️ No Lighter tick for hedge {symbol}")
                    continue

                ref_p = tick['bid'] if side == 'SELL' else tick['ask']
                if ref_p <= 0:
                    logger.warning(f"⚠️ Invalid Lighter price for hedge {symbol}")
                    continue

                limit_p = ref_p * 0.95 if side == 'SELL' else ref_p * 1.05

                logger.info(f"🛡️ Hedging: {side} {size} on Lighter...")
                await self.adapters['Lighter'].create_order(symbol, side, size, limit_p, "MARKET")
                logger.info("✅ Hedge Sent")
            except Exception as e:
                logger.error(f"❌ Hedge Error: {e}")