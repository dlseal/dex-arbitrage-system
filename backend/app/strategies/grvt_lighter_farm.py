import asyncio
import logging
import time
from typing import Dict, Any, Optional
from app.config import Config

logger = logging.getLogger("SmartFarm_GL")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_v2"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 状态管理
        self.active_orders: Dict[str, Dict[str, float]] = {}  # symbol -> {order_id: price}
        self.last_quote_time: Dict[str, float] = {}
        self.hedge_queue = asyncio.Queue()

        # 配置读取
        self.spread = Config.SPREAD_THRESHOLD  # 利润差价
        self.max_slippage = getattr(Config, 'MAX_SLIPPAGE_TOLERANCE', -0.0005)
        self.requote_threshold = getattr(Config, 'REQUOTE_THRESHOLD', 0.0005)

        # 启动对冲消费者
        asyncio.create_task(self._hedge_consumer())

        logger.info(f"🛡️ Strategy Ready. Spread: {self.spread * 100}% | Slippage Tol: {self.max_slippage * 100}%")

    async def on_tick(self, event: dict):
        try:
            event_type = event.get('type', 'tick')
            if event_type == 'trade':
                await self._process_trade_fill(event)
            elif event_type == 'tick':
                await self._process_tick(event)
        except Exception as e:
            logger.error(f"Strategy Error: {e}")

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        # 仅当两边都有行情时才计算
        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            # 简单的时间同步检查 (2秒内)
            t1 = self.tickers[symbol]['Lighter']['ts']
            t2 = self.tickers[symbol]['GRVT']['ts']
            if abs(t1 - t2) > 2000:
                return

            # 限制挂单频率 (每 1.5 秒一次)
            now = time.time()
            if now - self.last_quote_time.get(symbol, 0) > 1.5:
                self.last_quote_time[symbol] = now
                await self._manage_maker_orders(symbol)

    async def _process_trade_fill(self, trade: dict):
        """处理 GRVT 成交 -> 触发 Lighter 对冲"""
        if trade['exchange'] != 'GRVT': return

        symbol = trade['symbol']
        side = trade['side']  # BUY or SELL
        size = float(trade['size'])
        price = float(trade['price'])

        order_id = trade.get('order_id')
        logger.info(f"⚡️ [FILLED] GRVT {side} {size} @ {price} (ID: {order_id})")

        # 无论是不是我们内存记录的单子，只要是 GRVT 成交了，就去对冲
        # 因为可能是重启前的挂单成交了
        await self.hedge_queue.put({
            'symbol': symbol,
            'side': 'SELL' if side == 'BUY' else 'BUY',  # 反向对冲
            'size': size,
            'reason': f"Hedge for GRVT {side} @ {price}"
        })

        # 清理本地记录的订单状态
        if symbol in self.active_orders:
            # 简单粗暴：有成交就清空该币种所有挂单记录，下一轮 tick 会重新挂
            # 这样可以防止重复挂单
            self.active_orders.pop(symbol, None)

    async def _manage_maker_orders(self, symbol: str):
        """核心做市逻辑：基于 Lighter 价格，在 GRVT 挂单"""
        lighter_tick = self.tickers[symbol]['Lighter']

        # 获取参考价格 (Lighter 的买一卖一)
        ref_bid = lighter_tick['bid']
        ref_ask = lighter_tick['ask']

        if ref_bid <= 0 or ref_ask <= 0: return

        # 计算目标挂单价格
        # 策略：
        # 在 GRVT 挂买单 = Lighter卖单(对手价) * (1 - 利润 - 手续费预留)
        # 在 GRVT 挂卖单 = Lighter买单(对手价) * (1 + 利润 + 手续费预留)

        # 注意：Config.FARM_SIDE 可以控制单边刷量，也可以双边
        farm_side = Config.FARM_SIDE.upper()  # BUY, SELL, or BOTH

        target_orders = []  # (side, price)

        if farm_side in ['BUY', 'BOTH']:
            # 我们想在 GRVT 买，去 Lighter 卖
            # 只有当 (Lighter_Bid - GRVT_Ask) > Spread 时才有套利空间
            # 此时我们作为 Maker，要在 GRVT 挂一个比 "Lighter Bid" 低的价格
            # 目标价格 = Lighter_Bid * (1 - target_spread)
            target_buy = ref_bid * (1 - self.spread)
            target_orders.append(('BUY', target_buy))

        if farm_side in ['SELL', 'BOTH']:
            # 我们想在 GRVT 卖，去 Lighter 买
            # 目标价格 = Lighter_Ask * (1 + target_spread)
            target_sell = ref_ask * (1 + self.spread)
            target_orders.append(('SELL', target_sell))

        # 检查是否需要更新订单
        current_orders = self.active_orders.get(symbol, {})

        # 如果没有订单，直接挂
        if not current_orders and target_orders:
            await self._place_orders(symbol, target_orders)
            return

        # 如果有订单，检查价格偏差
        should_cancel = False
        # 这里简化处理：只看第一个单子 (假设单边模式)
        # 如果是双边，建议写更复杂的 diff
        for oid, old_price in current_orders.items():
            # 找到对应方向的目标价
            # 简单起见，如果偏差超过阈值，就全撤全挂
            closest_target = target_orders[0][1]  # 这是一个简化的假设
            diff = abs(old_price - closest_target) / closest_target
            if diff > self.requote_threshold:
                should_cancel = True
                break

        if should_cancel:
            await self._cancel_all(symbol)
            await self._place_orders(symbol, target_orders)

    async def _place_orders(self, symbol, targets):
        adapter = self.adapters['GRVT']
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        tasks = []
        prices = []
        for side, price in targets:
            tasks.append(adapter.create_order(
                symbol=f"{symbol}-USDT",
                side=side,
                amount=quantity,
                price=price
            ))
            prices.append(price)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        if symbol not in self.active_orders: self.active_orders[symbol] = {}

        success_count = 0
        for res, price in zip(results, prices):
            if isinstance(res, str) and res:
                self.active_orders[symbol][res] = price
                success_count += 1
            elif isinstance(res, Exception):
                logger.warning(f"Order failed: {res}")

        if success_count > 0:
            logger.info(f"🌊 [Re-Quote] {symbol} Placed {success_count} orders near {prices[0]:.2f}")

    async def _cancel_all(self, symbol):
        orders = self.active_orders.get(symbol, {})
        if not orders: return

        ids = list(orders.keys())
        tasks = [self.adapters['GRVT'].cancel_order(oid) for oid in ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.active_orders[symbol] = {}

    async def _hedge_consumer(self):
        """消费者：串行处理对冲，防止并发导致的资金不足"""
        while True:
            item = await self.hedge_queue.get()
            symbol = item['symbol']
            side = item['side']
            size = item['size']

            try:
                logger.info(f"🚀 Executing Hedge: {side} {size} on Lighter...")

                # 获取执行价格 (Market Order 不需要太精确，但为了估算...)
                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    logger.error("❌ Hedge failed: No Lighter ticker data")
                    continue

                ref_price = lighter_tick['bid'] if side == 'SELL' else lighter_tick['ask']
                if ref_price <= 0:
                    logger.error(f"❌ Hedge failed: Invalid Lighter price {ref_price}")
                    continue

                # 5% 滑点保护价格
                limit_price = ref_price * 0.95 if side == 'SELL' else ref_price * 1.05

                # 调用 Lighter 下单
                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol,
                    side=side,
                    amount=size,
                    price=limit_price,
                    order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ Hedge Filled! ID: {order_id}")
                else:
                    logger.error("❌ Hedge Order Failed (Return None)")

            except Exception as e:
                logger.error(f"❌ Hedge Exception: {e}")

            # 简单的速率限制
            await asyncio.sleep(0.5)