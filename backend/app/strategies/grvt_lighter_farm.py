import asyncio
import logging
import time
from typing import Dict, Any, Optional
from app.config import Config

logger = logging.getLogger("SmartFarm_v10")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_SmartFarm_Pro_v10"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # --- 状态管理 ---
        # 只维护单边最优单: {symbol: order_id}
        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}
        self.order_create_time: Dict[str, float] = {}

        # --- 并发锁 ---
        # 相比 v9，这里优化锁粒度，防止死锁
        self.locks: Dict[str, asyncio.Lock] = {}

        self.last_quote_time: Dict[str, float] = {}

        # --- 方向轮动管理 ---
        # 初始方向读取配置，后续自动翻转
        self.symbol_sides: Dict[str, str] = {}
        self.initial_side = Config.FARM_SIDE.upper()

        # --- 核心参数 ---
        self.target_margin = Config.MAX_SLIPPAGE_TOLERANCE  # 使用滑点配置作为利润/成本目标
        self.requote_threshold = getattr(Config, 'REQUOTE_THRESHOLD', 0.0005)
        self.REQUIRED_DEPTH_RATIO = 1.5  # 深度风控倍数

        logger.info(f"🛡️ SmartFarm v10 启动 | 初始: {self.initial_side} | 风控: On | Margin: {self.target_margin}")

    def _get_lock(self, symbol: str):
        if symbol not in self.locks:
            self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    def _get_current_side(self, symbol: str) -> str:
        # 如果配置是 BOTH，则这里需要特殊处理，但 SmartFarm 逻辑天然是单边的
        # 这里简化：如果是 BOTH，初始随机一边或默认 BUY，然后轮动
        return self.symbol_sides.get(symbol, 'BUY' if self.initial_side == 'BOTH' else self.initial_side)

    def _flip_side(self, symbol: str):
        current = self._get_current_side(symbol)
        new_side = 'SELL' if current == 'BUY' else 'BUY'
        self.symbol_sides[symbol] = new_side
        logger.info(f"🔄 [Flip] {symbol}: {current} -> {new_side}")

    async def on_tick(self, event: dict):
        try:
            event_type = event.get('type', 'tick')
            if event_type == 'trade':
                # 成交事件：最高优先级，必须 await 确保处理
                await self._process_trade_fill(event)
            elif event_type == 'tick':
                # 行情事件：非阻塞处理
                await self._process_tick(event)
        except Exception as e:
            logger.error(f"Strategy Error: {e}", exc_info=True)

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        # 检查锁：如果正在对冲，停止挂单计算
        lock = self._get_lock(symbol)
        if lock.locked(): return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            # 限制频率
            now = time.time()
            if now - self.last_quote_time.get(symbol, 0) < 0.5: return
            self.last_quote_time[symbol] = now

            # 放入后台任务，不阻塞
            asyncio.create_task(self._manage_maker_orders(symbol))

    async def _manage_maker_orders(self, symbol: str):
        # 双重检查锁
        if self._get_lock(symbol).locked(): return

        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        # 1. 确定当前挂单方向
        maker_side = self._get_current_side(symbol)

        # 2. 计算安全价格 (Smart Price)
        target_price = self._calculate_safe_maker_price(symbol, grvt_tick, lighter_tick, maker_side)
        if not target_price:
            # 价格计算失败（如深度不足），如果当前有单，考虑撤单
            if symbol in self.active_orders:
                asyncio.create_task(self._cancel_order_task(symbol, self.active_orders[symbol]))
            return

        current_order_id = self.active_orders.get(symbol)
        current_price = self.active_order_prices.get(symbol)
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # 3. 执行挂单逻辑
        if not current_order_id:
            # 无单 -> 挂新单
            # logger.info(f"🆕 [Quote] {symbol} {maker_side} {quantity} @ {target_price}")
            self.order_create_time[symbol] = time.time()
            asyncio.create_task(self._place_order_task(symbol, maker_side, quantity, target_price))

        else:
            # 有单 -> 检查是否需要 Requote
            # 保护期：5秒内不轻易撤单，除非价格偏离极大
            order_age = time.time() - self.order_create_time.get(symbol, 0)

            # 偏差检查
            price_diff_pct = abs(target_price - current_price) / current_price

            should_requote = False
            if price_diff_pct > self.requote_threshold:
                if order_age > 2.0:  # 超过2秒，允许因微小波动撤单
                    should_requote = True
                elif price_diff_pct > self.requote_threshold * 5:  # 剧烈波动，立即撤单
                    should_requote = True

            if should_requote:
                logger.info(f"♻️ [Requote] {symbol} Diff: {price_diff_pct * 100:.3f}%")
                asyncio.create_task(self._cancel_order_task(symbol, current_order_id))
                # 注意：撤单后 active_orders 会被乐观清理，下一次 Tick 会触发挂单

    def _calculate_safe_maker_price(self, symbol: str, grvt_tick: dict, lighter_tick: dict, side: str) -> Optional[
        float]:
        """
        核心风控：基于 Lighter 深度计算 GRVT 挂单价
        """
        adapter = self.adapters['GRVT']
        info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(info['tick_size']) if info else 0.01

        qty = Config.TRADE_QUANTITIES.get(symbol, 0.0001)
        required_qty = qty * self.REQUIRED_DEPTH_RATIO

        # 计算 Lighter 吃单均价 (VWAP)
        hedge_price = self._get_depth_weighted_price(lighter_tick, 'SELL' if side == 'BUY' else 'BUY', required_qty)
        if not hedge_price:
            return None  # 深度不足

        # 计算目标挂单价
        # 公式：挂单价 = 对冲成本 * (1 - 目标利润率)
        # target_margin < 0 代表愿意亏损 (Cost)
        if side == 'BUY':
            # 假如 Lighter 卖方均价 100，margin -0.0001 => 挂单 100 * 1.0001 = 100.01 (高于对冲价，容易成交)
            raw_target = hedge_price * (1 - self.target_margin)
            # 必须符合 Tick Size
            # 检查是否超过 GRVT 盘口 (避免 Taker) - 这里可选，如果是刷量可以激进点
        else:
            # 假如 Lighter 买方均价 100，margin -0.0001 => 挂单 100 * 0.9999 = 99.99 (低于对冲价)
            raw_target = hedge_price * (1 + self.target_margin)

        return raw_target

    def _get_depth_weighted_price(self, ticker, side, required_qty):
        # 兼容性处理：如果 Adapter 没传 depth，尝试用 best price
        depth = ticker.get('asks_depth' if side == 'BUY' else 'bids_depth')  # 对冲方向的深度
        if not depth:
            # 回退逻辑：如果没有深度数据，仅当配置允许时才使用 Best Price
            # 为了安全，建议返回 None，但为了演示这里回退
            return ticker.get('ask' if side == 'BUY' else 'bid')

        collected = 0.0
        cost = 0.0

        # 深度遍历
        for p_str, s_str in depth:
            p, s = float(p_str), float(s_str)
            needed = required_qty - collected
            take = min(s, needed)
            cost += take * p
            collected += take
            if collected >= required_qty:
                break

        if collected < required_qty * 0.5:
            return None  # 深度太差

        return cost / collected

    async def _place_order_task(self, symbol, side, qty, price):
        # [Fix] 增加 try-catch 防止报错中断
        try:
            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT", side=side, amount=qty, price=price
            )
            if new_id:
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = price
        except Exception as e:
            logger.error(f"Place Order Error: {e}")

    async def _cancel_order_task(self, symbol, order_id):
        try:
            # [Fix] 传递 symbol 参数
            await self.adapters['GRVT'].cancel_order(order_id, symbol=symbol)
        except Exception:
            pass
        # 乐观清理
        if symbol in self.active_orders and self.active_orders[symbol] == order_id:
            del self.active_orders[symbol]
            if symbol in self.active_order_prices: del self.active_order_prices[symbol]

    async def _process_trade_fill(self, trade: dict):
        if trade['exchange'] != 'GRVT': return

        symbol = trade['symbol']
        lock = self._get_lock(symbol)

        # 只要成交，必须立即对冲
        # 使用锁暂停挂单逻辑
        async with lock:
            logger.info(f"🚨 [FILLED] GRVT {trade['side']} {trade['size']} -> HEDGING!")

            # 1. 清理旧状态 (防止改单任务干扰)
            if symbol in self.active_orders:
                del self.active_orders[symbol]

            # 2. 执行死循环对冲 (v9 逻辑)
            await self._execute_hedge_loop(symbol, trade['side'], float(trade['size']))

    async def _execute_hedge_loop(self, symbol, grvt_side, size):
        hedge_side = 'SELL' if grvt_side.upper() == 'BUY' else 'BUY'

        # 增加对冲重试次数
        retry = 0
        while retry < 10:
            try:
                # 重新获取最新价格 (Price Discovery)
                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    await asyncio.sleep(0.1)
                    continue

                # 激进价格: 确保成交 (Market Order 模拟)
                base_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']
                if base_price <= 0:
                    retry += 1
                    continue

                exec_price = base_price * 1.05 if hedge_side == 'BUY' else base_price * 0.95

                logger.info(f"🌊 [Hedge] {hedge_side} {size} @ {exec_price:.2f} (Try {retry + 1})")

                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol, side=hedge_side, amount=size, price=exec_price, order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ Hedge Success ID: {order_id}")
                    # 对冲成功后，翻转方向
                    self._flip_side(symbol)
                    return

            except Exception as e:
                logger.error(f"❌ Hedge Retry {retry} Failed: {e}")

            retry += 1
            await asyncio.sleep(0.5)  # 稍微等待

        logger.critical(f"💀💀💀 CRITICAL: {symbol} Hedge FAILED after retries. Manual Intervention Required!")