# backend/app/strategies/grvt_lighter_farm.py
import asyncio
import logging
import time
import collections
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple, List, Deque
from app.config import settings

logger = logging.getLogger("GRVT_Lighter_Farm")


class GrvtLighterFarmStrategy:
    """
    GRVT (Maker) + Lighter (Taker) 生产级对冲做市策略 (v3 Optimized)

    主要特性:
    1. Leader-Follower: GRVT Maker 成交 -> Lighter Taker 立即对冲。
    2. Zero Leg Risk: 严格的 watchdog 和锁机制防止单边敞口。
    3. Anti-Toxic Flow: 基于短期动量保护，防止逆向选择 (接飞刀)。
    4. Smart Order Lifecycle: 最小存活时间机制，避免被限流。
    """

    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Pro_Farm_v3"
        self.adapters = adapters

        # --- 交易所适配器 ---
        self.grvt = adapters.get('GRVT')
        self.lighter = adapters.get('Lighter')
        if not self.grvt or not self.lighter:
            raise RuntimeError("CRITICAL: GRVT or Lighter adapter missing!")

        # --- 状态数据 ---
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 仓位状态 (内存缓存)
        self.pos_grvt: Dict[str, float] = {}
        self.pos_lighter: Dict[str, float] = {}

        # 挂单状态管理
        # active_maker_orders: symbol -> side -> order_id
        self.active_maker_orders: Dict[str, Dict[str, str]] = {}
        # maker_order_info: symbol -> side -> {'price': float, 'ts': float}
        self.maker_order_info: Dict[str, Dict[str, dict]] = {}

        # 市场微观结构数据 (用于防逆向选择)
        # 存储格式: deque of (timestamp, mid_price)
        self.price_history: Dict[str, Deque[Tuple[float, float]]] = {}

        # 锁与并发控制
        self.hedge_lock = asyncio.Lock()
        self.symbol_locks: Dict[str, asyncio.Lock] = {}

        # --- 配置参数 ---
        conf = settings.strategies.farming
        self.target_symbols = settings.common.target_symbols

        # 核心参数
        self.spread_margin = float(getattr(conf, 'spread_margin', 0.0005))  # 基础利润空间
        self.max_inventory_usd = float(conf.max_inventory_usd)

        # 优化参数: 订单存活时间
        self.min_order_lifetime = float(getattr(conf, 'min_order_lifetime', 2.0))  # 默认 2.0s
        self.requote_threshold = float(conf.requote_threshold)  # 普通改单阈值
        self.urgent_threshold = 0.01  # 1% 偏差视为紧急情况，无视存活时间

        # 优化参数: 逆向选择保护 (Toxic Flow Protection)
        self.momentum_window = 2.0  # 观察过去 2 秒的价格
        self.momentum_threshold = 0.001  # 如果 2s 内价格变动 > 0.1%，视为强趋势

        self.running = True
        logger.info(
            f"🛡️ [Strategy] GRVT-Lighter Pro Started. Lifetime={self.min_order_lifetime}s, Margin={self.spread_margin}")

        # 启动后台守护进程
        asyncio.create_task(self._watchdog_loop())

    def _get_lock(self, symbol: str):
        if symbol not in self.symbol_locks:
            self.symbol_locks[symbol] = asyncio.Lock()
        return self.symbol_locks[symbol]

    async def on_tick(self, event: dict):
        if not self.running: return

        etype = event.get('type')
        if etype == 'tick':
            await self._process_tick(event)
        elif etype == 'trade':
            await self._process_trade(event)

    async def _process_trade(self, trade: dict):
        """
        处理成交事件 (核心触发点)
        """
        symbol = trade['symbol']
        exchange = trade['exchange']

        try:
            size = float(trade['size'])
            side = trade['side']  # BUY or SELL
        except (ValueError, TypeError):
            return

        # 我们只关心 GRVT 的成交，因为它是 Leader
        if exchange == 'GRVT':
            logger.info(f"⚡️ [FILL DETECTED] GRVT {side} {size} @ {trade.get('price')}")

            async with self.hedge_lock:
                # 1. 更新本地 GRVT 持仓
                current = self.pos_grvt.get(symbol, 0.0)
                change = size if side == 'BUY' else -size
                self.pos_grvt[symbol] = current + change

                # 2. 立即触发对冲 (Critical)
                await self._execute_hedge_logic(symbol)

        elif exchange == 'Lighter':
            # Lighter 成交仅更新持仓
            async with self.hedge_lock:
                current = self.pos_lighter.get(symbol, 0.0)
                change = size if side == 'BUY' else -size
                self.pos_lighter[symbol] = current + change
                logger.info(f"✅ [HEDGE CONFIRMED] Lighter {side} {size}. Net Lighter Pos: {self.pos_lighter[symbol]}")

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if exchange == 'Lighter':
            # 1. 更新价格历史 (用于计算动量)
            self._update_price_history(symbol, tick)

            # 2. 触发 Quote 更新
            # 使用 create_task 避免阻塞 tick 处理流
            asyncio.create_task(self._update_maker_quotes(symbol))

    def _update_price_history(self, symbol: str, tick: dict):
        """维护最近 N 秒的价格历史，用于检测有毒流"""
        mid = (float(tick['bid']) + float(tick['ask'])) / 2
        now = time.time()

        if symbol not in self.price_history:
            self.price_history[symbol] = collections.deque()

        history = self.price_history[symbol]
        history.append((now, mid))

        # 清理过期数据
        while history and history[0][0] < now - self.momentum_window:
            history.popleft()

    def _detect_market_momentum(self, symbol: str) -> str:
        """
        检测 Lighter 市场动量
        Return: 'NEUTRAL', 'BULLISH' (暴涨), 'BEARISH' (暴跌)
        """
        history = self.price_history.get(symbol)
        if not history or len(history) < 2:
            return 'NEUTRAL'

        start_price = history[0][1]
        end_price = history[-1][1]

        if start_price == 0: return 'NEUTRAL'

        pct_change = (end_price - start_price) / start_price

        if pct_change > self.momentum_threshold:
            return 'BULLISH'
        elif pct_change < -self.momentum_threshold:
            return 'BEARISH'

        return 'NEUTRAL'

    # ==================================================================
    # 核心对冲逻辑 (Lighter Taker)
    # ==================================================================

    async def _execute_hedge_logic(self, symbol: str):
        """
        Lighter 市价对冲，强制平衡仓位
        """
        retry_count = 0
        max_retries = 5

        while retry_count < max_retries:
            try:
                # 重新计算需要的操作
                grvt_p = self.pos_grvt.get(symbol, 0.0)
                lighter_p = self.pos_lighter.get(symbol, 0.0)

                target_lighter = -grvt_p
                diff = target_lighter - lighter_p

                if abs(diff) < 0.0001:
                    if retry_count > 0:
                        logger.info(f"✅ [HEDGE DONE] {symbol} Balanced.")
                    return

                hedge_side = 'BUY' if diff > 0 else 'SELL'
                hedge_size = abs(diff)

                # 获取 Lighter 盘口
                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    await asyncio.sleep(0.2)
                    retry_count += 1
                    continue

                # --- 智能滑点计算 ---
                # 不使用固定滑点，而是根据盘口深度计算 "能够吃下 hedge_size 的加权价格" 再放宽一点
                # 这能保证成交且不过度滑点
                depth_price = self._get_weighted_price(lighter_tick, hedge_side, hedge_size)
                if not depth_price:
                    depth_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']

                # 额外给予 2% 的缓冲区应对网络延迟期间的价格跳变
                limit_price = depth_price * 1.02 if hedge_side == 'BUY' else depth_price * 0.98

                logger.info(f"🌊 [FIRING HEDGE] Lighter {hedge_side} {hedge_size} @ {limit_price:.4f}")

                # 发送 Market 单 (或模拟 Market 的 IOC)
                order_id = await self.lighter.create_order(
                    symbol=symbol,
                    side=hedge_side,
                    amount=hedge_size,
                    price=limit_price,
                    order_type="MARKET"
                )

                if order_id:
                    # 预更新状态
                    change = hedge_size if hedge_side == 'BUY' else -hedge_size
                    self.pos_lighter[symbol] += change
                    # 短暂等待 WS 确认或继续下一轮检查
                    await asyncio.sleep(0.1)
                    continue
                else:
                    logger.error("❌ [Hedge] Order Failed (No ID)")

            except Exception as e:
                logger.error(f"❌ [Hedge Error] {e}")

            retry_count += 1
            await asyncio.sleep(0.3)

        logger.critical(f"💀 [HEDGE FAILED] {symbol} Stopping Maker to prevent risk.")
        asyncio.create_task(self._cancel_all_maker(symbol))

    # ==================================================================
    # Maker 挂单逻辑 (GRVT)
    # ==================================================================

    async def _update_maker_quotes(self, symbol: str):
        """
        根据 Lighter 盘口 + 动量保护 + 最小存活时间 更新 GRVT 挂单
        """
        lock = self._get_lock(symbol)
        if lock.locked(): return

        async with lock:
            lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
            if not lighter_tick or lighter_tick.get('bid') == 0: return

            # 1. 失衡检查：如有未对冲仓位，禁止挂新单 (只允许 Close)
            g_pos = self.pos_grvt.get(symbol, 0.0)
            l_pos = self.pos_lighter.get(symbol, 0.0)
            is_unbalanced = abs(g_pos + l_pos) > 0.0001

            # 2. 动量检测 (Anti-Toxic Flow)
            # 如果市场正在剧烈波动，我们可能会暂停某一方向的挂单
            momentum = self._detect_market_momentum(symbol)

            qty = settings.get_trade_qty(symbol)
            orders_to_place = []  # list of (side, price)

            # --- 场景 A: 无持仓 (OPEN) ---
            if abs(g_pos) < (qty * 0.1) and not is_unbalanced:

                # 动量保护：
                # 暴涨 (Bullish) -> 不要挂 SELL 单 (会被低价吃掉然后拉盘)
                # 暴跌 (Bearish) -> 不要挂 BUY 单 (会接飞刀)
                allow_buy = momentum != 'BEARISH'
                allow_sell = momentum != 'BULLISH'

                bid_ref = self._get_weighted_price(lighter_tick, 'SELL', qty)
                ask_ref = self._get_weighted_price(lighter_tick, 'BUY', qty)

                if bid_ref and ask_ref:
                    if allow_buy:
                        my_bid = bid_ref * (1 - self.spread_margin)
                        orders_to_place.append(('BUY', my_bid))
                    else:
                        logger.warning(f"⚠️ [Anti-Toxic] Blocking BUY due to Falling Knife")

                    if allow_sell:
                        my_ask = ask_ref * (1 + self.spread_margin)
                        orders_to_place.append(('SELL', my_ask))
                    else:
                        logger.warning(f"⚠️ [Anti-Toxic] Blocking SELL due to Pumping Market")

            # --- 场景 B: 有持仓 (CLOSE) ---
            else:
                # 平仓逻辑通常不看动量，因为减少风险是第一位的
                # 但如果动量极强，可以考虑暂缓平仓(Profit Run)，这里保持简单：尽快平仓
                if g_pos > 0:  # Long GRVT -> Sell to Close
                    ref_price = self._get_weighted_price(lighter_tick, 'SELL', abs(g_pos))
                    if ref_price:
                        # 确保平仓单也能赚点差，或者至少不亏太多
                        orders_to_place.append(('SELL', ref_price * (1 + self.spread_margin)))

                elif g_pos < 0:  # Short GRVT -> Buy to Close
                    ref_price = self._get_weighted_price(lighter_tick, 'BUY', abs(g_pos))
                    if ref_price:
                        orders_to_place.append(('BUY', ref_price * (1 - self.spread_margin)))

            # 3. 执行挂单/改单 (包含最小存活时间检查)
            await self._reconcile_orders(symbol, orders_to_place, qty)

    async def _reconcile_orders(self, symbol: str, desired_orders: List[Tuple[str, float]], qty: float):
        """
        智能订单管理: 包含存活时间检查
        """
        current_orders = self.active_maker_orders.get(symbol, {})
        current_info = self.maker_order_info.get(symbol, {})

        desired_map = {side: price for side, price in desired_orders}
        now = time.time()

        # --- 1. 撤单逻辑 ---
        sides_to_cancel = []
        for side, oid in current_orders.items():
            info = current_info.get(side, {})
            curr_price = info.get('price', 0)
            curr_ts = info.get('ts', 0)

            # 情况 A: 策略不再需要该方向的单子 (比如动量保护触发，或者方向变了)
            if side not in desired_map:
                sides_to_cancel.append(side)
                continue

            # 情况 B: 价格变化检查 (Requote)
            new_price = desired_map[side]
            price_diff_pct = abs(curr_price - new_price) / curr_price if curr_price else 0

            # === Order Lifetime Check ===
            life_span = now - curr_ts
            is_urgent = price_diff_pct > self.urgent_threshold
            is_mature = life_span >= self.min_order_lifetime

            if is_urgent:
                # 价格偏离太大(如1%)，无视时间，立即重挂
                sides_to_cancel.append(side)
            elif price_diff_pct > self.requote_threshold:
                # 价格有变动，但不够紧急
                if is_mature:
                    sides_to_cancel.append(side)
                else:
                    # 未满存活时间，且非紧急，保持不动
                    # 从 desired 中移除，表示"已满足"，不需下新单
                    del desired_map[side]
            else:
                # 价格没变，保留
                del desired_map[side]

        # 执行撤单
        for side in sides_to_cancel:
            oid = current_orders[side]
            await self.grvt.cancel_order(oid, symbol=symbol)

            # 清理状态
            if side in self.active_maker_orders.get(symbol, {}):
                del self.active_maker_orders[symbol][side]
            if side in self.maker_order_info.get(symbol, {}):
                del self.maker_order_info[symbol][side]

        # --- 2. 下新单逻辑 ---
        for side, price in desired_map.items():
            # 计算数量
            order_qty = qty
            # Close 模式下数量匹配持仓
            if side == 'SELL' and self.pos_grvt.get(symbol, 0) > 0:
                order_qty = abs(self.pos_grvt[symbol])
            elif side == 'BUY' and self.pos_grvt.get(symbol, 0) < 0:
                order_qty = abs(self.pos_grvt[symbol])

            # 价格精度修正
            info = self.grvt.contract_map.get(f"{symbol}-USDT")
            if info:
                tick_size = float(info['tick_size'])
                price = round(price / tick_size) * tick_size

            try:
                oid = await self.grvt.create_order(
                    symbol=f"{symbol}-USDT",
                    side=side,
                    amount=order_qty,
                    price=price,
                    order_type="LIMIT",
                    post_only=True
                )

                if oid:
                    if symbol not in self.active_maker_orders:
                        self.active_maker_orders[symbol] = {}
                        self.maker_order_info[symbol] = {}

                    self.active_maker_orders[symbol][side] = oid
                    self.maker_order_info[symbol][side] = {
                        'price': price,
                        'ts': time.time()  # 记录创建时间
                    }
                    logger.info(f"🆕 [QUOTE] {symbol} {side} {order_qty} @ {price}")

            except Exception as e:
                logger.warning(f"⚠️ Quote Failed: {e}")

    async def _cancel_all_maker(self, symbol):
        if symbol in self.active_maker_orders:
            for side, oid in list(self.active_maker_orders[symbol].items()):
                await self.grvt.cancel_order(oid, symbol=symbol)
            self.active_maker_orders[symbol] = {}
            self.maker_order_info[symbol] = {}

    def _get_weighted_price(self, ticker, side, qty):
        # 深度加权计算
        depth_key = 'asks_depth' if side == 'SELL' else 'bids_depth'
        depth = ticker.get(depth_key, [])
        base_price = ticker.get('ask' if side == 'SELL' else 'bid')

        if not depth: return base_price

        cum_vol = 0.0
        cum_cost = 0.0
        for p, v in depth:
            p, v = float(p), float(v)
            take = min(v, qty - cum_vol)
            cum_cost += take * p
            cum_vol += take
            if cum_vol >= qty: break

        if cum_vol == 0: return base_price
        return cum_cost / cum_vol

    # ==================================================================
    # 后台守护
    # ==================================================================

    async def _watchdog_loop(self):
        logger.info("🐶 Watchdog started...")
        while self.running:
            try:
                await asyncio.sleep(5.0)

                # 双边持仓同步
                grvt_positions = await self.grvt.fetch_positions(symbols=self.target_symbols)
                lighter_positions = await self._fetch_lighter_positions_safe()

                for symbol in self.target_symbols:
                    # 提取 GRVT Pos
                    g_pos = 0.0
                    for p in grvt_positions:
                        p_sym = p.get('instrument') or p.get('symbol')
                        if symbol in p_sym:
                            sz = float(p.get('size') or p.get('contracts', 0))
                            if p.get('side', '').upper() == 'SHORT': sz = -sz
                            g_pos = sz
                            break

                    # 提取 Lighter Pos
                    l_pos = lighter_positions.get(symbol, self.pos_lighter.get(symbol, 0.0))

                    async with self.hedge_lock:
                        self.pos_grvt[symbol] = g_pos
                        self.pos_lighter[symbol] = l_pos

                        # 校验平衡
                        if abs(g_pos + l_pos) > 0.0001:
                            logger.warning(f"🐶 [Watchdog] Unbalanced {symbol}: G={g_pos} L={l_pos}")
                            await self._execute_hedge_logic(symbol)

            except Exception as e:
                logger.error(f"🐶 Watchdog Error: {e}")
                await asyncio.sleep(5)

    async def _fetch_lighter_positions_safe(self) -> Dict[str, float]:
        res = {}
        if hasattr(self.lighter, 'fetch_positions'):
            try:
                positions = await self.lighter.fetch_positions(symbols=self.target_symbols)
                if positions:
                    for p in positions:
                        sz = float(p['size'])
                        if p['side'] == 'SELL': sz = -sz
                        res[p['symbol']] = sz
            except:
                pass
        return res