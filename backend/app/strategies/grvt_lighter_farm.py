# backend/app/strategies/grvt_lighter_farm.py
import asyncio
import logging
import time
import collections
from typing import Dict, Any, Optional, Tuple, List, Deque
from app.config import settings

logger = logging.getLogger("GRVT_Lighter_Farm")


class GrvtLighterFarmStrategy:
    """
    GRVT (Maker) + Lighter (Taker) 生产级刷量对冲策略 (Pro V4)

    核心特性:
    1. Leader-Follower: GRVT成交后立即在Lighter市价对冲。
    2. Zero Leg Risk: 严格的Watchdog和锁机制，追求绝对的仓位平衡。
    3. Aggressive Pricing: 支持负滑点配置，允许亏损挂单以紧贴盘口。
    4. Anti-Toxic Flow: 包含动量保护和最小存活时间机制。
    """

    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_v4_Pro"
        self.adapters = adapters

        # --- 交易所适配器 ---
        self.grvt = adapters.get('GRVT')
        self.lighter = adapters.get('Lighter')
        if not self.grvt or not self.lighter:
            raise RuntimeError("CRITICAL: GRVT or Lighter adapter missing!")

        # --- 状态数据 ---
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 仓位状态 (内存缓存，定期同步)
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

        # --- 配置加载 ---
        conf = settings.strategies.farming
        self.target_symbols = settings.common.target_symbols

        # 核心参数 (支持负数滑点，负数代表愿意支付成本)
        self.slippage_tolerance = float(conf.max_slippage_tolerance)

        self.max_inventory_usd = float(conf.max_inventory_usd)
        self.requote_threshold = float(conf.requote_threshold)
        self.required_depth_ratio = float(getattr(conf, 'required_depth_ratio', 3.0))  # 默认 3.0 倍深度覆盖

        # 订单生命周期与风控参数
        self.min_order_lifetime = 2.0  # 默认最小存活 2s
        self.urgent_threshold = 0.01  # 1% 偏差视为紧急情况，无视存活时间
        self.momentum_window = 2.0  # 动量观察窗口
        self.momentum_threshold = 0.001  # 动量阈值

        self.running = True
        logger.info(
            f"🛡️ [Strategy] V4 Started. Tolerance={self.slippage_tolerance} (Neg=Aggressive), DepthReq={self.required_depth_ratio}x")

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

        # GRVT (Leader) 成交 -> 触发对冲
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

                # 目标: Lighter = -GRVT
                diff = -grvt_p - lighter_p

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

                # --- 智能滑点计算 (Taker) ---
                # 对冲时使用对方的盘口价作为基准 (买入看Ask, 卖出看Bid)
                # 并给予 1% 的硬滑点保护，保证成交 (Taker不需要省钱，只需要成交)
                ref_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']
                limit_price = ref_price * 1.01 if hedge_side == 'BUY' else ref_price * 0.99

                logger.info(f"🌊 [FIRING HEDGE] Lighter {hedge_side} {hedge_size} @ ~{limit_price:.4f}")

                # 发送 Market 单 (如果不支持，Adapter需转为IOC)
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
    # Maker 挂单逻辑 (GRVT) - 修正价格计算
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
            momentum = self._detect_market_momentum(symbol)

            qty = settings.get_trade_qty(symbol)
            orders_to_place = []  # list of (side, price)

            # --- 场景 A: 正常做市 (OPEN) ---
            if abs(g_pos) < (qty * 0.1) and not is_unbalanced:

                # 检查 Lighter 深度是否足够 (Safety Check)
                safe_qty = self._check_liquidity(lighter_tick, qty)
                if safe_qty < (qty * 0.1):
                    logger.warning(f"⚠️ [Liquidity] Depth too thin for {symbol}, skipping.")
                    await self._cancel_all_maker(symbol)
                    return

                allow_buy = momentum != 'BEARISH'
                allow_sell = momentum != 'BULLISH'

                # --- 价格计算核心修正 (Fix Pricing) ---
                # GRVT Buy Maker -> 对冲需 Lighter Sell Taker -> 锚定 Lighter Bid
                # Price = Bid * (1 - tolerance). 如果 tolerance 为负 (e.g. -0.0004), Price = Bid * 1.0004
                bid_ref = self._get_weighted_price(lighter_tick, 'BUY', safe_qty)

                # GRVT Sell Maker -> 对冲需 Lighter Buy Taker -> 锚定 Lighter Ask
                # Price = Ask * (1 + tolerance). 如果 tolerance 为负, Price = Ask * 0.9996
                ask_ref = self._get_weighted_price(lighter_tick, 'SELL', safe_qty)

                if bid_ref and ask_ref:
                    if allow_buy:
                        my_bid = bid_ref * (1 - self.slippage_tolerance)
                        orders_to_place.append(('BUY', my_bid))
                    else:
                        logger.warning(f"⚠️ [Anti-Toxic] Blocking BUY due to Falling Knife")

                    if allow_sell:
                        my_ask = ask_ref * (1 + self.slippage_tolerance)
                        orders_to_place.append(('SELL', my_ask))
                    else:
                        logger.warning(f"⚠️ [Anti-Toxic] Blocking SELL due to Pumping Market")

            # --- 场景 B: 平仓模式 (CLOSE) ---
            else:
                # 平仓时，我们希望尽快成交，可以使用更激进的 tolerance
                # 无论原本 tolerance 是多少，平仓至少给 5bps 空间
                close_tolerance = min(self.slippage_tolerance, -0.0005)

                if g_pos > 0:  # Long GRVT -> Sell to Close
                    # 锚定 Lighter Ask (买入平空)
                    ref = self._get_weighted_price(lighter_tick, 'SELL', abs(g_pos))
                    if ref:
                        orders_to_place.append(('SELL', ref * (1 + close_tolerance)))

                elif g_pos < 0:  # Short GRVT -> Buy to Close
                    # 锚定 Lighter Bid (卖出平多)
                    ref = self._get_weighted_price(lighter_tick, 'BUY', abs(g_pos))
                    if ref:
                        orders_to_place.append(('BUY', ref * (1 - close_tolerance)))

            # 3. 执行挂单/改单 (包含最小存活时间检查)
            await self._reconcile_orders(symbol, orders_to_place, qty)

    def _check_liquidity(self, ticker, target_qty) -> float:
        """ 检查 Lighter 是否有足够的深度 """
        required = target_qty * self.required_depth_ratio

        # 简单检查第一档深度
        bid_vol = sum([float(x[1]) for x in ticker.get('bids_depth', [])[:5]])
        ask_vol = sum([float(x[1]) for x in ticker.get('asks_depth', [])[:5]])

        min_liq = min(bid_vol, ask_vol)
        if min_liq < required:
            # 如果深度不够，按比例缩小下单量
            return max(0.0, min_liq / self.required_depth_ratio)
        return target_qty

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

            # 情况 A: 策略不再需要该方向的单子
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
                sides_to_cancel.append(side)
            elif price_diff_pct > self.requote_threshold:
                if is_mature:
                    sides_to_cancel.append(side)
                else:
                    # 未成熟且非紧急，保持不动 (不撤单，也不下新单)
                    del desired_map[side]
            else:
                # 价格没变，保持不动
                del desired_map[side]

        # 执行撤单
        for side in sides_to_cancel:
            oid = current_orders[side]
            await self.grvt.cancel_order(oid, symbol=symbol)
            if side in self.active_maker_orders.get(symbol, {}):
                del self.active_maker_orders[symbol][side]
            if side in self.maker_order_info.get(symbol, {}):
                del self.maker_order_info[symbol][side]

        # --- 2. 下新单逻辑 ---
        for side, price in desired_map.items():
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
                        'ts': time.time()
                    }
                    logger.info(f"🆕 [QUOTE] {symbol} {side} {order_qty} @ {price:.2f}")

            except Exception as e:
                logger.warning(f"⚠️ Quote Failed: {e}")

    async def _cancel_all_maker(self, symbol):
        if symbol in self.active_maker_orders:
            for side, oid in list(self.active_maker_orders[symbol].items()):
                await self.grvt.cancel_order(oid, symbol=symbol)
            self.active_maker_orders[symbol] = {}
            self.maker_order_info[symbol] = {}

    def _get_weighted_price(self, ticker, side, qty):
        """
        side='BUY' -> 返回 Bids 的加权均价 (我们作为Maker买入，意味着吃单者卖给我们，参考Bids)
        side='SELL' -> 返回 Asks 的加权均价
        """
        # 注意: 这里获取的是"参考价格"。
        # 如果我们要在GRVT买入(Maker)，我们将拥有多头，对冲需要去Lighter卖出(Taker)。
        # Lighter卖出(Taker)的价格是Lighter的Bids。
        # 所以这里的 side='BUY' 应该获取 Lighter 的 Bids Depth。
        depth_key = 'bids_depth' if side == 'BUY' else 'asks_depth'
        depth = ticker.get(depth_key, [])
        base_price = ticker.get('bid' if side == 'BUY' else 'ask')

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