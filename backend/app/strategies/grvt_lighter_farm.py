import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple, List
from app.config import settings

logger = logging.getLogger("GRVT_Lighter_Farm")


class GrvtLighterFarmStrategy:
    """
    GRVT (Maker) + Lighter (Taker) 严格对冲做市策略 (Leader-Follower模式)

    核心原则:
    1. GRVT 是 Leader (Maker)，Lighter 是 Follower (Taker)。
    2. 任何时刻，目标状态必须满足: Lighter_Position = -1 * GRVT_Position。
    3. GRVT 成交后(Partial/Full)，立即触发 Lighter 市价对冲。
    4. 只有当双边持仓完全平衡(净敞口为0)时，才允许挂新的 Open 单。
    """

    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Robust_Farm_v2"
        self.adapters = adapters

        # --- 交易所适配器 ---
        self.grvt = adapters.get('GRVT')
        self.lighter = adapters.get('Lighter')
        if not self.grvt or not self.lighter:
            raise RuntimeError("CRITICAL: GRVT or Lighter adapter missing!")

        # --- 状态数据 ---
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 仓位状态 (内存缓存，定期与API同步)
        self.pos_grvt: Dict[str, float] = {}
        self.pos_lighter: Dict[str, float] = {}

        # 挂单状态
        self.active_maker_orders: Dict[str, Dict[str, str]] = {}  # symbol -> {side: order_id}
        self.maker_order_prices: Dict[str, Dict[str, float]] = {}

        # 锁与并发控制
        self.hedge_lock = asyncio.Lock()  # 全局对冲锁，确保对冲逻辑串行
        self.symbol_locks: Dict[str, asyncio.Lock] = {}

        # --- 配置参数 ---
        conf = settings.strategies.farming
        self.target_symbols = settings.common.target_symbols
        self.spread_margin = abs(conf.max_slippage_tolerance)  # 目标利润率 (如 0.0005)
        self.max_inventory_usd = conf.max_inventory_usd
        self.requote_threshold = conf.requote_threshold

        # Lighter 对冲滑点 (市价单保护)
        self.hedge_slippage = 0.02  # 2% 确保成交

        self.running = True
        logger.info(f"🛡️ [Strategy] GRVT(Maker) -> Lighter(Taker) Farm Started. Margin: {self.spread_margin}")

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
        size = float(trade['size'])
        side = trade['side']  # BUY or SELL

        # 我们只关心 GRVT 的成交，因为它是 Leader
        if exchange == 'GRVT':
            logger.info(f"⚡️ [FILL DETECTED] GRVT {side} {size} @ {trade.get('price')}")

            # 1. 更新本地 GRVT 持仓
            async with self.hedge_lock:
                current = self.pos_grvt.get(symbol, 0.0)
                change = size if side == 'BUY' else -size
                self.pos_grvt[symbol] = current + change

                # 2. 立即触发对冲检查 (Fire and Forget but Awaited internally)
                # 注意：这里直接调用对冲逻辑，而不是仅仅更新状态
                await self._execute_hedge_logic(symbol)

        elif exchange == 'Lighter':
            # Lighter 成交仅更新持仓，作为对冲结果的确认
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

        # 只有在不需要紧急对冲的时候，才更新 Maker 挂单
        # 如果正在对冲，挂单逻辑会被锁阻塞
        if exchange == 'Lighter':
            # Lighter 价格变动触发 GRVT 挂单更新
            asyncio.create_task(self._update_maker_quotes(symbol))

    # ==================================================================
    # 核心对冲逻辑 (Critical Path)
    # ==================================================================

    async def _execute_hedge_logic(self, symbol: str):
        """
        计算 GRVT 和 Lighter 的持仓差额，并在 Lighter 上执行市价对冲。
        目标: Lighter_Pos + GRVT_Pos = 0
        """
        retry_count = 0
        max_retries = 5

        while retry_count < max_retries:
            try:
                # 在锁内计算，防止并发计算导致重复下单
                needed_action = None  # (side, size)

                grvt_p = self.pos_grvt.get(symbol, 0.0)
                lighter_p = self.pos_lighter.get(symbol, 0.0)

                # 目标 Lighter 持仓应为 -GRVT 持仓
                target_lighter = -grvt_p
                diff = target_lighter - lighter_p

                # 忽略微小误差 (dust)
                if abs(diff) < 0.0001:
                    if retry_count > 0:
                        logger.info(f"✅ [HEDGE DONE] {symbol} Balanced. G:{grvt_p} L:{lighter_p}")
                    return  # 平衡，退出

                # 确定对冲方向
                hedge_side = 'BUY' if diff > 0 else 'SELL'
                hedge_size = abs(diff)

                logger.warning(
                    f"🚨 [HEDGE REQUIRED] {symbol} | GRVT:{grvt_p} Lighter:{lighter_p} | Need Lighter {hedge_side} {hedge_size}")

                # --- 执行对冲 (Lighter Taker) ---
                # 获取 Lighter 盘口计算滑点保护价格
                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    logger.error(f"❌ [Hedge] No Lighter tick data for {symbol}")
                    await asyncio.sleep(0.5)
                    retry_count += 1
                    continue

                base_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']
                if base_price <= 0:
                    # 可能是盘口空了，使用上次成交价或报错
                    logger.error("❌ [Hedge] Lighter price is 0/Invalid")
                    await asyncio.sleep(0.5)
                    retry_count += 1
                    continue

                # 激进市价单 (通过 Limit IOC 模拟或直接 Market)
                # Lighter SDK 支持 create_market_order
                # 这里我们使用 create_order (Adapter层适配)
                # 为了保证成交，价格给予较大滑点
                exec_price = base_price * (1 + self.hedge_slippage) if hedge_side == 'BUY' else base_price * (
                            1 - self.hedge_slippage)

                logger.info(f"🌊 [FIRING HEDGE] Lighter {hedge_side} {hedge_size} @ ~{exec_price:.2f}")

                order_id = await self.lighter.create_order(
                    symbol=symbol,
                    side=hedge_side,
                    amount=hedge_size,
                    price=exec_price,
                    order_type="MARKET"  # 强制市价
                )

                if order_id:
                    # 假定成交，更新本地 Lighter 状态 (WS 会随后确认，这里先预更新防止重复下单)
                    change = hedge_size if hedge_side == 'BUY' else -hedge_size
                    self.pos_lighter[symbol] += change
                    logger.info(f"✅ [HEDGE SENT] ID: {order_id}. Local State Updated.")
                    # 重新循环检查是否完全平衡
                    await asyncio.sleep(0.2)
                    continue
                else:
                    logger.error("❌ [Hedge] Order Failed (No ID returned)")

            except Exception as e:
                logger.error(f"❌ [Hedge Error] {e}", exc_info=True)

            retry_count += 1
            await asyncio.sleep(0.5)

        logger.critical(f"💀 [HEDGE FAILED] {symbol} failed to balance after {max_retries} retries! Stopping Maker.")
        # 紧急情况：清空 Maker 单以防风险扩大
        asyncio.create_task(self._cancel_all_maker(symbol))

    # ==================================================================
    # Maker 挂单逻辑 (GRVT)
    # ==================================================================

    async def _update_maker_quotes(self, symbol: str):
        """
        根据 Lighter 盘口 + GRVT 持仓状态，更新 GRVT 挂单
        """
        lock = self._get_lock(symbol)
        if lock.locked(): return  # 如果正在处理，跳过

        async with lock:
            # 1. 安全检查
            lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
            if not lighter_tick or lighter_tick.get('bid') == 0: return

            # 2. 检查是否处于失衡状态 (Unbalanced)
            # 如果 GRVT 和 Lighter 不匹配，优先等待对冲完成，禁止挂新单
            g_pos = self.pos_grvt.get(symbol, 0.0)
            l_pos = self.pos_lighter.get(symbol, 0.0)
            if abs(g_pos + l_pos) > 0.0001:
                # 只有当正在尝试平仓且方向正确时才允许
                # 但为了安全，失衡时通常停止 Quote
                await self._cancel_all_maker(symbol)
                return

            # 3. 计算目标挂单
            qty = settings.get_trade_qty(symbol)
            orders_to_place = []  # list of (side, price)

            # --- CASE A: 无持仓 (OPEN 模式) ---
            if abs(g_pos) < (qty * 0.1):
                # 双边挂单，赚取 Spread + Rebate
                # Lighter 买一价 -> GRVT 买单 (价格 = Lighter_Bid * (1 - margin))
                # Lighter 卖一价 -> GRVT 卖单 (价格 = Lighter_Ask * (1 + margin))

                # 使用深度加权价格更安全
                bid_ref = self._get_weighted_price(lighter_tick, 'SELL', qty)  # Lighter 卖单深度对应我们要买的成本
                ask_ref = self._get_weighted_price(lighter_tick, 'BUY', qty)  # Lighter 买单深度对应我们要卖的收入

                if bid_ref and ask_ref:
                    my_bid = bid_ref * (1 - self.spread_margin)
                    my_ask = ask_ref * (1 + self.spread_margin)

                    orders_to_place.append(('BUY', my_bid))
                    orders_to_place.append(('SELL', my_ask))

            # --- CASE B: 有持仓 (CLOSE 模式) ---
            else:
                # 必须挂平仓单
                # 如果 GRVT 多头 (g_pos > 0)，我们需要 SELL Close
                # 价格参考 Lighter 的 BUY 盘口 (因为平仓后我们要去 Lighter 卖出平掉空头? 不，平仓后 Lighter 买入平空)

                # 逻辑梳理:
                # 持有: GRVT Long, Lighter Short
                # 平仓动作: GRVT Sell, Lighter Buy
                # 成本: Lighter Ask (买入价)
                # 收入: GRVT Bid (卖出价) -> 我们的 GRVT Sell Limit Price

                if g_pos > 0:  # Long GRVT
                    # 挂 SELL 单平仓
                    # 参考 Lighter 的 Ask (我们要去 Lighter 买回来平空)
                    ref_price = self._get_weighted_price(lighter_tick, 'SELL', abs(g_pos))  # Lighter Ask Depth
                    if ref_price:
                        # 我们希望: GRVT_Sell_Price > Lighter_Buy_Cost
                        # 考虑到要尽快平仓，可以利润微薄甚至微亏(赚手续费)
                        target_price = ref_price * (1 + self.spread_margin)
                        orders_to_place.append(('SELL', target_price))

                elif g_pos < 0:  # Short GRVT
                    # 挂 BUY 单平仓
                    # 参考 Lighter 的 Bid (我们要去 Lighter 卖出平多)
                    ref_price = self._get_weighted_price(lighter_tick, 'BUY', abs(g_pos))  # Lighter Bid Depth
                    if ref_price:
                        target_price = ref_price * (1 - self.spread_margin)
                        orders_to_place.append(('BUY', target_price))

            # 4. 执行挂单/改单
            await self._reconcile_orders(symbol, orders_to_place, qty)

    async def _reconcile_orders(self, symbol: str, desired_orders: List[Tuple[str, float]], qty: float):
        """
        对比当前挂单和期望挂单，执行撤单和下单
        """
        current_orders = self.active_maker_orders.get(symbol, {})
        current_prices = self.maker_order_prices.get(symbol, {})

        # 期望的 orders 字典: side -> price
        desired_map = {side: price for side, price in desired_orders}

        # 1. 撤销不再需要的单子
        sides_to_cancel = []
        for side, oid in current_orders.items():
            if side not in desired_map:
                sides_to_cancel.append(side)
                continue

            # 检查价格是否需要更新 (Requote)
            curr_p = current_prices.get(side, 0)
            new_p = desired_map[side]
            if abs(curr_p - new_p) / curr_p > self.requote_threshold:
                sides_to_cancel.append(side)
            else:
                # 价格合适，保留，从 desired 中移除避免重复下单
                del desired_map[side]

        for side in sides_to_cancel:
            oid = current_orders[side]
            await self.grvt.cancel_order(oid, symbol=symbol)
            del self.active_maker_orders[symbol][side]
            del self.maker_order_prices[symbol][side]

        # 2. 下新单
        for side, price in desired_map.items():
            # GRVT 必须使用 Post-Only
            try:
                # 确定数量：如果是 Close 模式，数量为持仓量；如果是 Open，数量为 qty
                # 简化逻辑：_update_maker_quotes 决定逻辑，这里下单
                order_qty = qty
                if side == 'SELL' and self.pos_grvt.get(symbol, 0) > 0:
                    order_qty = abs(self.pos_grvt[symbol])  # Close Long
                elif side == 'BUY' and self.pos_grvt.get(symbol, 0) < 0:
                    order_qty = abs(self.pos_grvt[symbol])  # Close Short

                # 精度修正
                info = self.grvt.contract_map.get(f"{symbol}-USDT")
                if info:
                    tick_size = float(info['tick_size'])
                    price = round(price / tick_size) * tick_size

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
                        self.maker_order_prices[symbol] = {}
                    self.active_maker_orders[symbol][side] = oid
                    self.maker_order_prices[symbol][side] = price
                    logger.info(f"🆕 [QUOTE] {symbol} {side} {order_qty} @ {price}")

            except Exception as e:
                logger.warning(f"⚠️ Quote Failed: {e}")

    async def _cancel_all_maker(self, symbol):
        if symbol in self.active_maker_orders:
            for side, oid in list(self.active_maker_orders[symbol].items()):
                await self.grvt.cancel_order(oid, symbol=symbol)
            self.active_maker_orders[symbol] = {}
            self.maker_order_prices[symbol] = {}

    def _get_weighted_price(self, ticker, side, qty):
        # 简单深度加权，防止假单
        depth_key = 'asks_depth' if side == 'SELL' else 'bids_depth'
        depth = ticker.get(depth_key, [])
        if not depth: return ticker.get('ask' if side == 'SELL' else 'bid')

        cum_vol = 0.0
        cum_cost = 0.0
        for p, v in depth:
            p, v = float(p), float(v)
            take = min(v, qty - cum_vol)
            cum_cost += take * p
            cum_vol += take
            if cum_vol >= qty: break

        if cum_vol == 0: return None
        return cum_cost / cum_vol

    # ==================================================================
    # 后台守护 (Watchdog) - 防止 WS 丢包导致的永久缺腿
    # ==================================================================

    async def _watchdog_loop(self):
        logger.info("🐶 Watchdog started...")
        while self.running:
            try:
                await asyncio.sleep(5.0)  # 每 5 秒全量校验一次

                # 1. 同步 GRVT 真实持仓
                grvt_positions = await self.grvt.fetch_positions(symbols=self.target_symbols)
                # 2. 同步 Lighter 真实持仓 (Lighter Adapter 需支持 fetch_positions，如不支持需依赖本地累加或实现 REST)
                # 假设 Lighter Adapter 暂时返回本地缓存或通过 REST 获取
                # 这里为了稳健，如果 Lighter API 没实现，我们只能信赖本地，但建议实现 REST Sync
                lighter_positions = await self._fetch_lighter_positions_safe()

                for symbol in self.target_symbols:
                    # 提取 GRVT Pos
                    g_pos = 0.0
                    for p in grvt_positions:
                        p_sym = p.get('instrument') or p.get('symbol')
                        if symbol in p_sym:
                            sz = float(p.get('size') or p.get('contracts', 0))
                            sd = p.get('side', '').upper()
                            if sd == 'SHORT': sz = -sz
                            g_pos = sz
                            break

                    # 提取 Lighter Pos
                    l_pos = lighter_positions.get(symbol, self.pos_lighter.get(symbol, 0.0))

                    # 更新本地缓存
                    async with self.hedge_lock:
                        self.pos_grvt[symbol] = g_pos
                        self.pos_lighter[symbol] = l_pos

                        # 3. 校验平衡
                        diff = g_pos + l_pos
                        if abs(diff) > 0.0001:
                            logger.warning(f"🐶 [Watchdog] Unbalanced {symbol}: G={g_pos} L={l_pos} Diff={diff}")
                            # 触发对冲逻辑
                            await self._execute_hedge_logic(symbol)

            except Exception as e:
                logger.error(f"🐶 Watchdog Error: {e}")
                await asyncio.sleep(5)

    async def _fetch_lighter_positions_safe(self) -> Dict[str, float]:
        # 尝试调用 Lighter 的 REST 接口，如果失败返回空字典
        # 实际生产中应在 LighterAdapter 中实现 fetch_positions
        res = {}
        if hasattr(self.lighter, 'fetch_positions'):
            try:
                # 假设返回标准 list dict
                positions = await self.lighter.fetch_positions(symbols=self.target_symbols)
                if positions:
                    for p in positions:
                        res[p['symbol']] = float(p['size']) if p['side'] == 'BUY' else -float(p['size'])
            except:
                pass
        return res