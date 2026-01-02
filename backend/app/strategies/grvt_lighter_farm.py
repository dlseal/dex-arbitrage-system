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
    GRVT (Maker) + Lighter (Taker) 生产级刷量对冲策略 (Pro V6)

    修复核心:
    1. [Price Clamping] 强制挂单价格不穿过盘口，杜绝 Post-Only Reject。
    2. [Zombie Cleaner] 监听订单状态，自动清理被拒/被撤的僵尸单。
    3. [State Recovery] 增强的 Watchdog 防止状态回滚。
    """

    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_v6_Clamped"
        self.adapters = adapters

        self.grvt = adapters.get('GRVT')
        self.lighter = adapters.get('Lighter')
        if not self.grvt or not self.lighter:
            raise RuntimeError("CRITICAL: GRVT or Lighter adapter missing!")

        # --- 状态数据 ---
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 仓位状态
        self.pos_grvt: Dict[str, float] = {}
        self.pos_lighter: Dict[str, float] = {}
        self.last_grvt_fill_ts: Dict[str, float] = {}  # 用于防止 REST 数据回滚

        # 挂单管理 (Symbol -> Side -> OrderID)
        self.active_maker_orders: Dict[str, Dict[str, str]] = {}
        self.maker_order_info: Dict[str, Dict[str, dict]] = {}

        # 价格动量历史
        self.price_history: Dict[str, Deque[Tuple[float, float]]] = {}

        self.hedge_lock = asyncio.Lock()
        self.symbol_locks: Dict[str, asyncio.Lock] = {}

        # --- 配置加载 ---
        conf = settings.strategies.farming
        self.target_symbols = settings.common.target_symbols

        self.farm_side = str(getattr(conf, 'side', 'BOTH')).upper()
        self.slippage_tolerance = float(conf.max_slippage_tolerance)
        self.requote_threshold = float(conf.requote_threshold)
        self.required_depth_ratio = float(getattr(conf, 'required_depth_ratio', 3.0))

        # 风控参数
        self.min_order_lifetime = 2.0
        self.urgent_threshold = 0.01
        self.momentum_window = 2.0
        self.momentum_threshold = 0.001

        self.running = True
        logger.info(f"🛡️ [Strategy] V6 Started. Clamping=Active, Side={self.farm_side}")

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
        elif etype == 'order':  # [关键] 监听订单状态变化
            await self._process_order_event(event)

    async def _process_order_event(self, event: dict):
        """
        处理订单状态回调 (CANCELED, REJECTED, FILLED)
        这是防止 Post-Only 被拒后策略卡死的第二道防线。
        注意: 需要 Adapter 支持推送此类事件。
        """
        if event.get('exchange') != 'GRVT': return

        order_id = str(event.get('order_id', '') or event.get('id', ''))
        status = event.get('status', '').upper()
        symbol = event.get('symbol', '')

        # 如果订单已结束 (被拒、被撤、完全成交)，清理本地状态
        if status in ['CANCELED', 'REJECTED', 'FILLED', 'CLOSED']:
            if status == 'REJECTED':
                logger.warning(f"❌ [Order Rejected] {symbol} ID:{order_id} - Post-Only violation detected.")

            # 安全移除
            self._remove_order_from_memory(symbol, order_id)

    def _remove_order_from_memory(self, symbol, order_id):
        if symbol in self.active_maker_orders:
            found_side = None
            for side, oid in self.active_maker_orders[symbol].items():
                if str(oid) == str(order_id):
                    found_side = side
                    break

            if found_side:
                del self.active_maker_orders[symbol][found_side]
                if found_side in self.maker_order_info.get(symbol, {}):
                    del self.maker_order_info[symbol][found_side]

    async def _process_trade(self, trade: dict):
        symbol = trade['symbol']
        exchange = trade['exchange']
        try:
            size = float(trade['size'])
            side = trade['side']
        except:
            return

        if exchange == 'GRVT':
            logger.info(f"⚡️ [FILL DETECTED] GRVT {side} {size} @ {trade.get('price')}")
            self.last_grvt_fill_ts[symbol] = time.time()

            async with self.hedge_lock:
                current = self.pos_grvt.get(symbol, 0.0)
                change = size if side == 'BUY' else -size
                self.pos_grvt[symbol] = current + change
                await self._execute_hedge_logic(symbol)

        elif exchange == 'Lighter':
            async with self.hedge_lock:
                current = self.pos_lighter.get(symbol, 0.0)
                change = size if side == 'BUY' else -size
                self.pos_lighter[symbol] = current + change
                logger.info(f"✅ [HEDGE CONFIRMED] Lighter {side} {size}. Net: {self.pos_lighter[symbol]}")

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']
        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if exchange == 'Lighter':
            self._update_price_history(symbol, tick)
            asyncio.create_task(self._update_maker_quotes(symbol))
        # GRVT tick 同样重要，用于 Price Clamping
        elif exchange == 'GRVT':
            pass

    def _update_price_history(self, symbol: str, tick: dict):
        mid = (float(tick['bid']) + float(tick['ask'])) / 2
        now = time.time()
        if symbol not in self.price_history:
            self.price_history[symbol] = collections.deque()
        history = self.price_history[symbol]
        history.append((now, mid))
        while history and history[0][0] < now - self.momentum_window:
            history.popleft()

    def _detect_market_momentum(self, symbol: str) -> str:
        history = self.price_history.get(symbol)
        if not history or len(history) < 2: return 'NEUTRAL'
        start, end = history[0][1], history[-1][1]
        if start == 0: return 'NEUTRAL'
        pct = (end - start) / start
        if pct > self.momentum_threshold: return 'BULLISH'
        if pct < -self.momentum_threshold: return 'BEARISH'
        return 'NEUTRAL'

    # ==================================================================
    # 核心对冲逻辑 (Lighter Taker)
    # ==================================================================

    async def _execute_hedge_logic(self, symbol: str):
        retry_count = 0
        max_retries = 5

        while retry_count < max_retries:
            try:
                grvt_p = self.pos_grvt.get(symbol, 0.0)
                lighter_p = self.pos_lighter.get(symbol, 0.0)
                diff = -grvt_p - lighter_p

                if abs(diff) < 0.0001: return

                hedge_side = 'BUY' if diff > 0 else 'SELL'
                hedge_size = abs(diff)

                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    await asyncio.sleep(0.2)
                    retry_count += 1
                    continue

                # Taker 对冲: 激进吃单
                ref_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']
                limit_price = ref_price * 1.015 if hedge_side == 'BUY' else ref_price * 0.985

                logger.info(f"🌊 [FIRING HEDGE] Lighter {hedge_side} {hedge_size} @ ~{limit_price:.4f}")

                order_id = await self.lighter.create_order(
                    symbol=symbol,
                    side=hedge_side,
                    amount=hedge_size,
                    price=limit_price,
                    order_type="MARKET"
                )

                if order_id:
                    change = hedge_size if hedge_side == 'BUY' else -hedge_size
                    self.pos_lighter[symbol] += change
                    await asyncio.sleep(0.1)
                    continue

            except Exception as e:
                logger.error(f"❌ [Hedge Error] {e}")

            retry_count += 1
            await asyncio.sleep(0.3)

        logger.critical(f"💀 [HEDGE FAILED] {symbol} - Stopping Quotes")
        asyncio.create_task(self._cancel_all_maker(symbol))

    # ==================================================================
    # Maker 挂单逻辑 (GRVT) - 核心修复: Price Clamping
    # ==================================================================

    async def _update_maker_quotes(self, symbol: str):
        lock = self._get_lock(symbol)
        if lock.locked(): return

        async with lock:
            lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
            grvt_tick = self.tickers.get(symbol, {}).get('GRVT')

            if not lighter_tick or lighter_tick.get('bid') == 0: return
            # 如果没有 GRVT 盘口，无法做 Clamping，暂时跳过
            if not grvt_tick or grvt_tick.get('bid') == 0: return

            g_pos = self.pos_grvt.get(symbol, 0.0)
            l_pos = self.pos_lighter.get(symbol, 0.0)
            is_balanced = abs(g_pos + l_pos) < 0.0001

            qty = settings.get_trade_qty(symbol)
            orders_to_place = []

            # 获取 GRVT 盘口，用于防止穿仓
            grvt_bid = grvt_tick['bid']
            grvt_ask = grvt_tick['ask']
            # 获取 Tick Size
            info = self.grvt.contract_map.get(f"{symbol}-USDT")
            tick_size = float(info['tick_size']) if info else 0.1

            # --- 场景 A: 正常做市 (OPEN) ---
            if is_balanced and abs(g_pos) < (qty * 0.1):
                safe_qty = self._check_liquidity(lighter_tick, qty)
                if safe_qty < (qty * 0.1):
                    await self._cancel_all_maker(symbol)
                    return

                momentum = self._detect_market_momentum(symbol)
                allow_buy = momentum != 'BEARISH'
                allow_sell = momentum != 'BULLISH'

                bid_ref = self._get_weighted_price(lighter_tick, 'BUY', safe_qty)
                ask_ref = self._get_weighted_price(lighter_tick, 'SELL', safe_qty)

                if bid_ref and ask_ref:
                    should_buy = self.farm_side in ['BOTH', 'BUY']
                    should_sell = self.farm_side in ['BOTH', 'SELL']

                    if should_buy and allow_buy:
                        target_price = bid_ref * (1 - self.slippage_tolerance)
                        # Clamping: 买单最高不能超过 Ask - Tick (或者 Bid)
                        # 为了稳健，不能高于 (GRVT_Ask - Tick)
                        limit_price = min(target_price, grvt_ask - tick_size)
                        orders_to_place.append(('BUY', limit_price))

                    if should_sell and allow_sell:
                        target_price = ask_ref * (1 + self.slippage_tolerance)
                        # Clamping: 卖单最低不能低于 Bid + Tick
                        # 为了稳健，不能低于 (GRVT_Bid + Tick)
                        limit_price = max(target_price, grvt_bid + tick_size)
                        orders_to_place.append(('SELL', limit_price))

            # --- 场景 B: 平仓模式 (CLOSE) ---
            else:
                close_tolerance = min(self.slippage_tolerance, -0.0005)

                if g_pos > 0:  # Long -> Sell to Close
                    ref = self._get_weighted_price(lighter_tick, 'SELL', abs(g_pos))
                    if ref:
                        target_price = ref * (1 + close_tolerance)
                        # [重要] 即使要平仓，Post-Only 也不能穿过买一价
                        # 如果计算出的价格太低，强制提升到 GRVT_Bid + Tick
                        # 虽然这可能导致无法立即成交，但比被拒单要好
                        limit_price = max(target_price, grvt_bid + tick_size)
                        orders_to_place.append(('SELL', limit_price))

                elif g_pos < 0:  # Short -> Buy to Close
                    ref = self._get_weighted_price(lighter_tick, 'BUY', abs(g_pos))
                    if ref:
                        target_price = ref * (1 - close_tolerance)
                        # [重要] 强制降低到 GRVT_Ask - Tick
                        limit_price = min(target_price, grvt_ask - tick_size)
                        orders_to_place.append(('BUY', limit_price))

            await self._reconcile_orders(symbol, orders_to_place, qty)

    def _check_liquidity(self, ticker, target_qty) -> float:
        required = target_qty * self.required_depth_ratio
        bid_vol = sum([float(x[1]) for x in ticker.get('bids_depth', [])[:5]])
        ask_vol = sum([float(x[1]) for x in ticker.get('asks_depth', [])[:5]])
        min_liq = min(bid_vol, ask_vol)
        if min_liq < required:
            return max(0.0, min_liq / self.required_depth_ratio)
        return target_qty

    async def _reconcile_orders(self, symbol: str, desired_orders: List[Tuple[str, float]], qty: float):
        current_orders = self.active_maker_orders.get(symbol, {})
        current_info = self.maker_order_info.get(symbol, {})
        desired_map = {side: price for side, price in desired_orders}
        now = time.time()

        sides_to_cancel = []
        for side, oid in current_orders.items():
            info = current_info.get(side, {})
            curr_p, ts = info.get('price', 0), info.get('ts', 0)

            if side not in desired_map:
                sides_to_cancel.append(side)
                continue

            new_p = desired_map[side]
            diff_pct = abs(curr_p - new_p) / curr_p if curr_p else 0

            life_span = now - ts
            is_urgent = diff_pct > self.urgent_threshold
            is_mature = life_span >= self.min_order_lifetime

            if is_urgent:
                sides_to_cancel.append(side)
            elif diff_pct > self.requote_threshold:
                if is_mature:
                    sides_to_cancel.append(side)
                else:
                    del desired_map[side]
            else:
                del desired_map[side]

        for side in sides_to_cancel:
            await self.grvt.cancel_order(current_orders[side], symbol=symbol)
            # 乐观移除，等待 _process_order_event 确认
            self._remove_order_from_memory(symbol, current_orders[side])

        for side, price in desired_map.items():
            order_qty = qty
            if side == 'SELL' and self.pos_grvt.get(symbol, 0) > 0:
                order_qty = abs(self.pos_grvt[symbol])
            elif side == 'BUY' and self.pos_grvt.get(symbol, 0) < 0:
                order_qty = abs(self.pos_grvt[symbol])

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
                    self.maker_order_info[symbol][side] = {'price': price, 'ts': time.time()}
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
        depth_key = 'bids_depth' if side == 'BUY' else 'asks_depth'
        depth = ticker.get(depth_key, [])
        base_price = ticker.get('bid' if side == 'BUY' else 'ask')
        if not depth: return base_price
        cum_vol, cum_cost = 0.0, 0.0
        for p, v in depth:
            p, v = float(p), float(v)
            take = min(v, qty - cum_vol)
            cum_cost += take * p
            cum_vol += take
            if cum_vol >= qty: break
        if cum_vol == 0: return base_price
        return cum_cost / cum_vol

    async def _watchdog_loop(self):
        logger.info("🐶 Watchdog started...")
        while self.running:
            try:
                await asyncio.sleep(5.0)

                grvt_positions = await self.grvt.fetch_positions(symbols=self.target_symbols)
                lighter_positions = await self._fetch_lighter_positions_safe()

                for symbol in self.target_symbols:
                    last_fill = self.last_grvt_fill_ts.get(symbol, 0)
                    trust_rest = (time.time() - last_fill) > 15.0

                    rest_g_pos = 0.0
                    for p in grvt_positions:
                        p_sym = p.get('instrument') or p.get('symbol')
                        if symbol in p_sym:
                            sz = float(p.get('size') or p.get('contracts', 0))
                            if p.get('side', '').upper() == 'SHORT': sz = -sz
                            rest_g_pos = sz
                            break

                    l_pos = lighter_positions.get(symbol, self.pos_lighter.get(symbol, 0.0))

                    async with self.hedge_lock:
                        if trust_rest:
                            self.pos_grvt[symbol] = rest_g_pos
                        self.pos_lighter[symbol] = l_pos

                        g_final = self.pos_grvt.get(symbol, 0.0)
                        if abs(g_final + l_pos) > 0.0001:
                            logger.warning(f"🐶 [Watchdog] Unbalanced {symbol}: G={g_final} L={l_pos}")
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