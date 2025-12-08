import asyncio
import logging
import time
from typing import Dict, Any, Optional, Set, Tuple
from app.config import Config

logger = logging.getLogger("SmartFarm_Pro_v13")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_SmartFarm_Pro_v13_ProfitUnlocked"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # --- 订单状态 ---
        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}
        self.pending_orders: Set[str] = set()

        self.order_create_time: Dict[str, float] = {}
        self.last_heartbeat = time.time()

        # --- 仓位 ---
        self.pos_cache: Dict[str, float] = {}
        self.pos_cache_time: Dict[str, float] = {}

        # --- 并发 ---
        self.locks: Dict[str, asyncio.Lock] = {}
        self.last_quote_time: Dict[str, float] = {}

        # --- 配置 ---
        self.symbol_sides: Dict[str, str] = {}
        self.initial_side = Config.FARM_SIDE.upper()
        self.target_margin = Config.MAX_SLIPPAGE_TOLERANCE
        self.requote_threshold = getattr(Config, 'REQUOTE_THRESHOLD', 0.0005)
        self.REQUIRED_DEPTH_RATIO = 1.5

        logger.info(f"🛡️ SmartFarm v13 启动 | Profit Taker: ON | Post-Only: Hybrid")

    def _get_lock(self, symbol: str):
        if symbol not in self.locks:
            self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    def _get_current_side(self, symbol: str) -> str:
        return self.symbol_sides.get(symbol, 'BUY' if self.initial_side == 'BOTH' else self.initial_side)

    def _flip_side(self, symbol: str):
        current = self._get_current_side(symbol)
        new_side = 'SELL' if current == 'BUY' else 'BUY'
        self.symbol_sides[symbol] = new_side
        logger.info(f"🔄 [Flip] {symbol}: {current} -> {new_side}")

    async def on_tick(self, event: dict):
        if time.time() - self.last_heartbeat > 60:
            logger.info(f"💓 Heartbeat | Active: {len(self.active_orders)}")
            self.last_heartbeat = time.time()

        try:
            event_type = event.get('type', 'tick')
            if event_type == 'trade':
                await self._process_trade_fill(event)
            elif event_type == 'tick':
                await self._process_tick(event)
        except Exception as e:
            logger.error(f"Tick Error: {e}", exc_info=True)

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']
        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        lock = self._get_lock(symbol)
        if lock.locked() or symbol in self.pending_orders: return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            now = time.time()
            if now - self.last_quote_time.get(symbol, 0) < 0.5: return
            self.last_quote_time[symbol] = now
            asyncio.create_task(self._manage_maker_orders(symbol))

    async def _manage_maker_orders(self, symbol: str):
        if self._get_lock(symbol).locked() or symbol in self.pending_orders: return

        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        # 数据校验
        if grvt_tick.get('bid', 0) <= 0 or grvt_tick.get('ask', 0) <= 0: return

        maker_side = self._get_current_side(symbol)

        # 1. 计算价格和下单类型 (Maker vs Taker)
        result = self._calculate_price_and_type(symbol, grvt_tick, lighter_tick, maker_side)
        if not result: return

        target_price, is_post_only = result

        current_order_id = self.active_orders.get(symbol)
        current_price = self.active_order_prices.get(symbol)
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # 2. 挂单逻辑
        if not current_order_id:
            self.pending_orders.add(symbol)

            # 日志区分
            tag = "⚡️ [TAKER]" if not is_post_only else "🆕 [MAKER]"
            logger.info(f"{tag} {symbol} {maker_side} {quantity} @ {target_price}")

            self.order_create_time[symbol] = time.time()
            asyncio.create_task(self._place_order_task(symbol, maker_side, quantity, target_price, is_post_only))

        # 3. 改单逻辑 (仅 Maker 单需要改)
        else:
            order_age = time.time() - self.order_create_time.get(symbol, 0)
            price_diff_pct = abs(target_price - current_price) / current_price if current_price else 0

            should_requote = False
            # Taker 机会出现，且偏差大 -> 立即重挂为 Taker
            if not is_post_only and price_diff_pct > 0.0001:
                should_requote = True
            # 普通 Maker 调价
            elif price_diff_pct > self.requote_threshold and order_age > 1.0:
                should_requote = True
            # 僵尸单清理
            elif order_age > 15.0:
                should_requote = True

            if should_requote:
                self.pending_orders.add(symbol)
                logger.info(
                    f"♻️ [Requote] {symbol} New: {target_price} (Type: {'PostOnly' if is_post_only else 'Taker'})")
                asyncio.create_task(self._cancel_order_task(symbol, current_order_id))

    def _calculate_price_and_type(self, symbol: str, grvt_tick: dict, lighter_tick: dict, side: str) -> Optional[
        Tuple[float, bool]]:
        """
        返回: (target_price, is_post_only)
        """
        adapter = self.adapters['GRVT']
        info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(info['tick_size']) if info else 0.01

        qty = Config.TRADE_QUANTITIES.get(symbol, 0.0001)
        required_qty = qty * self.REQUIRED_DEPTH_RATIO

        hedge_price = self._get_depth_weighted_price(lighter_tick, 'SELL' if side == 'BUY' else 'BUY', required_qty)
        if not hedge_price: return None

        market_ask = grvt_tick['ask']
        market_bid = grvt_tick['bid']

        is_post_only = True  # 默认 Maker

        # 定义一个仅用于判断是否"真正"套利的阈值（例如必须有 0.05% 正利润才吃单）
        # 防止因为负滑点配置导致频繁 Taker
        TAKER_PROFIT_THRESHOLD = 0.0005

        if side == 'BUY':
            raw_target = hedge_price * (1 - self.target_margin)

            # 计算是否有"真实"的套利利润（不包含您的负滑点意愿）
            real_arb_target = hedge_price * (1 - TAKER_PROFIT_THRESHOLD)

            # 只有当 真实套利目标 都能吃掉 卖一价 时，才做 Taker
            if real_arb_target >= market_ask:
                # 真正的暴利机会，Taker 吃单
                target_price = raw_target
                is_post_only = False
            else:
                # 否则，即使 raw_target 很高，也强制作为 Maker 挂在卖一价下面
                limit_price = market_ask - tick_size
                target_price = min(raw_target, limit_price)
                is_post_only = True

        else:  # SELL
            raw_target = hedge_price * (1 + self.target_margin)

            # 计算是否有"真实"的套利利润
            real_arb_target = hedge_price * (1 + TAKER_PROFIT_THRESHOLD)

            if real_arb_target <= market_bid:
                # 真正的暴利机会，Taker 吃单
                target_price = raw_target
                is_post_only = False
            else:
                # 否则，强制 Maker 挂在买一价上面
                limit_price = market_bid + tick_size
                target_price = max(raw_target, limit_price)
                is_post_only = True

        return target_price, is_post_only

    def _get_depth_weighted_price(self, ticker, side, required_qty):
        depth = ticker.get('asks_depth' if side == 'BUY' else 'bids_depth')
        if not depth: return ticker.get('ask' if side == 'BUY' else 'bid')

        collected, cost = 0.0, 0.0
        for p_str, s_str in depth:
            p, s = float(p_str), float(s_str)
            take = min(s, required_qty - collected)
            cost += take * p
            collected += take
            if collected >= required_qty: break

        if collected < required_qty * 0.5: return None
        return cost / collected

    async def _place_order_task(self, symbol, side, qty, price, post_only):
        """
        [紧急修复版]
        1. 允许平仓单通过 (Allow Closing Orders)
        2. 移除拦截器中的自动对冲 (防止无限对冲)
        """
        # 1. 获取当前持仓 (使用缓存避免 429)
        current_pos = await self._check_actual_position(symbol)

        # 2. 智能拦截逻辑
        # 如果有持仓，我们需要判断：这是"加仓"还是"平仓"？
        if abs(current_pos) > (qty * 0.1):
            is_closing = False

            # 逻辑：持多单(>0)且卖出(SELL) = 平仓 -> 允许
            if current_pos > 0 and side.upper() == 'SELL':
                is_closing = True
            # 逻辑：持空单(<0)且买入(BUY) = 平仓 -> 允许
            elif current_pos < 0 and side.upper() == 'BUY':
                is_closing = True

            if is_closing:
                logger.info(f"✅ [Interceptor] 放行平仓单: {side} {qty} (当前持仓: {current_pos})")
            else:
                # 如果是同方向加仓（例如持多还在买），则必须拦截！
                # 警告：这里绝对不能触发自动对冲，否则会死循环！
                logger.warning(f"🛑 [Interceptor] 拦截加仓单: {side} (当前持仓: {current_pos}) - 等待 Flip 逻辑修正")
                self.pending_orders.discard(symbol)
                return

        try:
            # 3. 正常下单
            # 增加打印，确认正在下单
            # logger.info(f"🚀 发送下单请求: {symbol} {side} {price}")

            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT",
                side=side,
                amount=qty,
                price=price,
                params={'post_only': post_only}
            )

            if new_id:
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = price
                logger.info(f"✅ Placed: {symbol} {side} {qty} @ {price} (ID: {new_id})")
            else:
                logger.warning(f"⚠️ No ID returned for {symbol}")

        except Exception as e:
            logger.error(f"❌ Order failed: {e}")
        finally:
            self.pending_orders.discard(symbol)

    async def _check_actual_position(self, symbol: str) -> float:
        """带缓存的持仓查询"""
        now = time.time()
        # 缓存有效期 2 秒
        if symbol in self.pos_cache and (now - self.pos_cache_time.get(symbol, 0) < 2.0):
            return self.pos_cache[symbol]

        try:
            adapter = self.adapters.get('GRVT')
            if not adapter: return 0.0

            loop = asyncio.get_running_loop()
            positions = await loop.run_in_executor(
                None,
                lambda: adapter.rest_client.fetch_positions(
                    params={'sub_account_id': adapter.trading_account_id}
                )
            )
            found_size = 0.0
            for p in positions:
                inst_id = p.get('instrument') or p.get('symbol') or ""
                if symbol in inst_id:
                    size = float(p.get('size', 0) or p.get('contracts', 0))
                    # 修正空单符号（如果 API 返回绝对值）
                    if size > 0 and p.get('side', '').upper() == 'SHORT':
                        size = -size
                    found_size = size
                    break

            self.pos_cache[symbol] = found_size
            self.pos_cache_time[symbol] = now
            return found_size

        except Exception as e:
            logger.error(f"Position check failed: {e}")
            # API 失败时返回缓存值，避免返回 0 误导逻辑
            return self.pos_cache.get(symbol, 0.0)

    async def _cancel_order_task(self, symbol, order_id):
        """
        [修改版] 必须等待撤单确认，才清理内存状态
        """
        cancel_success = False
        try:
            # 1. 发起撤单
            await self.adapters['GRVT'].cancel_order(order_id, symbol=symbol)

            # 2. 如果代码能走到这里，说明 API 调用没有抛出异常，视为成功
            cancel_success = True
            logger.info(f"🗑️ 撤单请求已发送: {symbol} (ID: {order_id})")

        except Exception as e:
            err_msg = str(e).lower()
            # 3. 特殊处理：如果报错是“订单不存在”或“已完成”，也视为撤单成功（目的是清理内存）
            if "not found" in err_msg or "filled" in err_msg or "cancelled" in err_msg:
                cancel_success = True
                logger.warning(f"⚠️ 订单已失效，清理内存: {e}")
            else:
                logger.error(f"❌ 撤单失败 (保留内存状态): {e}")
                # 撤单失败了，意味着订单可能还在挂着。
                # 此时绝对不能 del self.active_orders[symbol]，否则主循环会以为没单子了，又去挂一个新的
                pass

        # 4. 只有在确认撤单成功（或订单消失）时，才清理内存
        if cancel_success:
            if symbol in self.active_orders and self.active_orders[symbol] == order_id:
                del self.active_orders[symbol]
                if symbol in self.active_order_prices:
                    del self.active_order_prices[symbol]

        # 释放锁
        self.pending_orders.discard(symbol)

    async def _process_trade_fill(self, trade: dict):
        if trade['exchange'] != 'GRVT': return
        symbol = trade['symbol']

        lock = self._get_lock(symbol)
        async with lock:
            logger.info(f"🚨 [FILLED] GRVT {trade['side']} {trade['size']} -> HEDGING!")

            if symbol in self.active_orders: del self.active_orders[symbol]
            self.pending_orders.discard(symbol)

            await self._execute_hedge_loop(symbol, trade['side'], float(trade['size']))

    async def _execute_hedge_loop(self, symbol, grvt_side, size):
        hedge_side = 'SELL' if grvt_side.upper() == 'BUY' else 'BUY'
        retry = 0
        while retry < 10:
            try:
                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    await asyncio.sleep(0.1);
                    continue

                base_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']
                if base_price <= 0:
                    retry += 1;
                    await asyncio.sleep(0.2);
                    continue

                exec_price = base_price * 1.05 if hedge_side == 'BUY' else base_price * 0.95
                logger.info(f"🌊 [Hedge] {hedge_side} {size} @ {exec_price:.2f} (Try {retry + 1})")

                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol, side=hedge_side, amount=size, price=exec_price, order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ Hedge Success ID: {order_id}")
                    self._flip_side(symbol)
                    return
            except Exception as e:
                logger.error(f"❌ Hedge Retry {retry} Failed: {e}")

            retry += 1
            await asyncio.sleep(0.5)

        logger.critical(f"💀💀💀 CRITICAL: {symbol} Hedge FAILED. Manual Intervention Required!")