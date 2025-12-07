import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from app.config import Config

logger = logging.getLogger("InventoryFarm")


class GrvtInventoryFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "Grvt_Inventory_Grid_v2"  # Version bumped
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        self.max_inventory_usd = Config.MAX_INVENTORY_USD
        self.layers = Config.INVENTORY_LAYERS
        self.layer_spread = Config.INVENTORY_LAYER_SPREAD

        # 优化：从配置读取重挂单阈值，避免无效撤单
        self.requote_threshold = getattr(Config, 'REQUOTE_THRESHOLD', 0.0005)

        self.current_inventory: Dict[str, float] = {}
        self.active_orders: Dict[str, Dict[str, float]] = {}  # {order_id: price}

        # 优化：库存读写锁，防止 WS 和 REST 线程竞态
        self.inventory_lock = asyncio.Lock()

        self.locks: Dict[str, asyncio.Lock] = {}
        self.last_quote_time: Dict[str, float] = {}
        self.hedge_cooldowns: Dict[str, float] = {}

        self.is_ready = False
        logger.info(
            f"🚜 InventoryFarm V2 启动 | 目标持仓上限: ${self.max_inventory_usd} | 重挂阈值: {self.requote_threshold}")
        asyncio.create_task(self._sync_position_loop())

    def _get_lock(self, symbol: str):
        if symbol not in self.locks: self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    async def _sync_position_loop(self):
        """定期从 REST API 同步真实持仓，修正偏差"""
        while True:
            try:
                await asyncio.sleep(15 if self.is_ready else 5)
                adapter = self.adapters.get('GRVT')
                if not adapter or not adapter.is_connected: continue

                positions = await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: adapter.rest_client.fetch_positions(
                        params={'sub_account_id': adapter.trading_account_id})
                )

                async with self.inventory_lock:
                    for symbol in Config.TARGET_SYMBOLS:
                        real_pos = 0.0
                        for p in positions:
                            if symbol in p.get('symbol', '') or symbol == p.get('instrument', '').split('-')[0]:
                                real_pos = float(p.get('contracts', 0) or p.get('size', 0))
                                break

                        local_pos = self.current_inventory.get(symbol, 0.0)
                        if abs(real_pos - local_pos) > 0.0001:
                            logger.warning(f"⚠️ [持仓校准] {symbol} 本地:{local_pos} -> 真实:{real_pos}")
                            self.current_inventory[symbol] = real_pos

                if not self.is_ready:
                    self.is_ready = True
                    logger.info("✅ 初始持仓同步完成，策略开始运行")
            except Exception as e:
                logger.error(f"❌ 持仓同步失败: {e}")

    async def on_tick(self, event: dict):
        if not self.is_ready: return
        if event.get('symbol') not in Config.TARGET_SYMBOLS: return

        try:
            if event.get('type') == 'trade':
                await self._process_trade(event)
            elif event.get('type') == 'tick':
                await self._process_tick(event)
        except Exception as e:
            logger.error(f"Strategy Error: {e}")

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            lock = self._get_lock(symbol)
            if not lock.locked():
                await self._update_grid_orders(symbol)

    async def _update_grid_orders(self, symbol):
        if time.time() < self.hedge_cooldowns.get(symbol, 0):
            return

        now = time.time()
        if now - self.last_quote_time.get(symbol, 0) < 1.0: return
        self.last_quote_time[symbol] = now

        current_pos = self.current_inventory.get(symbol, 0.0)
        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        market_price = grvt_tick['bid']
        if market_price <= 0: return
        pos_value = current_pos * market_price

        target_side = Config.FARM_SIDE.upper()

        is_full = False
        if target_side == 'BUY' and pos_value >= self.max_inventory_usd: is_full = True
        if target_side == 'SELL' and pos_value <= -self.max_inventory_usd: is_full = True

        if is_full:
            if not self._get_lock(symbol).locked():
                logger.info(f"🌕 [满仓] {symbol} 持仓 ${pos_value:.2f} -> 触发对冲")
                asyncio.create_task(self._execute_batch_hedge(symbol))
            return

        await self._place_layered_orders(symbol, target_side, grvt_tick, lighter_tick)

    def _should_update_grid(self, symbol: str, target_prices: List[float]) -> bool:
        """核心优化：判断是否需要更新订单"""
        current_orders = self.active_orders.get(symbol, {})
        if not current_orders:
            return True

        if len(current_orders) != len(target_prices):
            return True

        existing_prices = sorted(list(current_orders.values()), reverse=True)
        target_sorted = sorted(target_prices, reverse=True)

        for p_old, p_new in zip(existing_prices, target_sorted):
            if p_new == 0: continue
            diff_pct = abs(p_old - p_new) / p_new
            if diff_pct > self.requote_threshold:
                return True

        return False

    async def _place_layered_orders(self, symbol, side, grvt_tick, lighter_tick):
        adapter = self.adapters['GRVT']
        info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(info['tick_size']) if info else 0.01

        base_price = grvt_tick['ask'] if side == 'BUY' else grvt_tick['bid']
        target_prices = []
        for i in range(self.layers):
            spread_ticks = 1 + i * self.layer_spread
            p = base_price - (tick_size * spread_ticks) if side == 'BUY' else base_price + (tick_size * spread_ticks)
            target_prices.append(p)

        hedge_price = lighter_tick['bid'] if side == 'BUY' else lighter_tick['ask']
        if hedge_price <= 0: return

        est_pnl = (hedge_price - target_prices[0]) / target_prices[0] if side == 'BUY' else (target_prices[
                                                                                                 0] - hedge_price) / hedge_price

        if est_pnl < Config.MAX_SLIPPAGE_TOLERANCE:
            if self.active_orders.get(symbol):
                await self._cancel_all(symbol)
            return

        if not self._should_update_grid(symbol, target_prices):
            return

        if self.active_orders.get(symbol):
            await self._cancel_all(symbol)

        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        tasks = []
        for p in target_prices:
            tasks.append(adapter.create_order(symbol=f"{symbol}-USDT", side=side, amount=quantity, price=p))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        if symbol not in self.active_orders: self.active_orders[symbol] = {}
        valid_orders = 0
        for res, p in zip(results, target_prices):
            if isinstance(res, str) and res:
                self.active_orders[symbol][res] = p
                valid_orders += 1
            elif isinstance(res, Exception):
                logger.error(f"Order failed: {res}")

        if valid_orders > 0:
            logger.info(
                f"⛓️ [Grid Update] {symbol} 挂单 x{valid_orders} @ {target_prices[0]:.2f} (Est PnL: {est_pnl * 100:.3f}%)")

    async def _cancel_all(self, symbol):
        orders = self.active_orders.get(symbol, {})
        if not orders: return

        order_ids = list(orders.keys())
        tasks = [self.adapters['GRVT'].cancel_order(oid) for oid in order_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

        self.active_orders[symbol] = {}

    async def _process_trade(self, trade: dict):
        if trade['exchange'] != 'GRVT': return
        symbol = trade['symbol']
        side = trade['side']
        size = trade['size']

        change = size if side == 'BUY' else -size

        async with self.inventory_lock:
            old_pos = self.current_inventory.get(symbol, 0.0)
            self.current_inventory[symbol] = old_pos + change
            new_pos = self.current_inventory[symbol]

        logger.info(f"⚡️ {symbol} 成交 {side} {size} | 库存: {old_pos:.4f} -> {new_pos:.4f}")

        market_price = trade['price']
        if abs(new_pos * market_price) >= self.max_inventory_usd * 1.1:
            if not self._get_lock(symbol).locked():
                logger.warning(f"🔥 [突发满仓] 库存激增，立即对冲！")
                asyncio.create_task(self._execute_batch_hedge(symbol))

    async def _execute_batch_hedge(self, symbol):
        lock = self._get_lock(symbol)
        if lock.locked(): return

        async with lock:
            await self._cancel_all(symbol)

            pos = self.current_inventory.get(symbol, 0.0)
            if abs(pos) < 0.0001: return

            hedge_side = 'SELL' if pos > 0 else 'BUY'
            hedge_size = abs(pos)

            logger.info(f"🌊 [开始对冲] 目标: Lighter {hedge_side} {hedge_size}")

            try:
                if 'Lighter' not in self.tickers[symbol]:
                    raise Exception("Lighter 数据缺失，无法对冲")

                lighter_tick = self.tickers[symbol]['Lighter']
                base_price = lighter_tick['bid'] if hedge_side == 'SELL' else lighter_tick['ask']
                if base_price <= 0:
                    raise Exception("Lighter 价格无效 (0)")

                exec_price = base_price * 0.95 if hedge_side == 'SELL' else base_price * 1.05

                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol, side=hedge_side, amount=hedge_size, price=exec_price, order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ [对冲完成] Lighter ID: {order_id}")
                    async with self.inventory_lock:
                        self.current_inventory[symbol] = 0.0
                else:
                    raise Exception("Lighter 返回 OrderID 为空")

            except Exception as e:
                logger.error(f"❌ 对冲严重失败: {e}")
                self.hedge_cooldowns[symbol] = time.time() + 10.0
                logger.warning(f"⏳ {symbol} 进入 10s 紧急冷却")

            await asyncio.sleep(1.0)