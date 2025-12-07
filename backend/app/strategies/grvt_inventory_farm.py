import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from app.config import Config

logger = logging.getLogger("InventoryFarm")


class GrvtInventoryFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "Grvt_Inventory_Grid_v2.1_Fixed"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        self.max_inventory_usd = Config.MAX_INVENTORY_USD
        self.layers = Config.INVENTORY_LAYERS
        self.layer_spread = Config.INVENTORY_LAYER_SPREAD
        self.requote_threshold = getattr(Config, 'REQUOTE_THRESHOLD', 0.0005)

        self.current_inventory: Dict[str, float] = {}
        self.active_orders: Dict[str, Dict[str, float]] = {}

        # 锁：inventory_lock 保护持仓数据；action_locks 保护下单/撤单原子性
        self.inventory_lock = asyncio.Lock()
        self.action_locks: Dict[str, asyncio.Lock] = {}

        self.last_quote_time: Dict[str, float] = {}
        self.hedge_cooldowns: Dict[str, float] = {}

        self.is_ready = False
        logger.info(f"🚜 InventoryFarm Ready | Max Inv: ${self.max_inventory_usd}")

        asyncio.create_task(self._sync_position_loop())

    def _get_action_lock(self, symbol: str):
        if symbol not in self.action_locks:
            self.action_locks[symbol] = asyncio.Lock()
        return self.action_locks[symbol]

    async def _sync_position_loop(self):
        """定期校准持仓，修复 WS 推送遗漏导致的偏差"""
        while True:
            try:
                await asyncio.sleep(20)
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
                            if symbol in p.get('instrument', '') or symbol == p.get('symbol', ''):
                                real_pos = float(p.get('contracts', 0) or p.get('size', 0))
                                break

                        local_pos = self.current_inventory.get(symbol, 0.0)
                        if abs(real_pos - local_pos) > 0.0001:
                            logger.warning(f"⚠️ [Sync] {symbol} Fix: {local_pos} -> {real_pos}")
                            self.current_inventory[symbol] = real_pos

                if not self.is_ready:
                    self.is_ready = True
                    logger.info("✅ Position Synced. Strategy Start.")

            except Exception as e:
                logger.error(f"❌ Position Sync Failed: {e}")

    async def on_tick(self, event: dict):
        if not self.is_ready: return

        try:
            if event.get('type') == 'trade':
                await self._process_trade(event)
            elif event.get('type') == 'tick':
                symbol = event.get('symbol')
                if symbol in Config.TARGET_SYMBOLS:
                    # 关键优化：不要在 on_tick 里 await 耗时操作
                    # 使用 create_task 将计算逻辑放入后台，防止阻塞 Engine
                    asyncio.create_task(self._process_tick_logic(event))
        except Exception as e:
            logger.error(f"Strategy Error: {e}")

    async def _process_tick_logic(self, tick: dict):
        """处理行情的后台任务"""
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            lock = self._get_action_lock(symbol)

            # 如果正在执行下单/撤单（Locked），则跳过本次 Tick，防止逻辑重入冲突
            if not lock.locked():
                async with lock:
                    await self._update_grid_orders(symbol)

    async def _update_grid_orders(self, symbol):
        if time.time() < self.hedge_cooldowns.get(symbol, 0): return
        if time.time() - self.last_quote_time.get(symbol, 0) < 1.0: return

        self.last_quote_time[symbol] = time.time()

        async with self.inventory_lock:
            current_pos = self.current_inventory.get(symbol, 0.0)

        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        market_price = grvt_tick['bid']
        if market_price <= 0: return

        pos_value = current_pos * market_price
        target_side = Config.FARM_SIDE.upper()

        is_full_buy = (target_side == 'BUY' and pos_value >= self.max_inventory_usd)
        is_full_sell = (target_side == 'SELL' and pos_value <= -self.max_inventory_usd)

        if is_full_buy or is_full_sell:
            logger.info(f"🌕 [Full] {symbol} Value: ${pos_value:.0f}. Stop Quoting.")
            await self._cancel_local_orders(symbol)
            return

        target_prices = self._calculate_grid_prices(symbol, target_side, grvt_tick)

        hedge_price = lighter_tick['bid'] if target_side == 'BUY' else lighter_tick['ask']
        if hedge_price <= 0: return

        est_pnl = (hedge_price - target_prices[0]) / target_prices[0] if target_side == 'BUY' else \
            (target_prices[0] - hedge_price) / hedge_price

        if est_pnl < Config.MAX_SLIPPAGE_TOLERANCE:
            await self._cancel_local_orders(symbol)
            return

        if not self._should_update_grid(symbol, target_prices):
            return

        await self._cancel_local_orders(symbol)
        await self._place_new_orders(symbol, target_side, target_prices)

    def _calculate_grid_prices(self, symbol, side, tick):
        adapter = self.adapters['GRVT']
        info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(info['tick_size']) if (info and 'tick_size' in info) else 0.01

        base_price = tick['ask'] if side == 'BUY' else tick['bid']
        prices = []
        for i in range(self.layers):
            spread = tick_size * (1 + i * self.layer_spread)
            p = base_price - spread if side == 'BUY' else base_price + spread
            prices.append(p)
        return prices

    def _should_update_grid(self, symbol: str, target_prices: List[float]) -> bool:
        current_orders = self.active_orders.get(symbol, {})
        if not current_orders: return True
        if len(current_orders) != len(target_prices): return True

        current_prices = sorted(list(current_orders.values()), reverse=True)
        target_sorted = sorted(target_prices, reverse=True)

        for p_old, p_new in zip(current_prices, target_sorted):
            if p_new == 0: continue
            if abs(p_old - p_new) / p_new > self.requote_threshold:
                return True
        return False

    async def _cancel_local_orders(self, symbol):
        """
        修复 Bug: 只有在撤单请求发出后，才安全地管理 active_orders。
        """
        orders = self.active_orders.get(symbol, {})
        if not orders: return

        order_ids = list(orders.keys())
        tasks = [self.adapters['GRVT'].cancel_order(oid) for oid in order_ids]

        await asyncio.gather(*tasks, return_exceptions=True)

        # 假设撤单指令发出后，订单即失效。
        # (更严格的做法是等待 'order cancelled' 事件，但这里做简化处理以保证挂单效率)
        self.active_orders[symbol] = {}

    async def _place_new_orders(self, symbol, side, prices):
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))
        tasks = []
        adapter = self.adapters['GRVT']

        for p in prices:
            tasks.append(adapter.create_order(f"{symbol}-USDT", side, quantity, p))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        new_active = {}
        for res, p in zip(results, prices):
            if isinstance(res, str) and res:
                new_active[res] = p
            else:
                pass

        self.active_orders[symbol] = new_active
        if new_active:
            logger.info(f"⛓️ [Requote] {symbol} x{len(new_active)} Top: {prices[0]:.2f}")

    async def _process_trade(self, trade: dict):
        if trade['exchange'] != 'GRVT': return
        symbol = trade['symbol']
        size = float(trade['size'])
        side = trade['side']

        change = size if side == 'BUY' else -size
        async with self.inventory_lock:
            self.current_inventory[symbol] = self.current_inventory.get(symbol, 0.0) + change
            new_inv = self.current_inventory[symbol]

        logger.info(f"⚡️ [Fill] {symbol} {side} {size} | New Inv: {new_inv:.4f}")

        asyncio.create_task(self._check_hedge_trigger(symbol, new_inv))

    async def _check_hedge_trigger(self, symbol, inv):
        grvt_tick = self.tickers.get(symbol, {}).get('GRVT')
        if not grvt_tick: return

        value = inv * grvt_tick.get('bid', 0)
        if abs(value) > self.max_inventory_usd * 1.1:
            logger.warning(f"🔥 [Surge] Inventory {value:.2f} > Limit. Hedging!")
            await self._execute_batch_hedge(symbol)

    async def _execute_batch_hedge(self, symbol):
        """
        紧急对冲逻辑：
        1. 使用 action_lock 暂停挂单
        2. 撤销所有本地挂单
        3. 在 Lighter 市场吃单平仓
        """
        lock = self._get_action_lock(symbol)

        if lock.locked():
            logger.warning(f"⚠️ [Hedge] Locked. Retrying later.")
            return

        async with lock:
            await self._cancel_local_orders(symbol)

            async with self.inventory_lock:
                pos = self.current_inventory.get(symbol, 0.0)

            if abs(pos) < 0.0001: return

            hedge_side = 'SELL' if pos > 0 else 'BUY'
            hedge_size = abs(pos)

            logger.info(f"🌊 [Hedge Start] Target: Lighter {hedge_side} {hedge_size}")

            try:
                if 'Lighter' not in self.tickers[symbol]:
                    raise Exception("Lighter Tick Missing")

                lighter_tick = self.tickers[symbol]['Lighter']

                base_price = lighter_tick['bid'] if hedge_side == 'SELL' else lighter_tick['ask']
                if base_price <= 0:
                    raise Exception("Lighter Price Invalid (0)")

                exec_price = base_price * 0.95 if hedge_side == 'SELL' else base_price * 1.05

                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol, side=hedge_side, amount=hedge_size, price=exec_price, order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ [Hedge Done] Lighter ID: {order_id}")
                    async with self.inventory_lock:
                        self.current_inventory[symbol] = 0.0
                else:
                    raise Exception("Lighter OrderID is None")

            except Exception as e:
                logger.error(f"❌ Hedge Critical Fail: {e}")
                self.hedge_cooldowns[symbol] = time.time() + 10.0
                logger.warning(f"⏳ {symbol} Enter 10s cooldown")

            await asyncio.sleep(1.0)