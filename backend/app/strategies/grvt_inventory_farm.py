import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from app.config import Config

logger = logging.getLogger("InventoryFarm")


class GrvtInventoryFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "Grvt_Inventory_Grid_v1"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # --- 从 Config 读取配置 ---
        self.max_inventory_usd = Config.MAX_INVENTORY_USD
        self.layers = Config.INVENTORY_LAYERS
        self.layer_spread = Config.INVENTORY_LAYER_SPREAD

        # 运行时状态
        self.current_inventory: Dict[str, float] = {}  # 当前持仓数量
        self.active_orders: Dict[str, Dict[str, float]] = {}  # {symbol: {order_id: price}}
        self.locks: Dict[str, asyncio.Lock] = {}
        self.last_quote_time: Dict[str, float] = {}

        # 安全状态
        self.is_ready = False

        logger.info(f"🚜 InventoryFarm 启动 | 目标持仓上限: ${self.max_inventory_usd} | 层数: {self.layers}")
        logger.info(f"   - 监听币种: {Config.TARGET_SYMBOLS}")

        # 启动持仓同步守护任务
        asyncio.create_task(self._sync_position_loop())

    def _get_lock(self, symbol: str):
        if symbol not in self.locks: self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    async def _sync_position_loop(self):
        """定期从交易所获取真实持仓，修正本地状态"""
        while True:
            try:
                # 首次启动等待 5 秒，后续每 15 秒同步一次
                await asyncio.sleep(15 if self.is_ready else 5)

                adapter = self.adapters.get('GRVT')
                if adapter and adapter.rest_client:
                    # 调用 SDK 获取持仓
                    positions = await asyncio.get_running_loop().run_in_executor(
                        None,
                        lambda: adapter.rest_client.fetch_positions(
                            params={'sub_account_id': adapter.trading_account_id})
                    )

                    for symbol in Config.TARGET_SYMBOLS:
                        real_pos = 0.0
                        # 解析返回的持仓数据
                        for p in positions:
                            # 兼容 symbol 格式 (如 BTC-USDT-Perpetual)
                            if symbol in p.get('symbol', ''):
                                real_pos = float(p.get('contracts', 0) or p.get('size', 0))
                                break

                        # 差异较大时修正本地数据
                        local_pos = self.current_inventory.get(symbol, 0.0)
                        if abs(real_pos - local_pos) > 0.0001:
                            logger.warning(f"⚠️ [持仓校准] {symbol} 本地:{local_pos} -> 真实:{real_pos}")
                            self.current_inventory[symbol] = real_pos

                if not self.is_ready:
                    self.is_ready = True
                    logger.info("✅ 初始持仓同步完成，策略开始挂单")

            except Exception as e:
                logger.error(f"❌ 持仓同步失败: {e}")

    async def on_tick(self, event: dict):
        # 未完成初始同步前，不执行任何操作
        if not self.is_ready:
            return

        if event.get('symbol') not in Config.TARGET_SYMBOLS:
            return

        if event.get('type') == 'trade':
            await self._process_trade(event)
        elif event.get('type') == 'tick':
            await self._process_tick(event)

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
                logger.info(f"🌕 [满仓] {symbol} 持仓 ${pos_value:.2f} (>{self.max_inventory_usd}) -> 触发批量对冲")
                asyncio.create_task(self._execute_batch_hedge(symbol))
            return

        await self._place_layered_orders(symbol, target_side, grvt_tick, lighter_tick)

    async def _place_layered_orders(self, symbol, side, grvt_tick, lighter_tick):
        adapter = self.adapters['GRVT']
        info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(info['tick_size']) if info else 0.01

        base_price = grvt_tick['ask'] if side == 'BUY' else grvt_tick['bid']
        if base_price <= 0: return

        target_prices = []
        for i in range(self.layers):
            spread_ticks = 1 + i * self.layer_spread
            if side == 'BUY':
                p = base_price - (tick_size * spread_ticks)
            else:
                p = base_price + (tick_size * spread_ticks)
            target_prices.append(p)

        hedge_price = lighter_tick['bid'] if side == 'BUY' else lighter_tick['ask']
        if hedge_price <= 0: return

        if side == 'BUY':
            est_pnl = (hedge_price - target_prices[0]) / target_prices[0]
        else:
            est_pnl = (target_prices[0] - hedge_price) / hedge_price

        if est_pnl < Config.MAX_SLIPPAGE_TOLERANCE:
            if self.active_orders.get(symbol):
                await self._cancel_all(symbol)
            return

        if self.active_orders.get(symbol):
            await self._cancel_all(symbol)

        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        tasks = []
        for p in target_prices:
            tasks.append(adapter.create_order(
                symbol=f"{symbol}-USDT", side=side, amount=quantity, price=p
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        if symbol not in self.active_orders: self.active_orders[symbol] = {}
        for res, p in zip(results, target_prices):
            if isinstance(res, str) and res:
                self.active_orders[symbol][res] = p

    async def _cancel_all(self, symbol):
        orders = self.active_orders.get(symbol, {})
        if not orders: return

        tasks = [self.adapters['GRVT'].cancel_order(oid) for oid in orders.keys()]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.active_orders[symbol] = {}

    async def _process_trade(self, trade: dict):
        if trade['exchange'] != 'GRVT': return

        symbol = trade['symbol']
        side = trade['side']
        size = trade['size']

        change = size if side == 'BUY' else -size
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
        async with lock:
            await self._cancel_all(symbol)

            # 使用当前内存中的持仓 (会被 _sync_position_loop 定期校准)
            pos = self.current_inventory.get(symbol, 0.0)

            # 过滤微小尘埃仓位，避免触发无效对冲
            if abs(pos) < 0.001:
                return

            hedge_side = 'SELL' if pos > 0 else 'BUY'
            hedge_size = abs(pos)

            logger.info(f"🌊 [开始对冲] 目标: Lighter {hedge_side} {hedge_size}")

            try:
                lighter_tick = self.tickers[symbol]['Lighter']
                base_price = lighter_tick['bid'] if hedge_side == 'SELL' else lighter_tick['ask']
                exec_price = base_price * 0.95 if hedge_side == 'SELL' else base_price * 1.05

                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol, side=hedge_side, amount=hedge_size, price=exec_price, order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ [对冲完成] 库存清零")
                    self.current_inventory[symbol] = 0.0
                else:
                    logger.error(f"❌ 对冲下单失败！")

            except Exception as e:
                logger.error(f"❌ 对冲异常: {e}")

            await asyncio.sleep(2.0)