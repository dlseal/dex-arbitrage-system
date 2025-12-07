import asyncio
import logging
import time
from typing import Dict, Any, Optional
from app.config import Config

logger = logging.getLogger("GL_Farm_Opt")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_SmartFarm_v9_Optimized"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 订单管理
        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}
        self.order_create_time: Dict[str, float] = {}

        # 并发控制与熔断状态
        self.pending_orders: set = set()  # 正在挂单中的币种（防止重复挂单）
        self.faulty_symbols: set = set()  # 已熔断的币种（禁止交易）

        # 状态管理
        self.locks: Dict[str, asyncio.Lock] = {}
        self.last_quote_time: Dict[str, float] = {}

        # 方向管理
        self.symbol_sides: Dict[str, str] = {}
        self.initial_side = Config.FARM_SIDE.upper()

        # 配置参数
        self.MAX_SKEW_USD = 2000.0
        self.REQUIRED_DEPTH_RATIO = 1.5

        logger.info(f"🛡️ SmartFarm 策略启动 (Optimized) | 初始方向: {self.initial_side}")

    def _get_lock(self, symbol: str):
        if symbol not in self.locks:
            self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    def _get_current_side(self, symbol: str) -> str:
        return self.symbol_sides.get(symbol, self.initial_side)

    def _flip_side(self, symbol: str):
        current = self._get_current_side(symbol)
        new_side = 'SELL' if current == 'BUY' else 'BUY'
        self.symbol_sides[symbol] = new_side
        logger.info(f"🔄 [方向翻转] {symbol}: {current} -> {new_side}")

    async def on_tick(self, event: dict):
        event_type = event.get('type', 'tick')

        if event_type == 'trade':
            # 必须等待处理完 Trade 才能继续
            await self._process_trade_fill(event)
        elif event_type == 'tick':
            await self._process_tick(event)

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        # 如果正在对冲，暂停挂单逻辑
        lock = self._get_lock(symbol)
        if lock.locked():
            return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            await self._manage_maker_orders(symbol)

    async def _manage_maker_orders(self, symbol: str):
        # ✅ 优化：熔断检查
        if symbol in self.faulty_symbols:
            return

        # ✅ 优化：并发挂单锁检查（防止同一 Tick 重复触发挂单）
        if symbol in self.pending_orders:
            return

        now = time.time()
        # 频率限制 (500ms)
        if now - self.last_quote_time.get(symbol, 0) < 0.5: return
        self.last_quote_time[symbol] = now

        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        maker_side = self._get_current_side(symbol)

        # 计算安全价格
        target_price = self._calculate_safe_maker_price(symbol, grvt_tick, lighter_tick, maker_side)
        if not target_price: return

        current_order_id = self.active_orders.get(symbol)
        current_price = self.active_order_prices.get(symbol)
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # 1. 挂新单
        if not current_order_id:
            logger.info(f"🆕 [挂单] {symbol} {maker_side} {quantity} @ {target_price}")
            self.order_create_time[symbol] = time.time()

            # ✅ 优化：上锁，防止下个 Tick 重复进入
            self.pending_orders.add(symbol)
            asyncio.create_task(self._place_order_task(symbol, maker_side, quantity, target_price))

        # 2. 改单检查 (智能逻辑)
        else:
            order_age = time.time() - self.order_create_time.get(symbol, 0)
            price_diff_pct = abs(target_price - current_price) / current_price if current_price else 0

            # 如果价差巨大(>0.5%)，立即改单；否则等待 1 秒，减少被套利风险
            is_emergency = price_diff_pct > 0.005
            if is_emergency or order_age > 1.0:
                if price_diff_pct > Config.REQUOTE_THRESHOLD:
                    asyncio.create_task(self._cancel_order_task(symbol, current_order_id))
    async def _place_order_task(self, symbol, side, qty, price):
        try:
            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT", side=side, amount=qty, price=price
            )
            if new_id and new_id != "0x00":
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = price
        finally:
            # ✅ 优化：无论成功失败，必须释放挂单锁
            if symbol in self.pending_orders:
                self.pending_orders.remove(symbol)

    async def _cancel_order_task(self, symbol, order_id):
        try:
            await self.adapters['GRVT'].cancel_order(order_id)
        except Exception:
            pass
        # 乐观更新：假设撤单成功，清理本地状态以便下次 Tick 重新下单
        if symbol in self.active_orders and self.active_orders[symbol] == order_id:
            del self.active_orders[symbol]
            del self.active_order_prices[symbol]

    def _calculate_safe_maker_price(self, symbol: str, grvt_tick: dict, lighter_tick: dict, side: str) -> Optional[
        float]:
        """
        计算 Maker 价格，核心加入深度检查
        """
        adapter = self.adapters['GRVT']
        contract_info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(contract_info['tick_size']) if contract_info else 0.01

        quantity = Config.TRADE_QUANTITIES.get(symbol, 0.0001)
        required_hedge_qty = quantity * self.REQUIRED_DEPTH_RATIO

        # 从 Lighter 获取加权平均价 (Weighted Average Price)
        # 注意：需要 Lighter Adapter 提供 bids/asks 列表 (Top 5) 而不是仅仅 bid/ask 价格
        # 这里假设 lighter_tick 包含了 'bids_depth' 和 'asks_depth' (需修改 Adapter)
        hedge_price = self._get_depth_weighted_price(lighter_tick, 'SELL' if side == 'BUY' else 'BUY',
                                                     required_hedge_qty)

        if not hedge_price:
            return None  # 深度不足，不报价

        if side == 'BUY':
            raw_target = grvt_tick['ask'] - tick_size
            # 预期 PnL = (Lighter卖出均价 - GRVT买入价) / GRVT买入价
            expected_pnl = (hedge_price - raw_target) / raw_target
        else:
            raw_target = grvt_tick['bid'] + tick_size
            expected_pnl = (raw_target - hedge_price) / hedge_price

        if expected_pnl < Config.MAX_SLIPPAGE_TOLERANCE:
            return None

        return raw_target

    def _get_depth_weighted_price(self, ticker, side, required_qty):
        """计算吃掉 required_qty 所需的加权价格"""
        # 如果 Adapter 没传深度，回退到 Best Price (不安全模式)
        depth = ticker.get('asks_depth' if side == 'BUY' else 'bids_depth')
        if not depth:
            return ticker.get('ask' if side == 'BUY' else 'bid')

        collected = 0.0
        cost = 0.0

        # depth 格式: [[price, size], [price, size]...]
        for p, s in depth:
            take = min(s, required_qty - collected)
            cost += take * p
            collected += take
            if collected >= required_qty:
                break

        if collected < required_qty * 0.5:  # 深度连一半都不到，极其危险
            return None

        return cost / collected

    async def _process_trade_fill(self, trade: dict):
        """
        处理成交回报 (入口)
        ✅ 优化：完全异步化，将耗时的对冲逻辑扔到后台任务
        """
        if trade['exchange'] != 'GRVT': return

        symbol = trade['symbol']
        # 🚀 启动后台任务，主线程立即释放
        asyncio.create_task(self._background_hedge_task(symbol, trade))

    async def _background_hedge_task(self, symbol: str, trade: dict):
        """
        [新增] 后台对冲任务
        """
        lock = self._get_lock(symbol)
        order_id = trade.get('order_id')

        # 在后台任务中竞争锁，确保对冲有序
        async with lock:
            logger.info(f"🚨 [成交触发] GRVT {trade['side']} {trade['size']} -> 执行后台对冲")

            # 1. 清理本地挂单状态
            if symbol in self.active_orders:
                if not order_id or str(self.active_orders[symbol]) == str(order_id):
                    del self.active_orders[symbol]
                    if symbol in self.active_order_prices:
                        del self.active_order_prices[symbol]

            # 2. 如果是部分成交，立即撤销剩余订单
            if order_id:
                asyncio.create_task(self._safe_cancel(symbol, order_id))

            # 3. 执行对冲
            await self._execute_hedge_loop(symbol, trade['side'], trade['size'])

    async def _execute_hedge_loop(self, symbol, grvt_side, size):
        hedge_side = 'SELL' if grvt_side.upper() == 'BUY' else 'BUY'
        retry = 0

        while retry < 5:
            try:
                # 获取最新的深度价格
                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                if not lighter_tick:
                    await asyncio.sleep(0.1)
                    continue

                # 市价单预估价 (aggressive)
                base_price = lighter_tick['ask'] if hedge_side == 'BUY' else lighter_tick['bid']
                exec_price = base_price * 1.05 if hedge_side == 'BUY' else base_price * 0.95

                logger.info(f"🌊 [Lighter对冲] {hedge_side} {size} @ {exec_price:.2f} (Try {retry + 1})")
                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol, side=hedge_side, amount=size, price=exec_price, order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ 对冲成功 ID: {order_id}")
                    self._flip_side(symbol)
                    return  # 成功退出
            except Exception as e:
                logger.error(f"❌ 对冲失败: {e}")

            retry += 1
            await asyncio.sleep(0.5)

        logger.critical(f"💀💀💀 {symbol} 对冲彻底失败，触发熔断！停止该币种交易。")
        self.faulty_symbols.add(symbol)