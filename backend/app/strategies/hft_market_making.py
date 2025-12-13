import asyncio
import logging
import time
import math
from collections import deque
from typing import Dict, Any, Optional

from app.config import Config

logger = logging.getLogger("HFT_AS_OFI")


class OnlineStats:
    """Welford算法实现的流式方差计算器，O(1) 复杂度，内存占用极小"""

    def __init__(self, window_size=100):
        self.window_size = window_size
        self.values = deque(maxlen=window_size)
        self.sum = 0.0
        self.sq_sum = 0.0

    def update(self, value: float):
        if len(self.values) == self.window_size:
            old_val = self.values.popleft()
            self.sum -= old_val
            self.sq_sum -= old_val * old_val

        self.values.append(value)
        self.sum += value
        self.sq_sum += value * value

    def get_std_dev(self) -> float:
        n = len(self.values)
        if n < 2: return 0.0
        mean = self.sum / n
        variance = (self.sq_sum / n) - (mean * mean)
        return math.sqrt(max(0.0, variance))


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AS_OFI_Pro_v1"
        self.adapters = adapters

        # 1. 解析目标交易所
        self.exchange_name = Config.HFT_EXCHANGE

        # 2. 动态解析交易对和数量 (按用户要求)
        if not Config.TARGET_SYMBOLS:
            logger.error("❌ 未配置 TARGET_SYMBOLS，无法启动策略")
            self.is_active = False
            return

        self.symbol = Config.TARGET_SYMBOLS[0]  # 取第一个
        self.quantity = Config.TRADE_QUANTITIES.get(
            self.symbol,
            Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001)
        )

        logger.info(f"🎯 策略目标: {self.exchange_name} | {self.symbol} | Qty: {self.quantity}")

        # AS 模型参数
        self.risk_aversion = Config.HFT_RISK_AVERSION
        self.ofi_sensitivity = Config.HFT_OFI_SENSITIVITY
        self.min_spread_ticks = Config.HFT_MIN_SPREAD_TICKS
        self.update_threshold_ticks = Config.HFT_UPDATE_THRESHOLD_TICKS
        self.max_pos_usd = Config.HFT_MAX_POS_USD

        # 内部状态
        self.tick_size = 0.01  # 稍后从 Adapter 获取
        self.inventory = 0.0
        self.mid_price_stats = OnlineStats(window_size=Config.HFT_WINDOW_SIZE)
        self.ofi_window = deque(maxlen=50)  # 短期 OFI 均值窗口

        self.prev_tick: Optional[Dict] = None

        # 订单状态 (Side -> OrderID)
        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}

        self.lock = asyncio.Lock()
        self.is_active = True
        self.pos_sync_time = 0

        self._validate_adapter()

    def _validate_adapter(self):
        if self.exchange_name not in self.adapters:
            logger.error(f"❌ Adapter {self.exchange_name} 未加载！")
            self.is_active = False

    async def on_tick(self, event: dict):
        if not self.is_active: return

        # 路由事件
        if event.get('exchange') != self.exchange_name: return

        evt_type = event.get('type')

        # 1. 成交事件 -> 更新持仓
        if evt_type == 'trade':
            await self._on_trade(event)
            return

        # 2. 报价事件 -> 计算信号 & 调整挂单
        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            # Drop mechanism: 如果上一帧计算还没完，丢弃当前帧，防止延迟堆积
            if self.lock.locked(): return

            async with self.lock:
                await self._process_tick(event)

    async def _process_tick(self, tick: dict):
        current_ts = time.time()
        tick_ts = tick.get('ts', 0) / 1000.0

        # --- A. 延迟熔断 (Latency Guard) ---
        latency = current_ts - tick_ts
        if latency > 0.5:  # 500ms 阈值
            if latency > 2.0: logger.warning(f"⚠️ [High Latency] {latency * 1000:.0f}ms. Skipping.")
            return

        # 初始化 Tick Size
        if self.tick_size == 0.01:
            await self._update_contract_info()

        # 提取数据
        bid_p = tick['bid']
        ask_p = tick['ask']
        # 尝试获取 Volume，如果没有则默认为 1 (降级为 Price-OFI)
        bid_v = tick.get('bid_volume', 1.0)
        ask_v = tick.get('ask_volume', 1.0)

        mid_price = (bid_p + ask_p) / 2.0
        self.mid_price_stats.update(mid_price)

        # --- B. 计算 OFI (Order Flow Imbalance) ---
        ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
        self.ofi_window.append(ofi)
        avg_ofi = sum(self.ofi_window) / max(1, len(self.ofi_window))

        # --- C. Avellaneda-Stoikov 计算 ---
        volatility = self.mid_price_stats.get_std_dev()
        # 防止波动率为 0 导致 Spread 过小
        if volatility <= 0: volatility = mid_price * 0.0001

        # 1. 保留价格 (Reservation Price)
        # r = s + (alpha * OFI) - (q * gamma * sigma^2)
        inv_risk = self.inventory * self.risk_aversion * (volatility ** 2)
        reservation_price = mid_price + (self.ofi_sensitivity * avg_ofi * self.tick_size) - inv_risk

        # 2. 报价 Spread
        # spread = min_spread + (beta * sigma)
        half_spread = (self.min_spread_ticks * self.tick_size) + (Config.HFT_VOLATILITY_FACTOR * volatility)

        target_bid = reservation_price - half_spread
        target_ask = reservation_price + half_spread

        # --- D. 风控修剪 ---
        # 1. 价格防倒挂
        target_bid = min(target_bid, ask_p - self.tick_size)
        target_ask = max(target_ask, bid_p + self.tick_size)

        # 2. 价格对齐 Tick
        target_bid = round(target_bid / self.tick_size) * self.tick_size
        target_ask = round(target_ask / self.tick_size) * self.tick_size

        # 3. 最大持仓保护
        pos_value = self.inventory * mid_price
        allow_buy = pos_value < self.max_pos_usd
        allow_sell = pos_value > -self.max_pos_usd

        # --- E. 执行逻辑 ---
        await self._execute_quotes(target_bid, target_ask, allow_buy, allow_sell)

        # 定期同步准确持仓 (10s)
        if current_ts - self.pos_sync_time > 10.0:
            asyncio.create_task(self._sync_position())
            self.pos_sync_time = current_ts

    def _calculate_ofi(self, bid_p, bid_v, ask_p, ask_v) -> float:
        """计算 OFI 值"""
        if not self.prev_tick:
            self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bid_v': bid_v, 'ask_v': ask_v}
            return 0.0

        prev_bid_p = self.prev_tick['bid']
        prev_bid_v = self.prev_tick.get('bid_v', 1.0)
        prev_ask_p = self.prev_tick['ask']
        prev_ask_v = self.prev_tick.get('ask_v', 1.0)

        # Bid Side Change
        if bid_p > prev_bid_p:
            e_n_bid = bid_v
        elif bid_p < prev_bid_p:
            e_n_bid = -prev_bid_v
        else:
            e_n_bid = bid_v - prev_bid_v

        # Ask Side Change
        if ask_p > prev_ask_p:
            e_n_ask = -prev_ask_v
        elif ask_p < prev_ask_p:
            e_n_ask = ask_v
        else:
            e_n_ask = prev_ask_v - ask_v  # 注意 Ask 增加表示阻力，对 OFI 是负贡献

        self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bid_v': bid_v, 'ask_v': ask_v}

        # OFI = Bid流 - Ask流
        return e_n_bid - e_n_ask

    async def _execute_quotes(self, target_bid, target_ask, allow_buy, allow_sell):
        tasks = []

        # 买单逻辑
        if allow_buy:
            tasks.append(self._manage_order('BUY', target_bid))
        elif self.active_orders['BUY']:
            # 超过持仓限制，撤买单
            tasks.append(self._cancel_order('BUY'))

        # 卖单逻辑
        if allow_sell:
            tasks.append(self._manage_order('SELL', target_ask))
        elif self.active_orders['SELL']:
            tasks.append(self._cancel_order('SELL'))

        if tasks:
            await asyncio.gather(*tasks)

    async def _manage_order(self, side, target_price):
        current_id = self.active_orders[side]
        current_price = self.active_prices[side]

        # 1. 检查是否需要更新 (Threshold Check)
        if current_id:
            diff_ticks = abs(target_price - current_price) / self.tick_size
            if diff_ticks <= self.update_threshold_ticks:
                return  # 变动太小，忽略

        # 2. 执行更新 (Cancel + Replace)
        # 注意：这里先发撤单不等待，直接发新单，提高速度。
        # 风险：可能导致瞬间双重挂单，但对于 HFT 来说，速度优先，且有 MaxPos 兜底。
        if current_id:
            asyncio.create_task(self.adapters[self.exchange_name].cancel_order(current_id, symbol=self.symbol))

        try:
            # 强制 Post-Only
            new_id = await self.adapters[self.exchange_name].create_order(
                symbol=self.symbol,
                side=side,
                amount=self.quantity,
                price=target_price,
                params={"post_only": True}
            )

            if new_id:
                self.active_orders[side] = new_id
                self.active_prices[side] = target_price
            else:
                self.active_orders[side] = None

        except Exception as e:
            logger.error(f"Quote Failed {side}: {e}")
            self.active_orders[side] = None

    async def _cancel_order(self, side):
        oid = self.active_orders[side]
        if oid:
            await self.adapters[self.exchange_name].cancel_order(oid, symbol=self.symbol)
            self.active_orders[side] = None
            self.active_prices[side] = 0.0

    async def _on_trade(self, trade):
        if trade['symbol'] != self.symbol: return

        size = float(trade['size'])
        if trade['side'].upper() == 'BUY':
            self.inventory += size
        else:
            self.inventory -= size

        logger.info(f"⚡️ Fill {trade['side']} {size} | Inv: {self.inventory:.4f}")

    async def _sync_position(self):
        """API 强一致性同步"""
        try:
            adapter = self.adapters[self.exchange_name]
            # 兼容 GRVT SDK 的获取持仓逻辑
            if hasattr(adapter, 'rest_client'):
                loop = asyncio.get_running_loop()
                positions = await loop.run_in_executor(
                    None,
                    lambda: adapter.rest_client.fetch_positions(
                        params={'sub_account_id': adapter.trading_account_id}
                    )
                )
                for p in positions:
                    # GRVT symbol 通常是 "BTC-USDT-Perpetual"
                    if self.symbol in (p.get('instrument') or ""):
                        size = float(p.get('size', 0) or p.get('contracts', 0))
                        # 修正方向
                        if size > 0 and p.get('side', '').upper() == 'SHORT':
                            size = -size
                        self.inventory = size
                        break
        except Exception as e:
            logger.error(f"Sync Pos Error: {e}")

    async def _update_contract_info(self):
        adapter = self.adapters[self.exchange_name]
        # 尝试从 Adapter 缓存读取
        if hasattr(adapter, 'contract_map'):
            # 兼容不同 Adapter 的 Key 格式 (BTC vs BTC-USDT)
            key = f"{self.symbol}-USDT"
            if key not in adapter.contract_map:
                key = self.symbol

            info = adapter.contract_map.get(key)
            if info:
                self.tick_size = float(info.get('tick_size', 0.01))
                logger.info(f"📏 Tick Size Updated: {self.tick_size}")