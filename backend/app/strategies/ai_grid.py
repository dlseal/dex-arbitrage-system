# backend/app/strategies/ai_grid.py
import asyncio
import logging
import time
import math
import pandas as pd
import numpy as np
from collections import deque
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP
from typing import Dict, Any, List, Optional, Tuple

from app.config import settings
from app.utils.llm_client import fetch_grid_advice

logger = logging.getLogger("AI_Grid_Pro")


class MarketDataAggregator:
    """
    轻量级市场数据聚合器：负责合成 K 线并计算核心指标
    """

    def __init__(self, window_size=100):
        # 存储 15m K线: [ts, open, high, low, close, volume]
        self.candles = deque(maxlen=window_size)
        self.current_candle = None
        self.last_candle_ts = 0
        self.candle_interval = 15 * 60  # 15分钟

    def on_tick(self, price: float, volume: float, ts: float):
        # 1. 简单的 K 线合成逻辑
        # 将时间戳对齐到 15m 网格
        candle_ts = int(ts // self.candle_interval) * self.candle_interval

        if self.current_candle is None:
            self.current_candle = {
                'ts': candle_ts, 'open': price, 'high': price,
                'low': price, 'close': price, 'vol': volume
            }

        # 如果进入了新的时间窗口，归档旧 K 线
        if candle_ts > self.current_candle['ts']:
            self.candles.append(self.current_candle.copy())
            self.current_candle = {
                'ts': candle_ts, 'open': price, 'high': price,
                'low': price, 'close': price, 'vol': volume
            }
        else:
            # 更新当前 K 线
            c = self.current_candle
            c['high'] = max(c['high'], price)
            c['low'] = min(c['low'], price)
            c['close'] = price
            c['vol'] += volume


    def get_technical_indicators(self) -> Dict[str, Any]:
        """使用 Pandas 计算 RSI, ATR, Bollinger Bands"""
        if len(self.candles) < 20:
            return {}

        df = pd.DataFrame(list(self.candles))

        # --- 1. RSI (14) ---
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # --- 2. ATR (14) ---
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        df['atr'] = true_range.rolling(window=14).mean()

        # --- 3. Bollinger Bands (20) ---
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['std20'] = df['close'].rolling(window=20).std()
        df['upper_bb'] = df['ma20'] + (df['std20'] * 2)
        df['lower_bb'] = df['ma20'] - (df['std20'] * 2)

        latest = df.iloc[-1]

        # 计算布林带位置 % (0=下轨, 100=上轨, >100=突破上轨)
        bb_range = latest['upper_bb'] - latest['lower_bb']
        bb_pct = 50.0
        if bb_range > 0:
            bb_pct = ((latest['close'] - latest['lower_bb']) / bb_range) * 100

        # 判断短期趋势 (比较当前价格与 MA20)
        trend = "SIDEWAYS"
        if latest['close'] > latest['ma20'] * 1.005:
            trend = "UPWARD"
        elif latest['close'] < latest['ma20'] * 0.995:
            trend = "DOWNWARD"

        return {
            "rsi": round(latest['rsi'], 1) if not pd.isna(latest['rsi']) else 50.0,
            "atr": round(latest['atr'], 2) if not pd.isna(latest['atr']) else 0.0,
            "bb_pct": round(bb_pct, 1),
            "trend_1h": trend
        }

    def get_recent_candles_str(self, limit=5) -> str:
        """格式化最近 K 线供 LLM 阅读"""
        if not self.candles: return "No data"
        res = []
        # 取最近 N 根
        recent = list(self.candles)[-limit:]
        for c in recent:
            t_str = time.strftime("%H:%M", time.localtime(c['ts']))
            res.append(f"[{t_str}] O:{c['open']} H:{c['high']} L:{c['low']} C:{c['close']}")
        return "\n".join(res)


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AI_Grid_Pro_v4_DataEnhanced"
        self.adapters = adapters

        # --- 配置加载 ---
        self.conf = settings.strategies.ai_grid
        self.exchange_name = self.conf.exchange

        if not settings.common.target_symbols:
            logger.critical("❌ [Init] Target symbols not configured.")
            self.is_active = False
            return

        self.symbol = settings.common.target_symbols[0]

        # --- 核心参数 ---
        self.upper_price = 0.0
        self.lower_price = 0.0
        self.grid_count = 0
        self.grid_mode = getattr(self.conf, 'grid_mode', 'GEOMETRIC').upper()

        self.principal = self.conf.principal
        self.leverage = self.conf.leverage
        self.min_order_notional = self.conf.min_order_size

        # --- 数据增强模块 ---
        self.aggregator = MarketDataAggregator(window_size=200)
        self.last_check_price = 0.0
        # 波动率触发阈值：如果价格相比上次 AI 检查时偏离超过 3%，强制触发更新
        self.volatility_trigger_pct = 0.03

        # 盘口数据缓存
        self.last_book_imbalance = 0.0  # >0 买盘强, <0 卖盘强
        self.bid_vol_cache = 0.0
        self.ask_vol_cache = 0.0

        # --- 状态管理 ---
        self.active_orders: Dict[str, Dict] = {}
        self.grid_levels: List[float] = []
        self.quantity_per_grid = 0.0

        self.next_check_ts = 0.0
        self.max_check_interval = self.conf.max_check_interval

        # --- 并发控制 ---
        self.state_lock = asyncio.Lock()
        self.is_updating = False
        self.is_active = True
        self.initialized = False
        self.emergency_stopped = False

        self.tick_size = Decimal("0.01")
        self.min_qty_size = Decimal("0.0001")

        self._validate_adapters()
        asyncio.create_task(self._bootstrap_strategy())

    def _validate_adapters(self):
        if self.exchange_name not in self.adapters:
            logger.critical(f"❌ [Init] Exchange {self.exchange_name} not loaded.")
            self.is_active = False
        else:
            logger.info(f"✅ [Init] Strategy Loaded: {self.exchange_name}")

    async def _bootstrap_strategy(self):
        await asyncio.sleep(2.0)
        await self._init_market_info()

        logger.info("🧹 [Bootstrap] Cleaning up existing orders...")
        await self._cancel_all_orders_force()

        self.initialized = True
        self.next_check_ts = time.time() - 1.0
        logger.info("🚀 [Bootstrap] Ready. Waiting for AI initialization...")

    async def _init_market_info(self):
        retry = 0
        while retry < 5:
            try:
                adapter = self.adapters[self.exchange_name]
                if hasattr(adapter, 'contract_map'):
                    # 模糊匹配 symbol (如 BTC 匹配 BTC-USDT 或 BTC-PERP)
                    targets = [k for k in adapter.contract_map.keys() if self.symbol in k]
                    if targets:
                        info = adapter.contract_map[targets[0]]
                        self.tick_size = Decimal(str(info.get('tick_size', '0.01')))
                        self.min_qty_size = Decimal(str(info.get('min_size', '0.0001')))
                        logger.info(f"📏 [Precision] Tick={self.tick_size}, MinSize={self.min_qty_size}")
                        return
            except Exception as e:
                logger.warning(f"⚠️ Market info fetch failed: {e}")
            retry += 1
            await asyncio.sleep(2)

    async def on_tick(self, event: dict):
        if not self.is_active or self.emergency_stopped or not self.initialized:
            return

        evt_type = event.get('type')

        # 1. 处理成交
        if evt_type == 'trade':
            await self._process_trade_fill(event)
            return

        # 2. 处理行情 (Tick)
        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            current_price = float(event.get('bid', 0) + event.get('ask', 0)) / 2
            if current_price <= 0: return

            # --- 数据喂养 ---
            # 估算成交量：如果是 Nado 这种 Snapshot 只有深度，没有 volume 字段，可以用 bid_volume 代替
            # 或者如果有 trade event 也可以在 trade 里 feed
            vol_est = 0.0
            if 'bid_volume' in event:
                vol_est = (event['bid_volume'] + event['ask_volume']) / 2
                self.bid_vol_cache = event['bid_volume']
                self.ask_vol_cache = event['ask_volume']
                total_vol = self.bid_vol_cache + self.ask_vol_cache
                if total_vol > 0:
                    # 计算盘口失衡: (买-卖)/(买+卖) -> 范围 -1 到 1
                    self.last_book_imbalance = (self.bid_vol_cache - self.ask_vol_cache) / total_vol * 100

            ts = event.get('ts', time.time() * 1000) / 1000.0
            self.aggregator.on_tick(current_price, vol_est, ts)

            # --- 熔断检查 ---
            if self.grid_count > 0:
                if await self._check_circuit_breaker(current_price):
                    return

            if not self.is_updating:
                now = time.time()

                # --- 触发逻辑优化 ---
                should_trigger = False
                trigger_reason = ""

                # A. 时间触发
                if now >= self.next_check_ts:
                    should_trigger = True
                    trigger_reason = "TIMER"

                # B. 波动率触发 (暴涨暴跌强制咨询 AI)
                elif self.last_check_price > 0:
                    pct_change = abs(current_price - self.last_check_price) / self.last_check_price
                    if pct_change > self.volatility_trigger_pct:
                        should_trigger = True
                        trigger_reason = f"VOLATILITY_SURGE ({pct_change * 100:.1f}%)"

                if should_trigger:
                    asyncio.create_task(self._perform_ai_consultation(current_price, trigger_reason))

    async def _check_circuit_breaker(self, current_price: float) -> bool:
        # 简化版熔断：只在极端情况下停止
        if self.lower_price > 0 and current_price < self.lower_price * (1 - self.stop_loss_pct):
            logger.critical(f"🚨 [CIRCUIT] Price {current_price} < StopLoss {self.lower_price}")
            await self._emergency_shutdown()
            return True
        return False

    async def _emergency_shutdown(self):
        self.emergency_stopped = True
        self.is_active = False
        await self._cancel_all_orders_force()
        logger.critical("🛑 Strategy HALTED due to risk control.")

    async def _process_trade_fill(self, trade: dict):
        if trade['exchange'] != self.exchange_name: return
        # 兼容不同适配器的 ID 格式
        order_id = str(trade.get('order_id') or trade.get('client_order_id', ''))

        # 先从内存移除
        filled_order = None
        async with self.state_lock:
            if order_id in self.active_orders:
                filled_order = self.active_orders.pop(order_id)

        if filled_order:
            logger.info(f"⚡️ [Filled] {filled_order['side']} @ {filled_order['price']}")
            # 成交后立即触发一次补单检查
            if not self.is_updating and self.grid_count > 0:
                asyncio.create_task(self._reconcile_orders(float(filled_order['price'])))

    async def _perform_ai_consultation(self, current_price: float, trigger_reason: str = "TIMER"):
        """咨询 AI，附带完整市场数据"""
        if self.is_updating: return
        self.is_updating = True

        try:
            logger.info(f"🧠 [AI] Consulting ({trigger_reason})... Price: {current_price:.2f}")

            # 1. 获取技术指标
            indicators = self.aggregator.get_technical_indicators()
            candles_str = self.aggregator.get_recent_candles_str(limit=5)

            # 2. 构建上下文
            status_str = "ACTIVE" if (self.grid_count > 0 and self.upper_price > 0) else "INACTIVE"

            # 将丰富的数据注入 context
            context = {
                # 基础状态
                "upper": self.upper_price,
                "lower": self.lower_price,
                "count": self.grid_count,
                "mode": self.grid_mode,
                "inventory": "UNKNOWN",  # TODO: 如果有仓位监控可加入

                # --- 核心：喂给 AI 的新数据 ---
                "atr": indicators.get('atr', 'N/A'),
                "rsi": indicators.get('rsi', 'N/A'),
                "bb_pct": indicators.get('bb_pct', 'N/A'),
                "trend_1h": indicators.get('trend_1h', 'SIDEWAYS'),
                "recent_candles": candles_str,
                "bid_vol": f"{self.bid_vol_cache:.2f}",
                "ask_vol": f"{self.ask_vol_cache:.2f}",
                "imbalance": f"{self.last_book_imbalance:.1f}"
            }

            # 3. 调用 LLM
            advice = await fetch_grid_advice(self.symbol, current_price, context, status_str)

            # 4. 执行 AI 建议
            if advice:
                logger.info(f"🤖 [AI Decision] {advice.get('action')} | Reason: {advice.get('reason')}")

            action = advice.get("action", "CONTINUE").upper() if advice else "CONTINUE"
            duration = float(advice.get("duration_hours", 4.0)) if advice else 4.0

            if action == "UPDATE" and advice:
                raw_count = int(advice.get('grid_count', self.grid_count))
                new_upper = float(advice.get('upper_price', self.upper_price))
                new_lower = float(advice.get('lower_price', self.lower_price))

                if new_upper > new_lower and raw_count > 1:
                    self.upper_price = new_upper
                    self.lower_price = new_lower
                    self.grid_count = raw_count
                    self.last_check_price = current_price  # 更新基准价格

                    logger.info(f"✨ [AI Update] {self.lower_price}-{self.upper_price} | Grids: {self.grid_count}")

                    # 重新计算网格并下单
                    if self._calculate_grid_params(current_price):
                        await self._reconcile_orders(current_price)

            # 5. 设置下次检查时间
            # 如果 AI 建议 4小时，我们仍然会在期间监控波动率触发
            self.next_check_ts = time.time() + min(duration * 3600, self.max_check_interval)
            logger.info(f"⏳ Next Timer Check in {duration}h")

        except Exception as e:
            logger.error(f"❌ [AI] Loop Error: {e}", exc_info=True)
            self.next_check_ts = time.time() + 300  # 出错后 5 分钟重试
        finally:
            self.is_updating = False

    def _calculate_grid_params(self, current_price: float) -> bool:
        # ... (保持原有逻辑不变，计算 grid_levels 和 quantity_per_grid) ...
        try:
            if self.grid_count < 2: return False

            # 资金计算
            total_equity = self.principal * self.leverage
            safe_min_notional = float(self.min_order_notional) * 1.1  # 留 10% 缓冲
            max_grids = int(total_equity / safe_min_notional)

            if self.grid_count > max_grids:
                logger.warning(f"⚠️ [Risk] Downgrading grids {self.grid_count} -> {max_grids} (MinSize Limit)")
                self.grid_count = max_grids

            if self.grid_count < 2: return False

            per_grid_equity = total_equity / self.grid_count
            raw_qty = per_grid_equity / current_price
            d_qty = Decimal(str(raw_qty))

            # 向下取整到最小交易单位
            self.quantity_per_grid = float(
                (d_qty / self.min_qty_size).to_integral_value(ROUND_FLOOR) * self.min_qty_size)

            if self.quantity_per_grid <= 0: return False

            self.grid_levels = []
            d_upper = Decimal(str(self.upper_price))
            d_lower = Decimal(str(self.lower_price))

            if self.grid_mode == 'GEOMETRIC':
                ratio = math.pow(float(d_upper / d_lower), 1 / (self.grid_count - 1))
                d_ratio = Decimal(str(ratio))
                for i in range(self.grid_count):
                    p = float(d_lower * (d_ratio ** i))
                    self.grid_levels.append(self._quantize_price(p))
            else:
                step = (d_upper - d_lower) / Decimal(self.grid_count - 1)
                for i in range(self.grid_count):
                    p = float(d_lower + (step * i))
                    self.grid_levels.append(self._quantize_price(p))

            return True

        except Exception as e:
            logger.error(f"❌ Grid Calc Error: {e}", exc_info=True)
            return False

    def _quantize_price(self, price: float) -> float:
        d_p = Decimal(str(price))
        return float((d_p / self.tick_size).to_integral_value(ROUND_HALF_UP) * self.tick_size)

    async def _reconcile_orders(self, current_price: float):
        if not self.grid_levels or self.quantity_per_grid <= 0: return

        # 1. 计算目标挂单
        target_orders = set()
        buffer = float(self.tick_size) * 2.0  # 稍微放宽 Buffer 防止反复跳动

        for level_price in self.grid_levels:
            # 只有远离当前价格的 Grid 才挂单，防止 Taker 费率损耗
            if level_price < (current_price - buffer):
                target_orders.add(('BUY', f"{level_price:.6f}"))
            elif level_price > (current_price + buffer):
                target_orders.add(('SELL', f"{level_price:.6f}"))

        # 2. 对比现有挂单
        async with self.state_lock:
            current_active_snapshot = list(self.active_orders.items())

        to_cancel_ids = []
        matched_target_sigs = set()

        for oid, info in current_active_snapshot:
            p_str = f"{info['price']:.6f}"
            sig = (info['side'], p_str)
            if sig in target_orders:
                matched_target_sigs.add(sig)
            else:
                to_cancel_ids.append(oid)

        to_place_specs = []
        for side, p_str in target_orders:
            if (side, p_str) not in matched_target_sigs:
                to_place_specs.append((side, float(p_str)))

        if not to_cancel_ids and not to_place_specs: return

        logger.info(f"🔄 [Reconcile] Cancel: {len(to_cancel_ids)} | Place: {len(to_place_specs)}")
        adapter = self.adapters[self.exchange_name]

        # 3. 执行撤单
        if to_cancel_ids:
            chunk_size = 5  # 降低并发防限频
            for i in range(0, len(to_cancel_ids), chunk_size):
                chunk = to_cancel_ids[i:i + chunk_size]
                # 并发撤单
                await asyncio.gather(*[adapter.cancel_order(oid, symbol=self.symbol) for oid in chunk],
                                     return_exceptions=True)

            # 清理内存状态 (假设撤单都成功，保持一致性，如果失败下次 reconcile 会再次重试)
            async with self.state_lock:
                for oid in to_cancel_ids: self.active_orders.pop(oid, None)

        # 4. 执行挂单
        if to_place_specs:
            sem = asyncio.Semaphore(5)  # 限制并发挂单数

            async def _place_wrapper(side, price):
                async with sem:
                    try:
                        oid = await adapter.create_order(self.symbol, side, self.quantity_per_grid, price,
                                                         params={'post_only': True})
                        if oid:
                            async with self.state_lock:
                                self.active_orders[oid] = {'side': side, 'price': price}
                    except Exception as e:
                        logger.debug(f"Order fail: {e}")

            await asyncio.gather(*[_place_wrapper(s, p) for s, p in to_place_specs], return_exceptions=True)

    async def _cancel_all_orders_force(self):
        adapter = self.adapters[self.exchange_name]
        async with self.state_lock:
            known_ids = list(self.active_orders.keys())
            self.active_orders.clear()

        if known_ids:
            chunk_size = 5
            for i in range(0, len(known_ids), chunk_size):
                chunk = known_ids[i:i + chunk_size]
                await asyncio.gather(*[adapter.cancel_order(oid, symbol=self.symbol) for oid in chunk],
                                     return_exceptions=True)