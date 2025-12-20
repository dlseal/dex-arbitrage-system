import asyncio
import logging
import time
from collections import deque
from decimal import Decimal
from typing import Dict, Any, List, Optional, Deque, Tuple

import pandas as pd
import numpy as np

from app.config import settings
from app.utils.llm_client import fetch_grid_advice

logger = logging.getLogger("AiGridStrategy")


class TechnicalAnalysis:
    """
    生产级技术指标计算器
    使用 Pandas 向量化计算，确保性能和准确性
    """

    @staticmethod
    def calculate_indicators(candles: List[Dict]) -> Dict[str, Any]:
        """
        计算 ATR(14) 和 RSI(14)
        :param candles: List of dict {'ts':.., 'open':.., 'high':.., 'low':.., 'close':..}
        :return: {'atr': float, 'rsi': float, 'trend': str}
        """
        if not candles or len(candles) < 20:
            return {'atr': None, 'rsi': None, 'trend': "Insufficient Data"}

        try:
            df = pd.DataFrame(candles)
            # 确保数据类型正确
            for col in ['high', 'low', 'close', 'open']:
                df[col] = df[col].astype(float)

            # --- 1. 计算 ATR (Average True Range) ---
            # TR = Max(H-L, Abs(H-PrevC), Abs(L-PrevC))
            df['h-l'] = df['high'] - df['low']
            df['h-pc'] = abs(df['high'] - df['close'].shift(1))
            df['l-pc'] = abs(df['low'] - df['close'].shift(1))
            df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
            # 使用简单移动平均或 Wilder's Smoothing，这里使用 Rolling Mean
            df['atr'] = df['tr'].rolling(window=14).mean()

            # --- 2. 计算 RSI (Relative Strength Index) ---
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()

            # 避免除以零
            rs = gain / loss.replace(0, np.nan)
            df['rsi'] = 100 - (100 / (1 + rs))
            # 填充 NaN
            df['rsi'] = df['rsi'].fillna(50)

            # --- 3. 简单的趋势判定 (SMA交叉) ---
            df['sma_short'] = df['close'].rolling(window=7).mean()
            df['sma_long'] = df['close'].rolling(window=25).mean()

            latest = df.iloc[-1]
            atr_val = round(float(latest['atr']), 4) if pd.notnull(latest['atr']) else 0.0
            rsi_val = round(float(latest['rsi']), 2) if pd.notnull(latest['rsi']) else 50.0

            trend = "Consolidation"
            if latest['sma_short'] > latest['sma_long'] * 1.001:
                trend = "Bullish"
            elif latest['sma_short'] < latest['sma_long'] * 0.999:
                trend = "Bearish"

            return {
                'atr': atr_val,
                'rsi': rsi_val,
                'trend': trend
            }

        except Exception as e:
            logger.error(f"Indicator calculation error: {e}")
            return {'atr': 0.0, 'rsi': 50.0, 'trend': "Error"}


class CandleManager:
    """K 线数据管理器"""

    def __init__(self, interval_sec=900, max_len=100):  # 15m candles, 保留100根
        self.interval = interval_sec
        self.candles: Deque[Dict] = deque(maxlen=max_len)
        self.current_candle: Optional[Dict] = None

    def update(self, price: float, ts: float):
        # 1. 初始化
        if not self.current_candle:
            self._new_candle(price, ts)
            return

        # 2. 检查是否跨周期 (Align to interval)
        # 例如 900s: 10:00:00 -> 10:15:00
        candle_start_ts = self.current_candle['ts']
        next_candle_ts = candle_start_ts + self.interval

        if ts >= next_candle_ts:
            # 完成当前 K 线，推入历史
            self.candles.append(self.current_candle.copy())
            # 如果中间有空缺(长时间无成交)，这里简化处理直接开新线
            self._new_candle(price, ts)
        else:
            # 更新当前 K 线
            c = self.current_candle
            c['high'] = max(c['high'], price)
            c['low'] = min(c['low'], price)
            c['close'] = price
            c['volume'] += 1

    def _new_candle(self, price, ts):
        # 向下取整对齐时间戳
        aligned_ts = (int(ts) // self.interval) * self.interval
        self.current_candle = {
            'ts': aligned_ts,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': 0
        }

    def get_all_candles(self) -> List[Dict]:
        """获取历史 + 当前未完成的 K 线"""
        data = list(self.candles)
        if self.current_candle:
            data.append(self.current_candle.copy())
        return data

    def get_recent_candles_str(self) -> str:
        """格式化字符串供 LLM 阅读"""
        data = list(self.candles)[-5:]  # 仅最近5根
        if not data and self.current_candle:
            data = [self.current_candle]

        lines = []
        for c in data:
            t_str = time.strftime('%H:%M', time.localtime(c['ts']))
            lines.append(f"[{t_str}] O:{c['open']:.2f} H:{c['high']:.2f} L:{c['low']:.2f} C:{c['close']:.2f}")
        return "\n".join(lines)


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: Any = None):
        self.adapters = adapters
        self.risk_controller = risk_controller

        # 配置读取
        if hasattr(settings.strategies, 'ai_grid'):
            self.conf = settings.strategies.ai_grid
        else:
            raise ValueError("AiGrid config missing")

        self.exchange = self.conf.exchange
        if not settings.common.target_symbols:
            raise ValueError("Config target_symbols is empty")
        self.symbol = settings.common.target_symbols[0]

        self.adapter = adapters.get(self.exchange)

        # 核心参数
        self.grid_levels = self.conf.grid_count
        self.quantity_per_grid = 0.0001  # 默认值，会在 on_tick 或 update 中动态修正
        self.check_interval = 300  # 5分钟检查一次 AI
        self.escape_timeout = 60
        self.max_drawdown_pct = 0.10

        # 状态
        self.upper_price = self.conf.upper_price
        self.lower_price = self.conf.lower_price
        self.active_order_ids: List[str] = []

        self.last_ai_update_ts = 0
        self.price_escape_start_ts = 0
        self.initial_equity = 0.0
        self.is_active = True

        # 组件
        self.candle_manager = CandleManager(interval_sec=900)  # 15m K线

    async def start(self):
        """策略启动"""
        if not self.adapter:
            logger.error(f"❌ Adapter not found for {self.exchange}")
            self.is_active = False
            return

        # 获取初始权益用于风控
        try:
            balances = await self.adapter.get_balances() if hasattr(self.adapter, 'get_balances') else []
            # 简单查找 USDT 余额
            usdt = next((b.total for b in balances if 'USD' in b.currency), 0.0)
            self.initial_equity = usdt if usdt > 0 else 1000.0
            logger.info(f"🚀 AI Grid Started | Equity: {self.initial_equity}")
        except Exception as e:
            logger.warning(f"⚠️ Initial balance check failed: {e}")
            self.initial_equity = 1000.0

        # 首次构建网格
        await self._update_grid_structure(force=True)

    async def on_tick(self, tick_data: dict):
        if not self.is_active: return
        if tick_data.get('symbol') != self.symbol: return

        # 提取价格
        current_price = tick_data.get('last', 0)
        if current_price == 0:
            current_price = (tick_data.get('bid', 0) + tick_data.get('ask', 0)) / 2

        if current_price <= 0: return

        # 1. 更新 K 线
        self.candle_manager.update(current_price, time.time())

        # 2. 检查风控
        await self._check_capital_protection(current_price)
        self._check_price_escape(current_price)

        # 3. 定时 AI 优化
        if time.time() - self.last_ai_update_ts > self.check_interval:
            await self._update_grid_structure()

    def _check_price_escape(self, current_price):
        """检查价格是否脱离网格区间"""
        is_escaped = current_price > self.upper_price or current_price < self.lower_price

        if is_escaped:
            if self.price_escape_start_ts == 0:
                self.price_escape_start_ts = time.time()
                logger.warning(
                    f"⚠️ Price Escaped Grid [{self.lower_price:.2f}, {self.upper_price:.2f}] @ {current_price:.2f}")

            # 持续 60s 脱离则重置
            if time.time() - self.price_escape_start_ts > self.escape_timeout:
                logger.info(f"🔄 Trend Escape Confirmed. Requesting AI Rebalance...")
                asyncio.create_task(self._update_grid_structure(force=True))
                self.price_escape_start_ts = 0
        else:
            if self.price_escape_start_ts != 0:
                self.price_escape_start_ts = 0

    async def _check_capital_protection(self, current_price):
        """本金保护"""
        if not hasattr(self.adapter, 'fetch_positions'): return

        try:
            positions = await self.adapter.fetch_positions()
            target_pos = next((p for p in positions if self.symbol in str(p.get('symbol', ''))), None)

            if target_pos:
                size = float(target_pos.get('size', 0))
                # 如果 adapter 没有提供 entry_price，暂时用当前价代替(即忽略浮亏)，或者跳过
                entry = float(target_pos.get('entry_price', 0) or current_price)

                unrealized_pnl = (current_price - entry) * size
                current_equity = self.initial_equity + unrealized_pnl

                drawdown = (self.initial_equity - current_equity) / self.initial_equity

                if drawdown > self.max_drawdown_pct:
                    logger.critical(f"🛑 Max Drawdown ({drawdown * 100:.2f}%) Triggered! Stopping Strategy.")
                    await self._emergency_stop()
        except Exception:
            pass

    async def _update_grid_structure(self, force=False):
        """
        核心逻辑：计算指标 -> 调用 LLM -> 重建网格
        """
        self.last_ai_update_ts = time.time()

        # 1. 获取盘口数据
        try:
            ticker = await self.adapter.fetch_orderbook(self.symbol)
            if not ticker: return
            current_price = (ticker['bid'] + ticker['ask']) / 2
        except Exception:
            return

        # 2. 计算真实指标
        candles_data = self.candle_manager.get_all_candles()
        indicators = TechnicalAnalysis.calculate_indicators(candles_data)

        atr_val = indicators['atr']
        rsi_val = indicators['rsi']
        trend_str = indicators['trend']

        # 3. 组装 Prompt 参数
        context_params = {
            "atr": f"{atr_val:.4f}" if atr_val else "Collecting Data",
            "rsi": f"{rsi_val:.2f}" if rsi_val else "Collecting Data",
            "trend_1h": trend_str,
            "bid_vol": ticker.get('bid_volume', 0),
            "ask_vol": ticker.get('ask_volume', 0),
            "imbalance": 0.0,  # 可扩展计算 Orderbook Imbalance
            "recent_candles": self.candle_manager.get_recent_candles_str()
        }

        # 4. 调用 LLM
        # 如果数据太少(刚启动)，LLM 可能依据不足，但这符合预期
        advice = await fetch_grid_advice(
            symbol=self.symbol,
            current_price=current_price,
            current_params=self.conf.dict(),
            status_str="ACTIVE"
        )

        if not advice: return

        action = advice.get("action", "CONTINUE")
        logger.info(
            f"🤖 AI Advice: {action} | ATR:{context_params['atr']} RSI:{context_params['rsi']} | Reason: {advice.get('reason')}")

        if action == "UPDATE" or force:
            new_upper = float(advice.get("upper_price", self.upper_price))
            new_lower = float(advice.get("lower_price", self.lower_price))
            new_count = int(advice.get("grid_count", self.grid_levels))

            # 过滤微小变动
            if not force and abs(new_upper - self.upper_price) / self.upper_price < 0.01:
                return

            logger.info(f"♻️ Rebuilding Grid: [{new_lower:.2f} - {new_upper:.2f}] (Count: {new_count})")

            await self._cancel_all_orders()
            self.upper_price = new_upper
            self.lower_price = new_lower
            self.grid_levels = new_count

            await self._place_grid_orders(new_lower, new_upper, current_price)

    async def _place_grid_orders(self, lower, upper, current_price):
        if self.grid_levels <= 0: return
        step = (upper - lower) / self.grid_levels

        # 计算每格数量 (基于 min_order_size 配置，通常是 USDT 价值)
        # 例如 min_order_size = 100U, price = 50000 -> 0.002 BTC
        raw_qty = self.conf.min_order_size / current_price
        qty = float(Decimal(str(raw_qty)).quantize(Decimal("0.0001")))  # 简单精度控制

        tasks = []
        for i in range(self.grid_levels + 1):
            price = lower + (i * step)
            if price <= 0: continue

            # 避免在当前价附近挂单 (防止 Taker)
            if abs(price - current_price) / current_price < 0.002:
                continue

            side = "SELL" if price > current_price else "BUY"

            tasks.append(self.adapter.create_order(
                symbol=self.symbol,
                side=side,
                amount=qty,
                price=price,
                order_type="LIMIT",
                post_only=True
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        self.active_order_ids.clear()
        success = 0
        for res in results:
            if isinstance(res, str) and res:
                self.active_order_ids.append(res)
                success += 1
            elif isinstance(res, Exception):
                logger.debug(f"Grid order failed: {res}")

        logger.info(f"✅ Grid Placed: {success}/{len(tasks)} active orders.")

    async def _cancel_all_orders(self):
        if not self.active_order_ids: return
        tasks = [self.adapter.cancel_order(oid, symbol=self.symbol) for oid in self.active_order_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.active_order_ids.clear()

    async def _emergency_stop(self):
        self.is_active = False
        await self._cancel_all_orders()
        # 尝试平仓逻辑...
        logger.critical("🛑 Strategy Stopped.")