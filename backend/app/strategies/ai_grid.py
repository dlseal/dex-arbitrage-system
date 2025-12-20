# backend/app/strategies/ai_grid.py
import asyncio
import logging
import time
from collections import deque
from decimal import Decimal
from typing import Dict, Any, List, Optional, Deque

import pandas as pd
import numpy as np

from app.config import settings
from app.utils.llm_client import fetch_grid_advice

logger = logging.getLogger("AiGridStrategy")


class TechnicalAnalysis:
    """
    生产级技术指标计算器
    """

    @staticmethod
    def calculate_indicators(candles: List[Dict]) -> Dict[str, Any]:
        """
        计算 ATR(14) 和 RSI(14)
        """
        if not candles or len(candles) < 20:
            return {'atr': None, 'rsi': None, 'trend': "Insufficient Data"}

        try:
            df = pd.DataFrame(candles)
            # 确保数据类型正确
            for col in ['high', 'low', 'close', 'open']:
                df[col] = df[col].astype(float)

            # --- 1. 计算 ATR (Average True Range) ---
            df['h-l'] = df['high'] - df['low']
            df['h-pc'] = abs(df['high'] - df['close'].shift(1))
            df['l-pc'] = abs(df['low'] - df['close'].shift(1))
            df['tr'] = df[['h-l', 'h-pc', 'l-pc']].max(axis=1)
            df['atr'] = df['tr'].rolling(window=14).mean()

            # --- 2. 计算 RSI (Relative Strength Index) ---
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()

            # 避免除以零
            rs = gain / loss.replace(0, np.nan)
            df['rsi'] = 100 - (100 / (1 + rs))
            df['rsi'] = df['rsi'].fillna(50)

            # --- 3. 简单的趋势判定 ---
            df['sma_short'] = df['close'].rolling(window=7).mean()
            df['sma_long'] = df['close'].rolling(window=25).mean()

            latest = df.iloc[-1]
            return {
                'atr': round(float(latest['atr']), 4) if pd.notnull(latest['atr']) else 0.0,
                'rsi': round(float(latest['rsi']), 2) if pd.notnull(latest['rsi']) else 50.0,
                'trend': "Bullish" if latest['sma_short'] > latest['sma_long'] else "Bearish"
            }

        except Exception as e:
            logger.error(f"Indicator calculation error: {e}")
            return {'atr': 0.0, 'rsi': 50.0, 'trend': "Error"}


class CandleManager:
    """K 线数据管理器"""

    def __init__(self, interval_sec=900, max_len=100):
        self.interval = interval_sec
        self.candles: Deque[Dict] = deque(maxlen=max_len)
        self.current_candle: Optional[Dict] = None

    def update(self, price: float, ts: float):
        if not self.current_candle:
            self._new_candle(price, ts)
            return

        next_candle_ts = self.current_candle['ts'] + self.interval
        if ts >= next_candle_ts:
            self.candles.append(self.current_candle.copy())
            self._new_candle(price, ts)
        else:
            c = self.current_candle
            c['high'] = max(c['high'], price)
            c['low'] = min(c['low'], price)
            c['close'] = price
            c['volume'] += 1

    def _new_candle(self, price, ts):
        aligned_ts = (int(ts) // self.interval) * self.interval
        self.current_candle = {
            'ts': aligned_ts,
            'open': price, 'high': price, 'low': price, 'close': price,
            'volume': 0
        }

    def get_all_candles(self) -> List[Dict]:
        data = list(self.candles)
        if self.current_candle:
            data.append(self.current_candle.copy())
        return data

    def get_recent_candles_str(self) -> str:
        data = list(self.candles)[-5:]
        if not data and self.current_candle:
            data = [self.current_candle]
        lines = []
        for c in data:
            t_str = time.strftime('%H:%M', time.localtime(c['ts']))
            lines.append(f"[{t_str}] C:{c['close']:.2f}")
        return ", ".join(lines)


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: Any = None):
        self.adapters = adapters
        self.risk_controller = risk_controller
        self.name = "AI_GRID"  # [必须] 防止 Engine 启动报错

        # 配置读取兼容性处理
        if hasattr(settings.strategies, 'ai_grid'):
            self.conf = settings.strategies.ai_grid
        else:
            # Fallback for direct import scenarios
            from app.config import StrategyConfig
            self.conf = StrategyConfig(active="AI_GRID")

        self.exchange = self.conf.exchange
        if not settings.common.target_symbols:
            raise ValueError("Config target_symbols is empty")
        self.symbol = settings.common.target_symbols[0]

        self.adapter = adapters.get(self.exchange)

        # 核心参数
        self.grid_levels = self.conf.grid_count
        self.upper_price = self.conf.upper_price
        self.lower_price = self.conf.lower_price

        self.check_interval = 300
        self.max_drawdown_pct = 0.10  # 10% 最大回撤熔断

        # 状态
        self.active_order_ids: List[str] = []
        self.last_ai_update_ts = 0
        self.price_escape_start_ts = 0
        self.initial_equity = 0.0
        self.is_active = True

        self.candle_manager = CandleManager(interval_sec=900)

    async def start(self):
        """策略启动"""
        if not self.adapter:
            logger.error(f"❌ Adapter not found for {self.exchange}")
            self.is_active = False
            return

        # 获取初始权益
        try:
            balances = await self.adapter.get_balances() if hasattr(self.adapter, 'get_balances') else []
            usdt = next((b.total for b in balances if 'USD' in b.currency), 0.0)
            self.initial_equity = usdt if usdt > 0 else 1000.0
            logger.info(
                f"🚀 Strategy Started | Equity: {self.initial_equity} | Range: [{self.lower_price}-{self.upper_price}]")
        except Exception:
            self.initial_equity = 1000.0

        # 首次启动强制更新网格
        await self._update_grid_structure(force=True)

    async def on_tick(self, tick_data: dict):
        if not self.is_active: return

        # 宽松匹配 Symbol
        t_sym = tick_data.get('symbol', '')
        if self.symbol not in t_sym and t_sym not in self.symbol:
            return

        # 提取价格
        current_price = tick_data.get('last', 0)
        if current_price == 0:
            current_price = (tick_data.get('bid', 0) + tick_data.get('ask', 0)) / 2

        if current_price <= 0: return

        # 1. 更新 K 线
        self.candle_manager.update(current_price, time.time())

        # 2. 风控检查 (含熔断逻辑)
        await self._check_capital_protection(current_price)

        # 3. 价格区间检查
        self._check_price_escape(current_price)

        # 4. 定时 AI 优化
        if time.time() - self.last_ai_update_ts > self.check_interval:
            logger.info("⏰ Timer Trigger: Requesting AI Update...")
            await self._update_grid_structure()

    def _check_price_escape(self, current_price):
        """检查价格是否脱离网格区间"""
        is_escaped = current_price > self.upper_price or current_price < self.lower_price

        if is_escaped:
            if self.price_escape_start_ts == 0:
                self.price_escape_start_ts = time.time()
                logger.warning(
                    f"⚠️ Price Escaped [{self.lower_price:.2f}, {self.upper_price:.2f}] @ {current_price:.2f}")

            # 持续 60s 脱离则重置
            if time.time() - self.price_escape_start_ts > 60:
                logger.info(f"🔄 Escape Confirmed. Forcing AI Rebalance...")
                asyncio.create_task(self._update_grid_structure(force=True))
                self.price_escape_start_ts = 0
        else:
            self.price_escape_start_ts = 0

    async def _check_capital_protection(self, current_price):
        """
        [触发器] 风控检测：计算浮动盈亏，如果回撤超过阈值则调用 _emergency_stop
        """
        if not hasattr(self.adapter, 'fetch_positions'): return

        try:
            positions = await self.adapter.fetch_positions()
            target_pos = next((p for p in positions if self.symbol in str(p.get('symbol', ''))), None)

            if target_pos:
                size = float(target_pos.get('size', 0))
                if size == 0: return

                entry = float(target_pos.get('entry_price', 0) or current_price)

                # 计算未实现盈亏
                if size > 0:
                    unrealized_pnl = (current_price - entry) * abs(size)
                else:
                    unrealized_pnl = (entry - current_price) * abs(size)

                current_equity = self.initial_equity + unrealized_pnl
                drawdown = (self.initial_equity - current_equity) / self.initial_equity

                if drawdown > self.max_drawdown_pct:
                    logger.critical(
                        f"🛑 Max Drawdown Triggered! ({drawdown * 100:.2f}% > {self.max_drawdown_pct * 100:.2f}%)")
                    await self._emergency_stop()
        except Exception as e:
            logger.error(f"⚠️ Risk Check Error: {e}")

    async def _emergency_stop(self):
        """
        [执行器] 紧急停止：停止策略 -> 撤单 -> 市价全平
        """
        logger.critical("🛑 EMERGENCY STOP PROCEDURE STARTED!")
        self.is_active = False

        # 1. 撤销挂单
        logger.info("🛑 Step 1: Cancelling all open orders...")
        await self._cancel_all_orders()

        # 2. 强平持仓
        if hasattr(self.adapter, 'close_position'):
            logger.info(f"🛑 Step 2: Executing Market Close for {self.symbol}...")
            try:
                await asyncio.wait_for(self.adapter.close_position(self.symbol), timeout=15.0)
                logger.info("✅ Close Position Request Sent.")
            except Exception as e:
                logger.error(f"❌ Close Position Failed: {e}")
        else:
            logger.warning("⚠️ Adapter missing 'close_position'. Please manually close positions!")

        logger.critical("🛑 Strategy Fully Stopped.")

    async def _update_grid_structure(self, force=False):
        """
        核心逻辑：计算指标 -> 调用 LLM -> 重建网格
        """
        self.last_ai_update_ts = time.time()

        # 1. 获取盘口数据
        ticker = {}
        try:
            ticker = await self.adapter.fetch_orderbook(self.symbol)
            if not ticker:
                logger.warning("⚠️ AI Update Skipped: Orderbook is empty")
                return
            current_price = (ticker['bid'] + ticker['ask']) / 2
        except Exception as e:
            logger.error(f"⚠️ AI Update Skipped: Fetch OB failed: {e}")
            return

        # 2. 计算指标 & 组装参数
        candles_data = self.candle_manager.get_all_candles()
        indicators = TechnicalAnalysis.calculate_indicators(candles_data)

        context_params = {
            "atr": f"{indicators['atr']:.4f}",
            "rsi": f"{indicators['rsi']:.2f}",
            "trend_1h": indicators['trend'],
            "bid_vol": ticker.get('bid_volume', 0),
            "ask_vol": ticker.get('ask_volume', 0),
            "imbalance": 0.0,
            "recent_candles": self.candle_manager.get_recent_candles_str()
        }

        # [关键修复] 合并 Config参数 + 上下文指标参数
        request_params = self.conf.dict()
        request_params.update(context_params)

        logger.info(f"🤖 Consulting AI... (Price: {current_price:.2f} | RSI: {context_params['rsi']})")

        # 3. 调用 LLM
        advice = await fetch_grid_advice(
            symbol=self.symbol,
            current_price=current_price,
            current_params=request_params,
            status_str="ACTIVE"
        )

        if not advice:
            logger.warning("⚠️ AI Update Skipped: No advice returned")
            return

        action = advice.get("action", "CONTINUE")
        logger.info(f"🤖 AI Response: {action} | Reason: {advice.get('reason')}")

        if action == "UPDATE" or force:
            new_upper = float(advice.get("upper_price", self.upper_price))
            new_lower = float(advice.get("lower_price", self.lower_price))
            new_count = int(advice.get("grid_count", self.grid_levels))

            if not force and abs(new_upper - self.upper_price) / self.upper_price < 0.01:
                return

            logger.info(f"♻️ Rebuilding Grid -> [{new_lower:.2f} - {new_upper:.2f}] Count: {new_count}")

            await self._cancel_all_orders()
            self.upper_price = new_upper
            self.lower_price = new_lower
            self.grid_levels = new_count

            await self._place_grid_orders(new_lower, new_upper, current_price)

    async def _place_grid_orders(self, lower, upper, current_price):
        if self.grid_levels <= 0: return
        step = (upper - lower) / self.grid_levels

        raw_qty = self.conf.min_order_size / current_price
        qty = float(Decimal(str(raw_qty)).quantize(Decimal("0.0001")))

        tasks = []
        for i in range(self.grid_levels + 1):
            price = lower + (i * step)
            # 防 Taker 保护 (0.2%)
            if abs(price - current_price) / current_price < 0.002:
                continue

            side = "SELL" if price > current_price else "BUY"
            tasks.append(self.adapter.create_order(
                symbol=self.symbol,
                side=side, amount=qty, price=price,
                order_type="LIMIT", post_only=True
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = sum(1 for r in results if isinstance(r, str))
        logger.info(f"✅ Grid Placed: {success}/{len(tasks)} orders active.")

        # 简单的 ID 记录 (实际应由 OrderManager 维护)
        self.active_order_ids.clear()
        for res in results:
            if isinstance(res, str) and res:
                self.active_order_ids.append(res)

    async def _cancel_all_orders(self):
        if not self.active_order_ids: return
        tasks = [self.adapter.cancel_order(oid, symbol=self.symbol) for oid in self.active_order_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.active_order_ids.clear()