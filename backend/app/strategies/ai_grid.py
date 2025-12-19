import asyncio
import logging
import time
import math
import os
from typing import Dict, Any, List, Optional
from decimal import Decimal

from app.config import Config
from app.utils.llm_client import fetch_grid_advice

logger = logging.getLogger("AI_Grid_StepFixed")


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AI_Grid_Contract_StepFixed"
        self.adapters = adapters
        self.exchange_name = Config.GRID_EXCHANGE
        self.symbol = Config.TARGET_SYMBOLS[0]

        # 状态初始化
        self.grid_levels: List[float] = []
        self.active_orders: Dict[str, Dict] = {}
        self.grid_order_map: Dict[int, str] = {}

        # 基础参数
        self.upper_price = Config.GRID_UPPER_PRICE
        self.lower_price = Config.GRID_LOWER_PRICE
        self.grid_count = Config.GRID_COUNT
        self.quantity = 0.0001

        # 资金参数
        self.principal = Config.GRID_STRATEGY_PRINCIPAL
        self.leverage = Config.GRID_STRATEGY_LEVERAGE

        # 最小下单金额 (Notional)
        config_min = Config.GRID_MIN_ORDER_SIZE
        if config_min > 0:
            self.min_order_notional = config_min
        else:
            ex_name = self.exchange_name.lower()
            if 'nado' in ex_name:
                self.min_order_notional = 105.0
            elif 'grvt' in ex_name:
                self.min_order_notional = 10.0
            else:
                self.min_order_notional = 10.0

        # === 新增：数量步长配置 ===
        # Nado BTC 步长是 0.00005，为了安全，我们设为 0.0001
        # 其他交易所通常是 0.0001 或 0.00001
        self.qty_step_size = 0.0001

        # 时间调度
        self.strategy_expiry_ts = 0.0
        self.next_check_ts = 0.0
        self.max_check_interval = Config.GRID_MAX_CHECK_INTERVAL

        # 状态控制
        self.is_active = True
        self.is_updating = False
        self.initialized = False

        # 锁与并发
        self.lock = asyncio.Lock()
        self.order_semaphore = asyncio.Semaphore(5)

        self._validate_adapters()

    def _validate_adapters(self):
        if self.exchange_name not in self.adapters:
            logger.error(f"❌ [Grid] 目标交易所 {self.exchange_name} 未加载！")
            self.is_active = False
        else:
            logger.info(f"🤖 [Strategy] 就绪 | 交易所: {self.exchange_name}")
            logger.info(
                f"   💰 本金: ${self.principal} | 最小金额: ${self.min_order_notional} | 数量步长: {self.qty_step_size}")
            self.strategy_expiry_ts = time.time() - 1

    async def on_tick(self, event: dict):
        if not self.is_active: return
        evt_type = event.get('type')

        if evt_type == 'trade':
            await self._process_trade_event(event)
            return

        if self.is_updating: return

        now = time.time()
        should_ask_ai = (now >= self.strategy_expiry_ts) or (now >= self.next_check_ts)

        if should_ask_ai:
            if evt_type == 'tick' and event.get('symbol') == self.symbol:
                current_price = (event['bid'] + event['ask']) / 2
                if current_price > 0:
                    asyncio.create_task(self._perform_ai_consultation(current_price))

        if evt_type == 'tick':
            if not self.initialized and self.grid_levels:
                await self._initial_placement(event)

    def _round_to_step(self, value, step):
        """将数值向下取整到最近的步长倍数"""
        if step <= 0: return value
        # 使用 Decimal 防止浮点数精度问题 (e.g. 0.000300000000004)
        d_val = Decimal(str(value))
        d_step = Decimal(str(step))

        # 向下取整逻辑: (value // step) * step
        num_steps = math.floor(float(d_val / d_step))
        rounded_val = float(Decimal(str(num_steps)) * d_step)
        return rounded_val

    async def _perform_ai_consultation(self, current_price: float):
        if self.is_updating: return
        self.is_updating = True
        logger.info(f"🧠 [Consult] 正在咨询 AI (Ref: {current_price:.2f})...")

        try:
            current_context = None
            async with self.lock:
                if self.initialized and self.active_orders:
                    current_context = {"upper": self.upper_price, "lower": self.lower_price, "count": self.grid_count}

            advice = await fetch_grid_advice(self.symbol, current_price, current_context)

            if not advice:
                logger.warning("⚠️ AI 未响应，5分钟后重试")
                self.next_check_ts = time.time() + 300
                return

            action = advice.get("action", "UPDATE").upper()
            duration = float(advice.get("duration_hours", 4.0))

            now = time.time()
            self.strategy_expiry_ts = now + (duration * 3600)
            self.next_check_ts = now + min(duration * 3600, self.max_check_interval)

            if action == "UPDATE" or not self.initialized:
                await self._cancel_all_orders()

                async with self.lock:
                    raw_count = int(advice.get('grid_count', 5))
                    self.upper_price = float(advice.get('upper_price', current_price * 1.05))
                    self.lower_price = float(advice.get('lower_price', current_price * 0.95))

                    # === 资金计算与步长修正 ===
                    total_power = self.principal * self.leverage

                    # 1. 动态调整网格数
                    max_possible_grids = int(total_power / self.min_order_notional)
                    if max_possible_grids < 1:
                        logger.error(f"❌ 资金不足! ${total_power} < ${self.min_order_notional}")
                        self.grid_count = 1
                    elif raw_count > max_possible_grids:
                        logger.warning(f"⚠️ 资金分散警告: {raw_count}格 -> 降级为 {max_possible_grids}格")
                        self.grid_count = max_possible_grids
                    else:
                        self.grid_count = raw_count

                    # 2. 计算每格数量
                    per_grid_usdt = total_power / self.grid_count
                    ref_price = current_price
                    raw_qty = per_grid_usdt / ref_price

                    # 3. === 关键修复：使用步长向下取整 ===
                    # 之前的 0.00129 变成了 0.0012 (如果步长是 0.0001)
                    # 这样保证了它是 0.0001 的倍数，也就一定是 0.00005 (Nado) 的倍数
                    self.quantity = self._round_to_step(raw_qty, self.qty_step_size)

                    # 4. 防止取整后变成 0
                    if self.quantity < self.qty_step_size:
                        logger.warning(f"⚠️ 数量过小 ({raw_qty}), 强制设为最小步长 {self.qty_step_size}")
                        self.quantity = self.qty_step_size

                    logger.info(
                        f"💰 [资金分配] {self.grid_count} 格 | 每格 ${per_grid_usdt:.1f} | 原始 {raw_qty:.5f} -> 修正后 {self.quantity:.5f}")

                    self._calculate_grids()
                    self.initialized = False

            elif action == "CONTINUE":
                logger.info("✅ 策略维持现状")

        except Exception as e:
            logger.error(f"❌ AI 调度异常: {e}", exc_info=True)
            self.next_check_ts = time.time() + 300
        finally:
            self.is_updating = False

    async def _cancel_all_orders(self):
        adapter = self.adapters[self.exchange_name]
        async with self.lock:
            oids = list(self.active_orders.keys())

        if not oids: return
        logger.info(f"🧹 撤销 {len(oids)} 个挂单...")
        chunk_size = 5
        for i in range(0, len(oids), chunk_size):
            chunk = oids[i:i + chunk_size]
            tasks = [adapter.cancel_order(oid, symbol=self.symbol) for oid in chunk]
            await asyncio.gather(*tasks, return_exceptions=True)

        async with self.lock:
            self.active_orders.clear()
            self.grid_order_map.clear()

    async def _process_trade_event(self, trade: dict):
        if trade['exchange'] != self.exchange_name: return
        order_id = str(trade.get('order_id') or trade.get('client_order_id', ''))

        async with self.lock:
            if order_id not in self.active_orders: return
            order_info = self.active_orders.pop(order_id)
            grid_index = order_info['index']
            filled_side = order_info['side']

            if self.grid_order_map.get(grid_index) == order_id:
                del self.grid_order_map[grid_index]

            if self.is_updating: return

            logger.info(f"⚡️ [Fill] #{grid_index} {filled_side} 成交 -> 补单")
            try:
                if filled_side == 'BUY':
                    next_index = grid_index + 1
                    if next_index < len(self.grid_levels) and next_index not in self.grid_order_map:
                        asyncio.create_task(self._place_grid_order(next_index, 'SELL', self.grid_levels[next_index]))
                elif filled_side == 'SELL':
                    prev_index = grid_index - 1
                    if prev_index >= 0 and prev_index not in self.grid_order_map:
                        asyncio.create_task(self._place_grid_order(prev_index, 'BUY', self.grid_levels[prev_index]))
            except Exception as e:
                logger.error(f"补单调度失败: {e}")

    def _calculate_grids(self):
        try:
            d_upper = Decimal(str(self.upper_price))
            d_lower = Decimal(str(self.lower_price))
            if self.grid_count < 2:
                self.grid_levels = []
                return
            step = (d_upper - d_lower) / Decimal(self.grid_count - 1)
            self.grid_levels = []
            for i in range(self.grid_count):
                p = d_lower + (step * i)
                self.grid_levels.append(float(p.quantize(Decimal("0.0001"))))
        except:
            self.grid_levels = []

    async def _initial_placement(self, tick: dict):
        if tick['symbol'] != self.symbol or tick['exchange'] != self.exchange_name: return
        async with self.lock:
            if self.initialized: return
            current_price = (tick['bid'] + tick['ask']) / 2
            if current_price <= 0: return

            logger.info(f"🚀 [Deploy] 部署 {len(self.grid_levels)} 个网格... Ref: {current_price:.2f}")
            tasks = []
            for i, p in enumerate(self.grid_levels):
                side = None
                if p < current_price:
                    side = 'BUY'
                elif p > current_price:
                    side = 'SELL'
                if side:
                    tasks.append(self._place_grid_order(i, side, p))
            if tasks:
                await asyncio.gather(*tasks)
            self.initialized = True

    async def _place_grid_order(self, index, side, price):
        adapter = self.adapters[self.exchange_name]
        async with self.order_semaphore:
            try:
                # 安全检查
                if self.quantity * price < self.min_order_notional * 0.9:
                    # 日志降级为 DEBUG，避免刷屏
                    logger.debug(
                        f"Skipping small order #{index}: {self.quantity * price:.2f} < {self.min_order_notional}")
                    return

                oid = await adapter.create_order(self.symbol, side, self.quantity, price, params={'post_only': True})
                if oid:
                    async with self.lock:
                        self.active_orders[oid] = {'index': index, 'side': side, 'price': price}
                        self.grid_order_map[index] = oid
                await asyncio.sleep(0.2)
            except Exception as e:
                if "Post only" not in str(e):
                    logger.error(f"下单失败 #{index}: {e}")