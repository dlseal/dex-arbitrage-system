# backend/app/strategies/ai_grid.py
import asyncio
import logging
import time
import math
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP
from typing import Dict, Any, List, Optional, Set, Tuple

from app.config import settings
from app.utils.llm_client import fetch_grid_advice

logger = logging.getLogger("AI_Grid_Pro")


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AI_Grid_Pro_v3_Production"
        self.adapters = adapters

        # --- 配置加载 ---
        self.conf = settings.strategies.ai_grid
        self.exchange_name = self.conf.exchange

        if not settings.common.target_symbols:
            logger.critical("❌ [Init] Target symbols not configured.")
            self.is_active = False
            return

        self.symbol = settings.common.target_symbols[0]

        # --- 核心参数 (初始化为空，等待 AI 填充) ---
        # [修改点 1] 不再读取 conf 中的默认网格参数，改为 0
        self.upper_price = 0.0
        self.lower_price = 0.0
        self.grid_count = 0

        # 模式仍然可以读配置作为默认倾向，或者也由 AI 决定(当前代码仅部分支持AI决定模式)
        self.grid_mode = getattr(self.conf, 'grid_mode', 'GEOMETRIC').upper()

        self.principal = self.conf.principal
        self.leverage = self.conf.leverage
        self.min_order_notional = self.conf.min_order_size

        # 风控参数
        self.stop_loss_pct = 0.05
        self.maker_fee_rate = 0.0003

        # --- 状态管理 ---
        self.active_orders: Dict[str, Dict] = {}
        self.grid_levels: List[float] = []
        self.quantity_per_grid = 0.0

        # --- 时间控制 ---
        self.next_check_ts = 0.0
        self.max_check_interval = self.conf.max_check_interval

        # --- 并发控制 ---
        self.state_lock = asyncio.Lock()
        self.is_updating = False
        self.is_active = True
        self.initialized = False
        self.emergency_stopped = False

        # --- 基础数据缓存 ---
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
        """启动引导程序"""
        await asyncio.sleep(2.0)
        await self._init_market_info()

        logger.info("🧹 [Bootstrap] Cleaning up existing orders...")
        await self._cancel_all_orders_force()

        self.initialized = True

        # 立即触发一次 AI 咨询以初始化策略
        self.next_check_ts = time.time() - 1.0
        logger.info("🚀 [Bootstrap] Ready. Waiting for AI initialization...")

    async def _init_market_info(self):
        retry = 0
        while retry < 5:
            try:
                adapter = self.adapters[self.exchange_name]
                if hasattr(adapter, 'contract_map'):
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

        if evt_type == 'trade':
            await self._process_trade_fill(event)
            return

        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            current_price = (event['bid'] + event['ask']) / 2
            if current_price <= 0: return

            # 仅在有策略参数时检查熔断
            if self.grid_count > 0:
                if await self._check_circuit_breaker(current_price):
                    return

            if not self.is_updating:
                now = time.time()
                if now >= self.next_check_ts:
                    asyncio.create_task(self._perform_ai_consultation(current_price))

    async def _check_circuit_breaker(self, current_price: float) -> bool:
        if current_price < self.lower_price * (1 - self.stop_loss_pct):
            logger.critical(f"🚨 [CIRCUIT BREAKER] Price {current_price} < StopLoss")
            await self._emergency_shutdown()
            return True
        if current_price > self.upper_price * (1 + self.stop_loss_pct):
            logger.critical(f"🚨 [CIRCUIT BREAKER] Price {current_price} > MaxLimit")
            await self._emergency_shutdown()
            return True
        return False

    async def _emergency_shutdown(self):
        self.emergency_stopped = True
        self.is_active = False
        await self._cancel_all_orders_force()
        logger.critical("🛑 Strategy HALTED.")

    async def _process_trade_fill(self, trade: dict):
        if trade['exchange'] != self.exchange_name: return
        order_id = str(trade.get('order_id') or trade.get('client_order_id', ''))

        async with self.state_lock:
            if order_id not in self.active_orders: return
            filled_order = self.active_orders.pop(order_id)

        logger.info(f"⚡️ [Filled] {filled_order['side']} @ {filled_order['price']}")

        if not self.is_updating and self.grid_count > 0:
            asyncio.create_task(self._reconcile_orders(filled_order['price']))

    async def _perform_ai_consultation(self, current_price: float):
        """咨询 AI"""
        if self.is_updating: return
        self.is_updating = True

        try:
            logger.info(f"🧠 [AI] Consulting... Ref: {current_price:.2f}")

            # [修改点 2] 动态判断状态
            if self.grid_count == 0 or self.upper_price == 0:
                status_str = "INACTIVE"
                context = {"info": "No active grid. Waiting for initialization."}
            else:
                status_str = "ACTIVE"
                context = {
                    "upper": self.upper_price,
                    "lower": self.lower_price,
                    "count": self.grid_count,
                    "mode": self.grid_mode
                }

            # 调用 LLM，传入 status_str
            advice = await fetch_grid_advice(self.symbol, current_price, context, status_str)

            if advice:
                logger.info(f"🤖 [AI Decision] {advice.get('action')} | Reason: {advice.get('reason')}")
            else:
                logger.warning("🤖 [AI Decision] Empty response.")

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

                    logger.info(
                        f"✨ [AI Update Applied] {self.lower_price}-{self.upper_price} | Grids: {self.grid_count}")

                    if self._calculate_grid_params(current_price):
                        await self._reconcile_orders(current_price)
                else:
                    logger.warning(f"⚠️ [AI] Invalid params: {advice}")

            self.next_check_ts = time.time() + min(duration * 3600, self.max_check_interval)
            logger.info(f"⏳ Next AI Check in {duration}h")

        except Exception as e:
            logger.error(f"❌ [AI] Loop Error: {e}", exc_info=True)
            self.next_check_ts = time.time() + 300
        finally:
            self.is_updating = False

    def _calculate_grid_params(self, current_price: float) -> bool:
        try:
            if self.grid_count < 2: return False

            total_equity = self.principal * self.leverage
            safe_min_notional = float(self.min_order_notional) * 1.1
            max_grids = int(total_equity / safe_min_notional)

            if self.grid_count > max_grids:
                logger.warning(f"⚠️ [Risk] Downgrading grids {self.grid_count} -> {max_grids}")
                self.grid_count = max_grids

            if self.grid_count < 2: return False

            per_grid_equity = total_equity / self.grid_count
            raw_qty = per_grid_equity / current_price
            d_qty = Decimal(str(raw_qty))
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

        target_orders = set()
        buffer = float(self.tick_size) * 1.5

        for level_price in self.grid_levels:
            if level_price < (current_price - buffer):
                target_orders.add(('BUY', f"{level_price:.6f}"))
            elif level_price > (current_price + buffer):
                target_orders.add(('SELL', f"{level_price:.6f}"))

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

        if to_cancel_ids:
            chunk_size = 10
            for i in range(0, len(to_cancel_ids), chunk_size):
                chunk = to_cancel_ids[i:i + chunk_size]
                await asyncio.gather(*[adapter.cancel_order(oid, symbol=self.symbol) for oid in chunk],
                                     return_exceptions=True)
            async with self.state_lock:
                for oid in to_cancel_ids: self.active_orders.pop(oid, None)

        if to_place_specs:
            sem = asyncio.Semaphore(10)

            async def _place_wrapper(side, price):
                async with sem:
                    try:
                        oid = await adapter.create_order(self.symbol, side, self.quantity_per_grid, price,
                                                         params={'post_only': True})
                        if oid:
                            async with self.state_lock:
                                self.active_orders[oid] = {'side': side, 'price': price}
                    except Exception:
                        pass  # Ignore failures

            await asyncio.gather(*[_place_wrapper(s, p) for s, p in to_place_specs], return_exceptions=True)

    async def _cancel_all_orders_force(self):
        adapter = self.adapters[self.exchange_name]
        async with self.state_lock:
            known_ids = list(self.active_orders.keys())
            self.active_orders.clear()
        if known_ids:
            await asyncio.gather(*[adapter.cancel_order(oid, symbol=self.symbol) for oid in known_ids],
                                 return_exceptions=True)