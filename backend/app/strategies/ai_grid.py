import asyncio
import logging
import time
import math
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from typing import Dict, Any, List, Optional, Set, Tuple

from app.config import settings
from app.utils.llm_client import fetch_grid_advice

logger = logging.getLogger("AI_Grid_Pro")


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AI_Grid_Pro_v2"
        self.adapters = adapters

        # --- 配置加载 ---
        self.conf = settings.strategies.ai_grid
        self.exchange_name = self.conf.exchange

        if not settings.common.target_symbols:
            logger.critical("❌ Target symbols not configured.")
            self.is_active = False
            return

        self.symbol = settings.common.target_symbols[0]

        # --- 核心参数 ---
        self.upper_price = self.conf.upper_price
        self.lower_price = self.conf.lower_price
        self.grid_count = self.conf.grid_count
        self.grid_mode = getattr(self.conf, 'grid_mode', 'ARITHMETIC').upper()  # 支持 GEOMETRIC / ARITHMETIC

        self.principal = self.conf.principal
        self.leverage = self.conf.leverage
        self.min_order_notional = self.conf.min_order_size
        self.stop_loss_pct = 0.05  # 5% 熔断阈值

        # --- 状态管理 ---
        # active_orders 结构: {order_id: {'side': str, 'price': float, 'grid_index': int}}
        self.active_orders: Dict[str, Dict] = {}
        self.grid_levels: List[float] = []
        self.quantity_per_grid = 0.0

        # --- 时间控制 ---
        self.strategy_expiry_ts = 0.0
        self.next_check_ts = 0.0
        self.max_check_interval = self.conf.max_check_interval

        # --- 并发控制 ---
        self.state_lock = asyncio.Lock()  # 仅用于保护内存状态，不包含 IO
        self.is_updating = False
        self.is_active = True
        self.initialized = False
        self.emergency_stopped = False

        # --- 基础数据缓存 ---
        self.tick_size = Decimal("0.01")
        self.min_qty_size = Decimal("0.0001")

        self._validate_adapters()
        asyncio.create_task(self._init_market_info())

    def _validate_adapters(self):
        if self.exchange_name not in self.adapters:
            logger.critical(f"❌ Exchange {self.exchange_name} not loaded.")
            self.is_active = False
        else:
            logger.info(f"✅ Strategy Ready: {self.exchange_name} | Mode: {self.grid_mode}")
            self.strategy_expiry_ts = time.time() - 1

    async def _init_market_info(self):
        """异步获取市场精度信息，避免硬编码"""
        retry = 0
        while retry < 5:
            try:
                adapter = self.adapters[self.exchange_name]
                # 尝试从 Adapter 的 map 中读取
                if hasattr(adapter, 'contract_map'):
                    key = f"{self.symbol}-USDT"  # 假设标准格式
                    # 适配不同交易所的 key 格式
                    if key not in adapter.contract_map:
                        key = self.symbol

                    info = adapter.contract_map.get(key)
                    if info:
                        self.tick_size = Decimal(str(info.get('tick_size', '0.01')))
                        self.min_qty_size = Decimal(str(info.get('min_size', '0.0001')))
                        logger.info(f"📏 Precision Updated: Tick={self.tick_size}, MinSize={self.min_qty_size}")
                        return
            except Exception:
                pass
            retry += 1
            await asyncio.sleep(2)
        logger.warning("⚠️ Failed to fetch market info, using defaults.")

    async def on_tick(self, event: dict):
        if not self.is_active or self.emergency_stopped:
            return

        evt_type = event.get('type')

        if evt_type == 'trade':
            await self._process_trade_fill(event)
            return

        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            current_price = (event['bid'] + event['ask']) / 2
            if current_price <= 0: return

            # 1. 毫秒级本地熔断检查 (最高优先级)
            if await self._check_circuit_breaker(current_price):
                return

            # 2. 只有在非更新状态下才进行 AI 调度检查
            if not self.is_updating:
                now = time.time()
                if (now >= self.strategy_expiry_ts) or (now >= self.next_check_ts):
                    asyncio.create_task(self._perform_ai_consultation(current_price))

                # 3. 初始化部署
                elif not self.initialized and self.grid_levels:
                    asyncio.create_task(self._reconcile_orders(current_price))

    async def _check_circuit_breaker(self, current_price: float) -> bool:
        """本地风控熔断"""
        if current_price < self.lower_price * (1 - self.stop_loss_pct):
            logger.critical(f"🚨 PRICE DROP ALERT: {current_price} < {self.lower_price * (1 - self.stop_loss_pct)}")
            self.emergency_stopped = True
            self.is_active = False
            asyncio.create_task(self._emergency_shutdown())
            return True
        return False

    async def _emergency_shutdown(self):
        """紧急撤单"""
        logger.critical("🛑 EXECUTING EMERGENCY SHUTDOWN")
        adapter = self.adapters[self.exchange_name]
        oids = list(self.active_orders.keys())
        tasks = [adapter.cancel_order(oid, symbol=self.symbol) for oid in oids]
        await asyncio.gather(*tasks, return_exceptions=True)
        async with self.state_lock:
            self.active_orders.clear()

    async def _process_trade_fill(self, trade: dict):
        """处理成交：增量补单"""
        if trade['exchange'] != self.exchange_name: return

        # 兼容不同 Adapter 的 ID 字段
        order_id = str(trade.get('order_id') or trade.get('client_order_id', ''))

        # 快速检查，避免获取锁
        if order_id not in self.active_orders: return

        async with self.state_lock:
            # 双重检查
            if order_id not in self.active_orders: return
            filled_order = self.active_orders.pop(order_id)

        side = filled_order['side']
        price = filled_order['price']

        logger.info(f"⚡️ Filled: {side} @ {price} | ID: {order_id}")

        # 触发一次轻量级对齐，而不是单点补单，确保整体网格结构的完整性
        # 如果正在进行 AI 更新，则推迟对齐，依靠 AI 更新完成后的重建
        if not self.is_updating:
            # 这里不直接 await，而是生成 task 防止阻塞 tick 流
            # 获取最新价格作为对齐参考
            current_price = price
            asyncio.create_task(self._reconcile_orders(current_price))

    async def _perform_ai_consultation(self, current_price: float):
        if self.is_updating: return
        self.is_updating = True

        try:
            logger.info(f"🧠 AI Consultation triggered @ {current_price:.2f}")

            # 构建上下文
            context = {
                "upper": self.upper_price,
                "lower": self.lower_price,
                "count": self.grid_count,
                "mode": self.grid_mode
            }

            advice = await fetch_grid_advice(self.symbol, current_price, context)

            # 默认维持现状
            action = "CONTINUE"
            duration = 4.0

            if advice:
                action = advice.get("action", "CONTINUE").upper()
                duration = float(advice.get("duration_hours", 4.0))

                if action == "UPDATE":
                    raw_count = int(advice.get('grid_count', self.grid_count))
                    new_upper = float(advice.get('upper_price', self.upper_price))
                    new_lower = float(advice.get('lower_price', self.lower_price))

                    # 验证 AI 返回参数的安全性
                    if new_upper > new_lower and raw_count > 1:
                        self.upper_price = new_upper
                        self.lower_price = new_lower
                        self.grid_count = raw_count
                        # 重算网格参数
                        self._calculate_grid_params(current_price)
                        # 执行增量对齐
                        await self._reconcile_orders(current_price)
                    else:
                        logger.warning(f"⚠️ Invalid AI params: {advice}")

            self.next_check_ts = time.time() + min(duration * 3600, self.max_check_interval)

        except Exception as e:
            logger.error(f"❌ AI Loop Error: {e}", exc_info=True)
            self.next_check_ts = time.time() + 300
        finally:
            self.is_updating = False
            self.initialized = True

    def _calculate_grid_params(self, current_price: float):
        """计算网格价格层级与下单数量"""
        total_equity = self.principal * self.leverage

        # 1. 资金风控：计算最大允许格数
        # 预留 5% buffer 防止价格波动导致 notional 不足
        safe_min_notional = self.min_order_notional * 1.05
        max_grids = int(total_equity / safe_min_notional)

        if self.grid_count > max_grids:
            logger.warning(f"⚠️ Adjusted Grid Count {self.grid_count} -> {max_grids} (Insufficient Funds)")
            self.grid_count = max_grids if max_grids > 1 else 2

        # 2. 计算每格下单量
        per_grid_equity = total_equity / self.grid_count
        raw_qty = per_grid_equity / current_price

        # 数量精度对齐
        d_qty = Decimal(str(raw_qty))
        self.quantity_per_grid = float((d_qty / self.min_qty_size).to_integral_value(ROUND_FLOOR) * self.min_qty_size)

        # 3. 计算网格价格点 (Arithmetic vs Geometric)
        self.grid_levels = []
        d_upper = Decimal(str(self.upper_price))
        d_lower = Decimal(str(self.lower_price))

        try:
            if self.grid_mode == 'GEOMETRIC':
                # 等比数列: r = (max/min)^(1/(n-1))
                ratio = math.pow(float(d_upper / d_lower), 1 / (self.grid_count - 1))
                d_ratio = Decimal(str(ratio))
                for i in range(self.grid_count):
                    # p_i = lower * r^i
                    p = float(d_lower * (d_ratio ** i))
                    self.grid_levels.append(self._quantize_price(p))
            else:
                # 等差数列
                step = (d_upper - d_lower) / Decimal(self.grid_count - 1)
                for i in range(self.grid_count):
                    p = float(d_lower + (step * i))
                    self.grid_levels.append(self._quantize_price(p))

        except Exception as e:
            logger.error(f"❌ Grid Math Error: {e}")
            self.grid_levels = []

    def _quantize_price(self, price: float) -> float:
        d_p = Decimal(str(price))
        return float((d_p / self.tick_size).to_integral_value(ROUND_CEILING) * self.tick_size)

    async def _reconcile_orders(self, current_price: float):
        """
        核心逻辑：增量对齐 (Diff Update)
        计算由于价格变动或策略更新，哪些单子需要撤，哪些需要挂
        """
        if not self.grid_levels or self.quantity_per_grid <= 0: return

        # 1. 计算目标状态
        # 根据当前价格，决定每个网格层级是挂 BUY 还是 SELL
        target_orders: Set[Tuple[str, float]] = set()  # (side, price)

        for level_price in self.grid_levels:
            # 留出 1.5 倍 tick_size 的缓冲区，避免在现价附近反复摩擦
            buffer = float(self.tick_size) * 1.5

            if level_price < (current_price - buffer):
                target_orders.add(('BUY', level_price))
            elif level_price > (current_price + buffer):
                target_orders.add(('SELL', level_price))

        # 2. 获取当前实际状态快照
        async with self.state_lock:
            current_active_snapshot = list(self.active_orders.items())  # List[(oid, info)]

        # 3. 找出差异
        to_cancel_ids = []
        # 将目标集合转换为易于查找的结构: dict[price_str] -> side
        # 使用 string key 避免 float 精度问题
        target_map = {f"{p:.6f}": s for s, p in target_orders}

        # A. 检查现有订单：保留有效的，标记无效的
        matched_target_prices = set()

        for oid, info in current_active_snapshot:
            p_str = f"{info['price']:.6f}"
            side = info['side']

            if p_str in target_map and target_map[p_str] == side:
                # 该订单仍然有效，无需变动
                matched_target_prices.add(p_str)
            else:
                # 该订单已不在计划中，或者方向错了
                to_cancel_ids.append(oid)

        # B. 确定需要新挂的订单
        to_place_specs = []  # (side, price)
        for s, p in target_orders:
            p_str = f"{p:.6f}"
            if p_str not in matched_target_prices:
                to_place_specs.append((s, p))

        if not to_cancel_ids and not to_place_specs:
            return

        logger.info(f"🔄 Reconcile: Cancel {len(to_cancel_ids)} | Place {len(to_place_specs)}")

        adapter = self.adapters[self.exchange_name]

        # 4. 执行撤单 (并行)
        if to_cancel_ids:
            # 分批撤单防止请求风暴
            chunk_size = 10
            for i in range(0, len(to_cancel_ids), chunk_size):
                chunk = to_cancel_ids[i:i + chunk_size]
                await asyncio.gather(
                    *[adapter.cancel_order(oid, symbol=self.symbol) for oid in chunk],
                    return_exceptions=True
                )

            # 无论 API 返回成功与否，逻辑上我们认为这些 ID 已不再活跃（防止状态不一致）
            async with self.state_lock:
                for oid in to_cancel_ids:
                    self.active_orders.pop(oid, None)

        # 5. 执行挂单 (并行)
        # 信号量控制并发数
        sem = asyncio.Semaphore(10)

        async def _place_wrapper(side, price):
            async with sem:
                try:
                    # Post Only 确保我们是 Maker
                    oid = await adapter.create_order(
                        self.symbol, side, self.quantity_per_grid, price,
                        params={'post_only': True}
                    )
                    if oid:
                        async with self.state_lock:
                            self.active_orders[oid] = {'side': side, 'price': price}
                except Exception as e:
                    # 忽略 Post Only 拒绝错误，这意味着价格已越过
                    if "Post-Only" not in str(e) and "post only" not in str(e).lower():
                        logger.error(f"Order fail {side}@{price}: {e}")

        if to_place_specs:
            await asyncio.gather(
                *[_place_wrapper(s, p) for s, p in to_place_specs],
                return_exceptions=True
            )