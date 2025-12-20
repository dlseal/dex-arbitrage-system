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

        # --- 核心参数 ---
        self.upper_price = self.conf.upper_price
        self.lower_price = self.conf.lower_price
        self.grid_count = self.conf.grid_count

        # 支持 GEOMETRIC (等比) / ARITHMETIC (等差)
        self.grid_mode = getattr(self.conf, 'grid_mode', 'GEOMETRIC').upper()

        self.principal = self.conf.principal
        self.leverage = self.conf.leverage
        self.min_order_notional = self.conf.min_order_size

        # 风控参数
        self.stop_loss_pct = 0.05  # 5% 熔断阈值
        self.maker_fee_rate = 0.0003  # 假设 Maker 费率 0.03% (需根据实际配置)

        # --- 状态管理 ---
        # 内存中的订单状态: {order_id: {'side': str, 'price': float, 'ts': float}}
        self.active_orders: Dict[str, Dict] = {}
        self.grid_levels: List[float] = []
        self.quantity_per_grid = 0.0

        # --- 时间控制 ---
        self.strategy_expiry_ts = 0.0
        self.next_check_ts = 0.0
        self.max_check_interval = self.conf.max_check_interval

        # --- 并发控制 ---
        # state_lock 仅用于保护 self.active_orders 的读写，不包含网络 IO
        self.state_lock = asyncio.Lock()
        self.is_updating = False
        self.is_active = True
        self.initialized = False
        self.emergency_stopped = False

        # --- 基础数据缓存 ---
        self.tick_size = Decimal("0.01")
        self.min_qty_size = Decimal("0.0001")

        self._validate_adapters()

        # 启动初始化任务：获取精度 -> 清理旧单 -> 启动
        asyncio.create_task(self._bootstrap_strategy())

    def _validate_adapters(self):
        if self.exchange_name not in self.adapters:
            logger.critical(f"❌ [Init] Exchange {self.exchange_name} not loaded.")
            self.is_active = False
        else:
            logger.info(f"✅ [Init] Strategy Loaded: {self.exchange_name} | Mode: {self.grid_mode}")

    async def _bootstrap_strategy(self):
        """启动引导程序：确保环境安全后才开始交易"""
        await asyncio.sleep(2.0)  # 等待 WS 连接稳定

        # 1. 获取市场精度
        await self._init_market_info()

        # 2. 清理历史遗留订单 (防止重启失忆导致双重持仓)
        logger.info("🧹 [Bootstrap] Cleaning up existing orders...")
        await self._cancel_all_orders_force()

        # 3. 标记初始化完成，允许 on_tick 接管
        self.initialized = True
        self.strategy_expiry_ts = time.time() - 1  # 立即触发一次 AI
        logger.info("🚀 [Bootstrap] Ready to trade.")

    async def _init_market_info(self):
        retry = 0
        while retry < 5:
            try:
                adapter = self.adapters[self.exchange_name]
                if hasattr(adapter, 'contract_map'):
                    # 尝试模糊匹配 symbol
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
        logger.warning("⚠️ Using default precision settings.")

    async def on_tick(self, event: dict):
        if not self.is_active or self.emergency_stopped or not self.initialized:
            return

        evt_type = event.get('type')

        # 处理成交回报 (最高优先级)
        if evt_type == 'trade':
            await self._process_trade_fill(event)
            return

        # 处理行情 Tick
        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            current_price = (event['bid'] + event['ask']) / 2
            if current_price <= 0: return

            # 1. 毫秒级本地熔断检查 (Blocking)
            if await self._check_circuit_breaker(current_price):
                return

            # 2. AI 调度检查 (非阻塞)
            if not self.is_updating:
                now = time.time()

                # 触发 AI 更新条件：过期 或 到了检查时间
                if (now >= self.strategy_expiry_ts) or (now >= self.next_check_ts):
                    asyncio.create_task(self._perform_ai_consultation(current_price))

                # 3. 首次部署
                elif not self.grid_levels and not self.is_updating:
                    # 如果还没有网格，但已初始化，说明需要重新计算
                    pass  # 等待 AI 结果

    async def _check_circuit_breaker(self, current_price: float) -> bool:
        """本地风控熔断：防止 AI 失联时行情单边击穿"""
        # 下跌熔断
        if current_price < self.lower_price * (1 - self.stop_loss_pct):
            logger.critical(
                f"🚨 [CIRCUIT BREAKER] Price {current_price} < StopLoss {self.lower_price * (1 - self.stop_loss_pct)}")
            await self._emergency_shutdown()
            return True
        # 上涨熔断 (防止踏空太远导致回调时高位接盘)
        if current_price > self.upper_price * (1 + self.stop_loss_pct):
            logger.critical(f"🚨 [CIRCUIT BREAKER] Price {current_price} > MaxLimit {self.upper_price}")
            await self._emergency_shutdown()
            return True
        return False

    async def _emergency_shutdown(self):
        """紧急停止：撤单并锁定策略"""
        self.emergency_stopped = True
        self.is_active = False
        await self._cancel_all_orders_force()
        logger.critical("🛑 Strategy HALTED due to risk trigger.")

    async def _process_trade_fill(self, trade: dict):
        """处理成交：更新内存状态并触发增量对齐"""
        if trade['exchange'] != self.exchange_name: return

        # 兼容不同 Adapter 的 ID 字段
        order_id = str(trade.get('order_id') or trade.get('client_order_id', ''))

        async with self.state_lock:
            if order_id not in self.active_orders: return
            filled_order = self.active_orders.pop(order_id)

        side = filled_order['side']
        price = filled_order['price']
        logger.info(f"⚡️ [Filled] {side} {self.symbol} @ {price} | ID: {order_id}")

        # 如果不是正在重置网格，立即触发一次增量修复
        if not self.is_updating:
            # 这里的价格仅作为参考，reconcile 会用最新网格线计算
            asyncio.create_task(self._reconcile_orders(price))

    async def _perform_ai_consultation(self, current_price: float):
        """咨询 AI 并执行网格重置"""
        if self.is_updating: return
        self.is_updating = True

        try:
            logger.info(f"🧠 [AI] Consulting... Ref: {current_price:.2f}")

            context = {
                "upper": self.upper_price,
                "lower": self.lower_price,
                "count": self.grid_count,
                "mode": self.grid_mode
            }

            advice = await fetch_grid_advice(self.symbol, current_price, context)

            # 默认参数
            action = "CONTINUE"
            duration = 4.0

            if advice:
                action = advice.get("action", "CONTINUE").upper()
                duration = float(advice.get("duration_hours", 4.0))

                if action == "UPDATE":
                    raw_count = int(advice.get('grid_count', self.grid_count))
                    new_upper = float(advice.get('upper_price', self.upper_price))
                    new_lower = float(advice.get('lower_price', self.lower_price))

                    # 简单校验
                    if new_upper > new_lower and raw_count > 1:
                        self.upper_price = new_upper
                        self.lower_price = new_lower
                        self.grid_count = raw_count

                        logger.info(
                            f"🧠 [AI Update] Range: {self.lower_price}-{self.upper_price} | Grids: {self.grid_count}")

                        # 1. 重算网格
                        if self._calculate_grid_params(current_price):
                            # 2. 执行增量对齐 (Diff Update)
                            await self._reconcile_orders(current_price)
                    else:
                        logger.warning(f"⚠️ [AI] Invalid params received: {advice}")

            # 设置下次检查时间
            self.next_check_ts = time.time() + min(duration * 3600, self.max_check_interval)

        except Exception as e:
            logger.error(f"❌ [AI] Loop Error: {e}", exc_info=True)
            self.next_check_ts = time.time() + 300  # 出错后5分钟重试
        finally:
            self.is_updating = False

    def _calculate_grid_params(self, current_price: float) -> bool:
        """
        计算网格参数 (核心数学逻辑)
        返回: bool (是否计算成功且通过风控)
        """
        try:
            total_equity = self.principal * self.leverage

            # 1. 资金风控：计算最大允许格数
            # 预留 10% buffer 
            safe_min_notional = float(self.min_order_notional) * 1.1
            max_grids = int(total_equity / safe_min_notional)

            if self.grid_count > max_grids:
                logger.warning(f"⚠️ [Risk] Downgrading grids {self.grid_count} -> {max_grids} (Funds limit)")
                self.grid_count = max_grids

            if self.grid_count < 2:
                logger.error("❌ [Risk] Grid count too low (<2) after adjustment.")
                return False

            # 2. 计算每格下单量
            per_grid_equity = total_equity / self.grid_count
            raw_qty = per_grid_equity / current_price

            # 数量精度对齐
            d_qty = Decimal(str(raw_qty))
            self.quantity_per_grid = float(
                (d_qty / self.min_qty_size).to_integral_value(ROUND_FLOOR) * self.min_qty_size)

            if self.quantity_per_grid <= 0:
                logger.error("❌ [Risk] Quantity calculated to 0.")
                return False

            # 3. 计算网格价格点
            self.grid_levels = []
            d_upper = Decimal(str(self.upper_price))
            d_lower = Decimal(str(self.lower_price))

            # 费率盈亏平衡检查 (Break-even Check)
            avg_step_pct = (float(d_upper) - float(d_lower)) / self.grid_count / current_price
            min_profit_req = self.maker_fee_rate * 2.5  # 买卖两腿手续费 + 0.05%缓冲

            if avg_step_pct < min_profit_req:
                logger.warning(
                    f"⚠️ [Profit] Grid too dense ({avg_step_pct:.4%} < {min_profit_req:.4%}). May lose to fees!")
                # 在生产环境中，这里应该强制减少格数，或拒绝更新
                # self.grid_count = int(self.grid_count * 0.8) ... (可选优化)

            if self.grid_mode == 'GEOMETRIC':
                # 等比数列: r = (max/min)^(1/(n-1))
                ratio = math.pow(float(d_upper / d_lower), 1 / (self.grid_count - 1))
                d_ratio = Decimal(str(ratio))
                for i in range(self.grid_count):
                    p = float(d_lower * (d_ratio ** i))
                    self.grid_levels.append(self._quantize_price(p))
            else:
                # 等差数列
                step = (d_upper - d_lower) / Decimal(self.grid_count - 1)
                for i in range(self.grid_count):
                    p = float(d_lower + (step * i))
                    self.grid_levels.append(self._quantize_price(p))

            return True

        except Exception as e:
            logger.error(f"❌ Grid Calc Error: {e}", exc_info=True)
            return False

    def _quantize_price(self, price: float) -> float:
        """价格对齐到 TickSize"""
        d_p = Decimal(str(price))
        return float((d_p / self.tick_size).to_integral_value(ROUND_HALF_UP) * self.tick_size)

    async def _reconcile_orders(self, current_price: float):
        """
        核心逻辑：增量对齐 (Diff Update)
        """
        if not self.grid_levels or self.quantity_per_grid <= 0: return

        # 1. 计算目标状态 (Target State)
        # 规则: 价格之上挂 SELL, 价格之下挂 BUY
        # 缓冲区: 1.5 * tick_size，防止现价正好在网格线上反复触发挂单/撤单
        target_orders: Set[Tuple[str, str]] = set()  # Set of (side, price_str)
        buffer = float(self.tick_size) * 1.5

        for level_price in self.grid_levels:
            if level_price < (current_price - buffer):
                target_orders.add(('BUY', f"{level_price:.6f}"))
            elif level_price > (current_price + buffer):
                target_orders.add(('SELL', f"{level_price:.6f}"))

        # 2. 获取当前实际状态快照 (Current Snapshot)
        async with self.state_lock:
            current_active_snapshot = list(self.active_orders.items())

        # 3. 计算差异 (Diff)
        to_cancel_ids = []
        matched_target_sigs = set()  # 已有的目标签名 (side+price)

        # A. 遍历现有订单，决定去留
        for oid, info in current_active_snapshot:
            p_str = f"{info['price']:.6f}"
            sig = (info['side'], p_str)

            if sig in target_orders:
                matched_target_sigs.add(sig)  # 保留
            else:
                to_cancel_ids.append(oid)  # 废弃

        # B. 找出需要新挂的订单
        to_place_specs = []  # (side, price_float)
        for side, p_str in target_orders:
            if (side, p_str) not in matched_target_sigs:
                to_place_specs.append((side, float(p_str)))

        if not to_cancel_ids and not to_place_specs:
            return

        logger.info(f"🔄 [Reconcile] Cancel: {len(to_cancel_ids)} | Place: {len(to_place_specs)}")

        adapter = self.adapters[self.exchange_name]

        # 4. 执行撤单 (并行 & 分批)
        if to_cancel_ids:
            chunk_size = 10
            for i in range(0, len(to_cancel_ids), chunk_size):
                chunk = to_cancel_ids[i:i + chunk_size]
                await asyncio.gather(
                    *[adapter.cancel_order(oid, symbol=self.symbol) for oid in chunk],
                    return_exceptions=True
                )

            # 乐观更新内存：认为已撤销
            async with self.state_lock:
                for oid in to_cancel_ids:
                    self.active_orders.pop(oid, None)

        # 5. 执行挂单 (并行 & 信号量控制)
        if to_place_specs:
            sem = asyncio.Semaphore(10)  # 限制并发数为 10

            async def _place_wrapper(side, price):
                async with sem:
                    try:
                        # 必须使用 Post-Only
                        oid = await adapter.create_order(
                            self.symbol, side, self.quantity_per_grid, price,
                            params={'post_only': True}
                        )
                        if oid:
                            async with self.state_lock:
                                self.active_orders[oid] = {'side': side, 'price': price}
                    except Exception as e:
                        # 忽略 Post-Only 错误 (说明价格已穿越，无需挂单)
                        err_str = str(e).lower()
                        if "post" not in err_str and "maker" not in err_str:
                            logger.error(f"❌ Place failed {side}@{price}: {e}")

            await asyncio.gather(
                *[_place_wrapper(s, p) for s, p in to_place_specs],
                return_exceptions=True
            )

    async def _cancel_all_orders_force(self):
        """强制撤销该策略下的所有订单 (用于初始化或熔断)"""
        adapter = self.adapters[self.exchange_name]

        # 1. 尝试撤销内存中已知的
        async with self.state_lock:
            known_ids = list(self.active_orders.keys())
            self.active_orders.clear()

        if known_ids:
            tasks = [adapter.cancel_order(oid, symbol=self.symbol) for oid in known_ids]
            await asyncio.gather(*tasks, return_exceptions=True)

        # 2. (可选) 如果 Adapter 支持，调用撤销该交易对所有挂单的 API
        # 由于 BaseExchange 没定义 cancel_all，这里暂时只能做到这一步。
        # 生产环境建议在 Adapter 增加 cancel_all_orders(symbol) 方法。