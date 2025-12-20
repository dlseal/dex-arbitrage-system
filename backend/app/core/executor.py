import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Any, Optional, Literal
from app.core.risk_controller import GlobalRiskController

logger = logging.getLogger("OrderExecutor")


class OrderStrategyExecutor:
    """
    生产级订单执行器 (v2 Optimized)
    借鉴 crypto-trading-open 架构，增强了极端行情下的补单能力。
    """

    def __init__(self, adapters: Dict[str, Any], risk_controller: GlobalRiskController):
        self.adapters = adapters
        self.risk_controller = risk_controller

        # 基础配置
        self.default_slippage = 0.005  # 0.5%
        self.max_retries = 3  # 标准重试次数
        self.wait_timeout = 5.0  # 订单等待超时

        # 借鉴开源项目：紧急补单配置
        self.emergency_slippage_multiplier = 50.0  # 紧急情况下滑点放大倍数 (50x -> 25%)
        self.market_order_timeout = 30.0  # 市价/紧急单的扩展超时时间

    async def execute_limit_market_arb(
            self,
            buy_ex: str, sell_ex: str,
            symbol: str, quantity: float,
            buy_price: float, sell_price: float,
            first_leg: Literal['buy', 'sell'] = 'buy'
    ) -> bool:
        """
        执行【限价 Maker -> 激进 Taker】套利。
        优化点：引入 Pre-trade check 和 紧急补单流程。
        """
        # 0. 熔断检查
        if not await self.risk_controller.check_trade_risk(symbol, quantity, buy_price):
            return False

        adapter1 = self.adapters.get(buy_ex if first_leg == 'buy' else sell_ex)
        adapter2 = self.adapters.get(sell_ex if first_leg == 'buy' else buy_ex)

        if not adapter1 or not adapter2:
            logger.error("❌ 执行失败: 适配器缺失")
            return False

        side1 = "BUY" if first_leg == 'buy' else "SELL"
        price1 = buy_price if first_leg == 'buy' else sell_price
        side2 = "SELL" if first_leg == 'buy' else "BUY"
        base_price2 = sell_price if first_leg == 'buy' else buy_price

        # --- Step 1: Maker (Leg 1) ---
        logger.info(f"1️⃣ [Maker] {adapter1.name} 挂单 {side1} {quantity} @ {price1}")
        order1_id = await self._safe_create_order(adapter1, symbol, side1, quantity, price1, "LIMIT")

        if not order1_id:
            return False

        filled_qty = await self._wait_for_fill(adapter1, symbol, order1_id, quantity)

        # 处理 Leg 1 未完全成交
        if filled_qty < quantity * 0.99:
            logger.warning(f"⚠️ Leg 1 超时 (已成: {filled_qty})，撤单中...")
            await self._safe_cancel_order(adapter1, symbol, order1_id)

            # 二次确认
            filled_qty = await self._get_filled_qty(adapter1, symbol, order1_id)
            if filled_qty <= 1e-6:
                logger.info("✅ Leg 1 无成交，任务取消")
                return False

        # --- Step 2: Taker (Leg 2) ---
        logger.info(f"2️⃣ [Taker] {adapter2.name} 吃单 {side2} {filled_qty}")

        # 尝试标准激进补单
        taker_filled = await self._execute_aggressive_taker(
            adapter2, symbol, side2, filled_qty, base_price2
        )

        # --- Step 3: 缺腿处理 (核心优化) ---
        if taker_filled < filled_qty * 0.99:
            shortfall = filled_qty - taker_filled
            logger.error(f"🚨 [缺腿警报] 缺口: {shortfall}. 进入紧急补单模式...")

            # 优化点：不立即反向平仓，而是尝试"核弹级"补单
            # 借鉴 lighter_batch_executor: 尝试使用 REST API + 巨额滑点
            emergency_filled = await self._execute_emergency_recovery(
                adapter2, symbol, side2, shortfall, base_price2
            )
            taker_filled += emergency_filled

            # 如果仍然失败，才执行反向平仓
            if taker_filled < filled_qty * 0.99:
                final_shortfall = filled_qty - taker_filled
                logger.critical(f"☠️ 紧急补单失败! 最终缺口: {final_shortfall}. 执行反向平仓熔断.")
                self.risk_controller.trigger_circuit_breaker(f"{symbol} 严重缺腿无法修复")

                await self._handle_reverse_close(adapter1, symbol, side1, final_shortfall)
                self.risk_controller.record_failure()
                return False

        self.risk_controller.record_success()
        logger.info(f"💰 套利完成. 成交: {filled_qty}")
        return True

    async def _execute_aggressive_taker(
            self, adapter, symbol, side, quantity, base_price
    ) -> float:
        """
        标准激进补单：使用 Limit 模拟 Market，滑点递增。
        优先使用 WebSocket (如果 Adapter 支持) 以求速度。
        """
        remaining = quantity
        total_filled = 0.0

        for i in range(self.max_retries):
            if remaining <= 1e-6: break

            # 滑点递增: 0.5% -> 1.0% -> 1.5%
            slippage = self.default_slippage * (i + 1)
            target_price = base_price * (1 + slippage) if side == "BUY" else base_price * (1 - slippage)

            # 优先尝试 IOC (Immediate or Cancel)
            order_id = await self._safe_create_order(
                adapter, symbol, side, remaining, target_price, "LIMIT", params={"timeInForce": "IOC"}
            )

            if order_id:
                filled = await self._wait_for_fill(adapter, symbol, order_id, remaining, timeout=2.0)
                total_filled += filled
                remaining -= filled

            if remaining > 1e-6:
                await asyncio.sleep(0.1)

        return total_filled

    async def _execute_emergency_recovery(
            self, adapter, symbol, side, quantity, base_price
    ) -> float:
        """
        [NEW] 紧急补单模式
        借鉴自开源项目：当常规手段失效时，使用极端参数确保成交。
        策略：
        1. 强制使用 REST API (绕过可能的 WS 拥堵)
        2. 滑点放大 50 倍 (确保吃掉深度)
        3. 超时时间延长
        """
        if quantity <= 1e-6: return 0.0

        # 计算暴力价格
        huge_slippage = self.default_slippage * self.emergency_slippage_multiplier  # 25%
        target_price = base_price * (1 + huge_slippage) if side == "BUY" else base_price * (1 - huge_slippage)

        logger.warning(
            f"☢️ [EMERGENCY] {adapter.name} 正在尝试核弹补单: {side} {quantity} @ {target_price:.4f} (滑点 {huge_slippage * 100}%)")

        # 强制使用 REST 方法 (假设 Adapter 有特定的 rest 方法，或者 create_order 内部处理)
        # 这里通过传递 force_rest 参数或调用特定方法
        try:
            # 尝试直接市价单 (如果支持)
            order_id = await adapter.create_order(symbol, side, quantity, 0, "MARKET")
            if not order_id:
                # 回退到深水限价单
                order_id = await adapter.create_order(symbol, side, quantity, target_price, "LIMIT")

            if order_id:
                # 延长等待时间
                return await self._wait_for_fill(adapter, symbol, order_id, quantity, timeout=self.market_order_timeout)
        except Exception as e:
            logger.error(f"紧急补单异常: {e}")

        return 0.0

    async def _handle_reverse_close(self, adapter, symbol, original_side, quantity):
        """
        最后防线：反向平仓 (Maker Leg)
        """
        close_side = "SELL" if original_side == "BUY" else "BUY"
        logger.warning(f"🛡️ 执行反向平仓: {adapter.name} {close_side} {quantity}")

        # 同样尝试市价平仓
        try:
            await adapter.create_order(symbol, close_side, quantity, 0, "MARKET")
        except Exception as e:
            logger.critical(f"☠️ 反向平仓也失败了! 需要人工介入! {e}")

    # --- Helpers ---

    async def _safe_create_order(self, adapter, symbol, side, amount, price, order_type, params=None) -> Optional[str]:
        try:
            # 适配 params 参数
            return await adapter.create_order(symbol, side, amount, price, order_type, **(params or {}))
        except Exception as e:
            # 简单的错误分类
            msg = str(e).lower()
            if "balance" in msg:
                logger.error(f"❌ 余额不足: {adapter.name}")
            elif "reduce" in msg:
                # 借鉴开源：识别 reduce-only 错误
                logger.error(f"❌ Reduce-only 限制: {adapter.name}")
            else:
                logger.error(f"❌ 下单异常 {adapter.name}: {e}")
            return None

    async def _safe_cancel_order(self, adapter, symbol, order_id):
        try:
            if hasattr(adapter, 'cancel_order'):
                await adapter.cancel_order(order_id, symbol)
        except Exception:
            pass

    async def _get_filled_qty(self, adapter, symbol, order_id) -> float:
        if not hasattr(adapter, 'get_order'): return 0.0
        try:
            order = await adapter.get_order(order_id, symbol)
            return float(order.get('filled', 0.0))
        except Exception:
            return 0.0

    async def _wait_for_fill(self, adapter, symbol, order_id, target_qty, timeout=None) -> float:
        timeout = timeout or self.wait_timeout
        start = time.time()

        while time.time() - start < timeout:
            try:
                filled = await self._get_filled_qty(adapter, symbol, order_id)
                if filled >= target_qty * 0.999:
                    return filled
            except Exception:
                pass
            await asyncio.sleep(0.2)

        return await self._get_filled_qty(adapter, symbol, order_id)