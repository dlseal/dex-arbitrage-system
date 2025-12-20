import asyncio
import logging
import time
from typing import Dict, Any, Optional, Literal

logger = logging.getLogger("OrderExecutor")


class OrderStrategyExecutor:
    """
    生产级订单执行器
    职责：负责具体的套利执行流程，处理订单状态轮询、激进补单和缺腿熔断。
    """

    def __init__(self, adapters: Dict[str, Any]):
        self.adapters = adapters
        # 配置参数
        self.default_slippage = 0.005  # 基础滑点 0.5%
        self.max_retries = 3  # 补单最大重试次数
        self.wait_timeout = 10  # 订单等待超时时间(秒)
        self.poll_interval = 0.5  # 查单轮询间隔(秒)

    async def execute_limit_market_arb(
            self,
            buy_ex: str, sell_ex: str,
            symbol: str, quantity: float,
            buy_price: float, sell_price: float,
            first_leg: Literal['buy', 'sell'] = 'buy'
    ) -> bool:
        """
        执行【限价 Maker -> 激进 Taker】套利策略。
        返回 True 表示双边完全成交，False 表示失败（可能触发了熔断）。
        """
        # 1. 获取适配器
        adapter1 = self.adapters.get(buy_ex if first_leg == 'buy' else sell_ex)
        adapter2 = self.adapters.get(sell_ex if first_leg == 'buy' else buy_ex)

        if not adapter1 or not adapter2:
            logger.error(f"❌ 执行失败: 找不到交易所适配器 {buy_ex} 或 {sell_ex}")
            return False

        # 2. 准备参数
        # 第一腿 (Maker)
        side1 = "BUY" if first_leg == 'buy' else "SELL"
        price1 = buy_price if first_leg == 'buy' else sell_price

        # 第二腿 (Taker)
        side2 = "SELL" if first_leg == 'buy' else "BUY"
        base_price2 = sell_price if first_leg == 'buy' else buy_price

        # --- 步骤 1: 挂第一腿 (Maker) ---
        logger.info(f"1️⃣ [Maker] {adapter1.name} 挂单 {side1} {quantity} @ {price1}")
        order1_id = await self._safe_create_order(adapter1, symbol, side1, quantity, price1, "LIMIT")

        if not order1_id:
            logger.error(f"❌ {adapter1.name} 下单失败，套利中止")
            return False

        # --- 步骤 2: 等待第一腿成交 ---
        filled_qty = await self._wait_for_fill(adapter1, symbol, order1_id, quantity)

        # 处理未成交/部分成交情况
        if filled_qty < quantity * 0.99:  # 允许 1% 的精度误差
            logger.warning(f"⚠️ 第一腿超时或未完全成交 (已成: {filled_qty})，正在撤单...")
            await self._safe_cancel_order(adapter1, symbol, order1_id)

            # 二次确认：撤单期间可能发生了成交
            real_filled = await self._get_filled_qty(adapter1, symbol, order1_id)
            if real_filled > filled_qty:
                logger.info(f"ℹ️ 撤单期间新增成交: {real_filled - filled_qty}")
                filled_qty = real_filled

            if filled_qty <= 0:
                logger.info("✅ 第一腿无成交且已撤单，任务结束")
                return False

        logger.info(f"✅ 第一腿最终成交: {filled_qty}")

        # --- 步骤 3: 执行第二腿 (激进 Taker) ---
        # 注意：只对冲第一腿实际成交的数量
        logger.info(f"2️⃣ [Taker] {adapter2.name} 开始吃单 {side2} {filled_qty}...")

        taker_filled = await self._execute_aggressive_taker(
            adapter2, symbol, side2, filled_qty, base_price2
        )

        # --- 步骤 4: 缺腿检查与熔断 ---
        if taker_filled < filled_qty * 0.99:
            shortfall = filled_qty - taker_filled
            logger.critical(f"🚨 [严重风险] 出现缺腿! 缺口: {shortfall}. 启动反向平仓熔断...")

            # 熔断逻辑：在第一腿（成功的那个交易所）反向平掉多余的仓位
            await self._handle_shortfall(adapter1, symbol, side1, shortfall)
            return False

        logger.info(f"💰 套利圆满完成! 双边成交: {filled_qty}")
        return True

    async def _execute_aggressive_taker(
            self, adapter, symbol, side, quantity, base_price
    ) -> float:
        """
        激进补单逻辑：分批尝试，滑点递增，确保成交。
        """
        remaining = quantity
        total_filled = 0.0

        for i in range(self.max_retries):
            if remaining <= 1e-6: break

            # 动态滑点：第一次 0.5%，第二次 1%，第三次 2%
            slippage = self.default_slippage * (i + 1)

            if side == "BUY":
                target_price = base_price * (1 + slippage)
            else:
                target_price = base_price * (1 - slippage)

            logger.info(f"⚡ 补单尝试 {i + 1}/{self.max_retries}: {side} {remaining:.4f} @ {target_price:.4f}")

            # 强制使用 LIMIT 模拟 Market (DEX 最佳实践)
            # 对于 Lighter，这里发出的其实是 IOC (立即成交或取消) 的效果，如果 Adapter 支持 IOC 参数最好
            order_id = await self._safe_create_order(
                adapter, symbol, side, remaining, target_price, "LIMIT"
            )

            if order_id:
                # Taker 单等待时间较短 (3秒)
                filled = await self._wait_for_fill(adapter, symbol, order_id, remaining, timeout=3.0)
                total_filled += filled
                remaining -= filled

            if remaining > 1e-6:
                await asyncio.sleep(0.2)  # 短暂冷却

        return total_filled

    async def _handle_shortfall(self, adapter, symbol, original_side, quantity):
        """
        熔断处理：在原交易所反向平仓。
        例如：Leg1 买入 1.0 成功，Leg2 卖出失败。
        操作：在 Leg1 卖出 1.0 平仓。
        """
        close_side = "SELL" if original_side == "BUY" else "BUY"
        logger.warning(f"🛡️ 执行熔断平仓: {adapter.name} {close_side} {quantity}")

        # 使用极端的市价单或大滑点限价单进行平仓
        # 这里为了通用性，尝试发送市价单
        res = await self._safe_create_order(adapter, symbol, close_side, quantity, 0, "MARKET")

        if not res:
            logger.critical(f"☠️ 熔断平仓下单失败! 请立即人工介入处理 {adapter.name} 的敞口!")
        else:
            logger.info(f"✅ 熔断平仓指令已发送 (Order ID: {res})")

    # ------- 基础封装函数 -------

    async def _safe_create_order(self, adapter, symbol, side, amount, price, order_type) -> Optional[str]:
        try:
            return await adapter.create_order(symbol, side, amount, price, order_type)
        except Exception as e:
            logger.error(f"❌ 下单异常 {adapter.name}: {e}")
            return None

    async def _safe_cancel_order(self, adapter, symbol, order_id):
        try:
            if hasattr(adapter, 'cancel_order'):
                await adapter.cancel_order(order_id, symbol)
        except Exception as e:
            logger.error(f"❌ 撤单异常: {e}")

    async def _get_filled_qty(self, adapter, symbol, order_id) -> float:
        """单次查询成交量"""
        if not hasattr(adapter, 'get_order'): return 0.0
        try:
            order_data = await adapter.get_order(order_id, symbol)
            return float(order_data.get('filled', 0.0))
        except Exception:
            return 0.0

    async def _wait_for_fill(self, adapter, symbol, order_id, target_qty, timeout=None) -> float:
        """
        轮询查单，直到超时或完全成交
        """
        timeout = timeout or self.wait_timeout
        start_ts = time.time()

        if not hasattr(adapter, 'get_order'):
            logger.warning(f"⚠️ {adapter.name} 未实现 get_order，无法确认成交! 默认返回 0。")
            return 0.0

        last_filled = 0.0

        while time.time() - start_ts < timeout:
            try:
                order_data = await adapter.get_order(order_id, symbol)
                filled = float(order_data.get('filled', 0.0))
                status = order_data.get('status', 'UNKNOWN')

                last_filled = filled

                # 检查是否完全成交 (允许微小误差)
                if filled >= target_qty * 0.999:
                    return filled

                # 检查是否已结束 (CANCELED/FILLED/EXPIRED)
                # 注意：不同交易所状态码可能不同，这里假设通用状态
                if status in ['closed', 'canceled', 'expired', 'FILLED', 'CANCELED']:
                    return filled

            except Exception as e:
                logger.warning(f"查询订单出错: {e}")

            await asyncio.sleep(self.poll_interval)

        return last_filled