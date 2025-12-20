import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Any, List, Optional
from app.config import settings

# 假设你有 AI 客户端
from app.utils.llm_client import LLMClient

logger = logging.getLogger("AiGridStrategy")


class AiAdaptiveGridStrategy:
    def __init__(self, adapters: Dict[str, Any], symbol: str, exchange: str):
        self.adapters = adapters
        self.symbol = symbol
        self.exchange = exchange
        self.adapter = adapters.get(exchange)

        # --- 核心配置 ---
        self.grid_levels = 10  # 网格数量
        self.quantity_per_grid = 0.01  # 单格交易量
        self.check_interval = 60  # AI 重新预测的时间间隔 (秒)
        self.escape_timeout = 60  # 价格脱离网格多久后触发重置 (秒)
        self.max_drawdown_pct = 0.10  # 最大回撤阈值 (10%)

        # --- 运行时状态 ---
        self.grids: List[Dict] = []  # 存储网格订单 [{'id':.., 'price':.., 'side':..}]
        self.upper_price = 0.0
        self.lower_price = 0.0
        self.last_ai_update_ts = 0
        self.price_escape_start_ts = 0  # 价格脱离的开始时间
        self.initial_balance = 0.0  # 初始余额 (用于计算回撤)

        self.is_active = True
        self.llm_client = LLMClient()

    async def start(self):
        """策略启动"""
        if not self.adapter:
            logger.error(f"❌ Adapter not found for {self.exchange}")
            return

        # 1. 记录初始余额
        balance = await self.adapter.get_balance("USDT")  # 假设是 USDT 本位
        self.initial_balance = balance.get("total", 0)

        logger.info(f"🚀 AI Grid Started | Initial Balance: {self.initial_balance}")

        # 2. 首次建仓
        await self._update_grid_structure(force=True)

    async def on_tick(self, tick_data: dict):
        """核心 Tick 驱动循环"""
        if not self.is_active: return

        current_price = tick_data.get('last', 0)
        if current_price <= 0: return

        # 1. 安全检查：本金保护 (Capital Protection)
        await self._check_capital_protection(current_price)

        # 2. 状态检查：价格是否脱离网格 (Price Escape)
        self._check_price_escape(current_price)

        # 3. 定时检查：是否需要 AI 重新预测 (AI Rebalance)
        if time.time() - self.last_ai_update_ts > self.check_interval:
            await self._update_grid_structure()

    def _check_price_escape(self, current_price):
        """
        [优化点1] 价格脱离监控
        如果价格跑出 [lower_price, upper_price] 区间，并持续了一段时间，
        说明趋势已经形成，老网格失效，需要跟随趋势重置。
        """
        is_escaped = current_price > self.upper_price or current_price < self.lower_price

        if is_escaped:
            if self.price_escape_start_ts == 0:
                self.price_escape_start_ts = time.time()
                logger.warning(f"⚠️ 价格脱离网格区间 [{self.lower_price}, {self.upper_price}]! 当前: {current_price}")

            # 检查脱离持续时间
            duration = time.time() - self.price_escape_start_ts
            if duration > self.escape_timeout:
                logger.info(f"🔄 价格脱离已持续 {duration}s，触发网格重置 (Trend Follow)...")
                asyncio.create_task(self._update_grid_structure(force=True))
                self.price_escape_start_ts = 0  # 重置计时器
        else:
            if self.price_escape_start_ts != 0:
                logger.info("✅ 价格回归网格区间，解除脱离警报。")
                self.price_escape_start_ts = 0

    async def _check_capital_protection(self, current_price):
        """
        [优化点2] 本金保护逻辑
        计算当前浮动盈亏，如果回撤过大，触发熔断。
        """
        # 这是一个简化的估算，实际可能需要 fetch_position 获取精确持仓
        # 假设我们持有一些 Base Asset
        # estimated_balance = usdt_balance + (base_asset_balance * current_price)
        # drawdown = (self.initial_balance - estimated_balance) / self.initial_balance

        # if drawdown > self.max_drawdown_pct:
        #     logger.critical(f"🛑 触发最大回撤保护! 当前回撤: {drawdown*100:.2f}%")
        #     await self._emergency_stop()
        pass

    async def _update_grid_structure(self, force=False):
        """
        请求 AI 获取新的预测区间，并重建网格
        """
        self.last_ai_update_ts = time.time()

        try:
            # 1. 获取市场数据快照
            # klines = await self.adapter.get_klines(self.symbol, "15m", limit=50)

            # 2. 请求 LLM 分析 (模拟返回)
            # prediction = await self.llm_client.predict_trend(klines)
            # 假设 AI 返回: center=1000, range=200 (即 900-1100), volatility='high'

            # 这里用简单逻辑模拟 AI 建议
            ticker = await self.adapter.fetch_ticker(self.symbol)
            price = ticker['last']

            # [优化点3] 动态网格间距：波动大间距大，波动小间距小
            # volatility_factor = 0.01 if ai_says_high_volatility else 0.005
            volatility_factor = 0.008

            new_upper = price * (1 + volatility_factor * 5)
            new_lower = price * (1 - volatility_factor * 5)

            # 只有当新区间和老区间差异很大时才调整，避免频繁产生手续费
            if not force and abs(new_upper - self.upper_price) / self.upper_price < 0.02:
                return

            logger.info(f"🤖 AI 更新网格: [{new_lower:.2f}, {new_upper:.2f}]")

            # 3. 取消所有挂单 (Reset)
            await self._cancel_all_orders()

            # 4. 重新布单
            await self._place_grid_orders(new_lower, new_upper, price)

            self.upper_price = new_upper
            self.lower_price = new_lower

        except Exception as e:
            logger.error(f"❌ Grid Update Failed: {e}")

    async def _place_grid_orders(self, lower, upper, current_price):
        """
        标准几何/等差网格布单
        """
        step = (upper - lower) / self.grid_levels

        tasks = []
        for i in range(self.grid_levels + 1):
            price_level = lower + (i * step)

            # 价格太近不挂单，避免立即成交变成 Taker
            if abs(price_level - current_price) / current_price < 0.001:
                continue

            side = "SELL" if price_level > current_price else "BUY"

            tasks.append(
                self.adapter.create_order(
                    symbol=self.symbol,
                    side=side,
                    price=price_level,
                    amount=self.quantity_per_grid,
                    order_type="LIMIT"
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"✅ 已重新布署 {len(results)} 个网格订单")

    async def _cancel_all_orders(self):
        """取消当前策略的所有挂单"""
        # 需要 Adapter 支持 cancel_all 或者维护 order_id list
        # await self.adapter.cancel_all_orders(self.symbol)
        pass

    async def _emergency_stop(self):
        """紧急停止：撤单 + (可选)平仓"""
        self.is_active = False
        await self._cancel_all_orders()
        # await self.adapter.close_position(self.symbol)
        logger.critical("🛑 策略已因风控终止")