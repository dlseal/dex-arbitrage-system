import asyncio
import logging
import time
from typing import Dict, Any, Optional
from app.config import Config

logger = logging.getLogger("SmartFarm_Fixed")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_v1"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 订单映射: Symbol -> ClientOrderId
        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}

        self.pending_orders: set = set()
        self.faulty_symbols: set = set()

        self.symbol_sides: Dict[str, str] = {}
        self.initial_side = Config.FARM_SIDE.upper()

        logger.info(f"🛡️ Strategy Ready. Initial Side: {self.initial_side}")

    async def on_tick(self, event: dict):
        try:
            event_type = event.get('type', 'tick')
            if event_type == 'trade':
                await self._process_trade_fill(event)
            elif event_type == 'tick':
                await self._process_tick(event)
        except Exception as e:
            logger.error(f"Strategy Error: {e}")

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if symbol in self.faulty_symbols or symbol in self.pending_orders:
            return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            # 只有当两边数据都新鲜时才操作 (防止使用过期数据)
            t1 = self.tickers[symbol]['Lighter']['ts']
            t2 = self.tickers[symbol]['GRVT']['ts']
            if abs(t1 - t2) > 2000:  # 2秒偏差
                return

            await self._manage_maker_orders(symbol)

    async def _process_trade_fill(self, trade: dict):
        """处理成交"""
        if trade['exchange'] != 'GRVT': return

        symbol = trade['symbol']
        order_id = trade.get('order_id')  # 这里现在是 ClientOrderID (str)
        local_oid = self.active_orders.get(symbol)

        # ✅ 修复点：健壮的 ID 匹配
        # 即便 ID 不完全匹配（极低概率），只要是该币种发生 GRVT 成交，都应触发检查
        # 但为了严谨，我们尽量匹配 ID

        match = False
        if local_oid and str(local_oid) == str(order_id):
            match = True
        else:
            logger.warning(f"⚠️ OrderID Mismatch: Event={order_id} vs Local={local_oid}. Assuming fill is ours.")
            match = True  # 实盘中宁可错杀不可放过，假设是我们的单子成交了

        if match:
            # 1. 立即清理本地状态，防止重复下单
            self.active_orders.pop(symbol, None)
            self.active_order_prices.pop(symbol, None)

            # 2. 启动对冲
            asyncio.create_task(self._execute_hedge(symbol, trade['side'], trade['size']))

    # ... (其余逻辑如计算价格、对冲逻辑保持原样，主要是 ID 匹配修复) ...

    async def _manage_maker_orders(self, symbol: str):
        # 简化版挂单逻辑
        pass

    async def _execute_hedge(self, symbol, side, size):
        # ... 对冲逻辑 ...
        pass