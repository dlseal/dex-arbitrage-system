import asyncio
import logging
import time
from typing import Dict, Any, Optional
from app.config import Config


class LogColors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'


logger = logging.getLogger("Strategy")


class SpreadArbitrageStrategy:
    def __init__(self, adapters: Dict[str, Any], exchange_a: str, exchange_b: str):
        self.ex_a = exchange_a
        self.ex_b = exchange_b
        self.name = f"SpreadArb_{self.ex_a}_{self.ex_b}"
        self.adapters = adapters

        # 订单簿快照: {symbol: {exchange_name: {'bid': float, 'ask': float, 'ts': float}}}
        self.books: Dict[str, Dict[str, Dict]] = {}

        self.spread_threshold = Config.SPREAD_THRESHOLD
        self.trade_cooldown = Config.TRADE_COOLDOWN

        # 数据最大有效时间 (秒)，超过此时间的行情视为过期，不触发交易
        self.data_max_age = 5.0

        self.is_active = True
        self.is_trading = False

        self._validate_adapters()

    def _validate_adapters(self):
        """校验配置的交易所是否已加载"""
        missing = []
        if self.ex_a not in self.adapters: missing.append(self.ex_a)
        if self.ex_b not in self.adapters: missing.append(self.ex_b)

        if missing:
            logger.error(f"❌ [Strategy] 无法启动! 缺少 Adapter: {', '.join(missing)}")
            self.is_active = False
        else:
            logger.info(f"✅ [Strategy] {self.name} 已就绪 | 阈值: {self.spread_threshold * 100}%")

    async def on_tick(self, tick_data: dict):
        if not self.is_active: return

        exchange = tick_data.get('exchange')
        symbol = tick_data.get('symbol')

        # 过滤掉非目标交易所的数据
        if exchange not in [self.ex_a, self.ex_b]:
            return

        if symbol not in self.books:
            self.books[symbol] = {}

        self.books[symbol][exchange] = {
            'bid': float(tick_data.get('bid', 0)),
            'ask': float(tick_data.get('ask', 0)),
            'ts': time.time()
        }

        # 仅当两个交易所都有数据时才计算
        if self.ex_a in self.books[symbol] and self.ex_b in self.books[symbol]:
            await self._check_opportunity(symbol)

    async def _check_opportunity(self, symbol: str):
        if self.is_trading: return

        tick_a = self.books[symbol][self.ex_a]
        tick_b = self.books[symbol][self.ex_b]
        now = time.time()

        # 1. 数据时效性检查 (Optimization)
        age_a = now - tick_a['ts']
        age_b = now - tick_b['ts']
        if age_a > self.data_max_age or age_b > self.data_max_age:
            # 数据太旧，跳过，避免根据过期价格交易
            return

        # 2. 价格有效性检查 (Safety)
        if tick_a['bid'] <= 0 or tick_a['ask'] <= 0 or tick_b['bid'] <= 0 or tick_b['ask'] <= 0:
            return

        # 3. 计算双向价差
        # 路径 A: 卖出 A (Bid), 买入 B (Ask) -> 利润 = A.bid - B.ask
        diff_path_a = tick_a['bid'] - tick_b['ask']
        spread_path_a = diff_path_a / tick_b['ask']

        # 路径 B: 卖出 B (Bid), 买入 A (Ask) -> 利润 = B.bid - A.ask
        diff_path_b = tick_b['bid'] - tick_a['ask']
        spread_path_b = diff_path_b / tick_a['ask']

        # 4. 触发交易
        if spread_path_a > self.spread_threshold:
            await self._execute_arb(
                symbol=symbol,
                ex_sell=self.ex_a, ex_buy=self.ex_b,
                price_sell=tick_a['bid'], price_buy=tick_b['ask'],
                spread=spread_path_a, path_name=f"{self.ex_a}->{self.ex_b}"
            )

        elif spread_path_b > self.spread_threshold:
            await self._execute_arb(
                symbol=symbol,
                ex_sell=self.ex_b, ex_buy=self.ex_a,
                price_sell=tick_b['bid'], price_buy=tick_a['ask'],
                spread=spread_path_b, path_name=f"{self.ex_b}->{self.ex_a}"
            )

    async def _execute_arb(self, symbol, ex_sell, ex_buy, price_sell, price_buy, spread, path_name):
        if self.is_trading: return
        self.is_trading = True

        try:
            quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

            self._log_opportunity(symbol, path_name, spread, price_sell, price_buy, quantity)

            # --- 符号处理优化 ---
            # 直接传递 symbol (如 "BTC") 给 Adapter。
            # Adapter 内部应当负责将 "BTC" 转换为 "BTC-USDT" (如 GRVT) 或保留 "BTC" (如 Lighter)。
            # 这样避免了在 Strategy 层硬编码 "-USDT" 导致部分 API 报错。

            logger.info(f"🚀 [EXECUTE] {path_name} | Qty: {quantity}")

            # 并发下单
            task_sell = self.adapters[ex_sell].create_order(
                symbol=symbol, side="SELL", amount=quantity, price=price_sell, order_type="LIMIT"
            )
            task_buy = self.adapters[ex_buy].create_order(
                symbol=symbol, side="BUY", amount=quantity, price=price_buy, order_type="LIMIT"
            )

            results = await asyncio.gather(task_sell, task_buy, return_exceptions=True)

            # 结果处理与日志
            for ex_name, res in zip([ex_sell, ex_buy], results):
                if isinstance(res, Exception):
                    logger.error(f"❌ {ex_name} Order Failed: {res}")
                elif not res:
                    logger.error(f"❌ {ex_name} Order Failed (None returned)")
                else:
                    logger.info(f"✅ {ex_name} Order Placed ID: {res}")

        except Exception as e:
            logger.error(f"❌ Critical Trade Error: {e}", exc_info=True)
        finally:
            await asyncio.sleep(self.trade_cooldown)
            self.is_trading = False

    def _log_opportunity(self, symbol, path, spread, p_sell, p_buy, qty):
        pct = spread * 100
        msg = (
            f"\n{LogColors.GREEN}"
            f"💰 [{symbol} ARB FOUND] {path} | Profit: {pct:.4f}%\n"
            f"   📉 BUY  {p_buy:<10} on {path.split('->')[1]}\n"
            f"   📈 SELL {p_sell:<10} on {path.split('->')[0]}\n"
            f"   📦 Qty: {qty}"
            f"{LogColors.RESET}"
        )
        logger.info(msg)