import asyncio
import logging
import time
from typing import Dict, Any
from app.config import Config


class LogColors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'


logger = logging.getLogger("Strategy")


class SpreadArbitrageStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "SpreadArb_v1"
        self.adapters = adapters
        self.books: Dict[str, Dict[str, Dict]] = {}
        self.spread_threshold = Config.SPREAD_THRESHOLD

        logger.info(f"策略配置加载: 阈值={self.spread_threshold}, 冷却={Config.TRADE_COOLDOWN}s")
        self.is_active = True
        self.is_trading = False

    async def on_tick(self, tick_data: dict):
        if not self.is_active: return

        exchange = tick_data['exchange']
        symbol = tick_data['symbol']

        if symbol not in self.books:
            self.books[symbol] = {}

        self.books[symbol][exchange] = {
            'bid': tick_data['bid'],
            'ask': tick_data['ask'],
            'ts': time.time()
        }

        if 'Lighter' in self.books[symbol] and 'GRVT' in self.books[symbol]:
            await self._calculate_spread(symbol)

    async def _calculate_spread(self, symbol: str):
        lighter = self.books[symbol]['Lighter']
        grvt = self.books[symbol]['GRVT']

        # 价格有效性检查 (防止 0 价格触发除零错误或假信号)
        if lighter['bid'] <= 0 or lighter['ask'] <= 0 or grvt['bid'] <= 0 or grvt['ask'] <= 0:
            return

        # 场景 A: Lighter 卖 (Bid), GRVT 买 (Ask)
        diff_a = lighter['bid'] - grvt['ask']
        spread_a = diff_a / grvt['ask']

        # 场景 B: GRVT 卖 (Bid), Lighter 买 (Ask)
        diff_b = grvt['bid'] - lighter['ask']
        spread_b = diff_b / lighter['ask']

        if spread_a > self.spread_threshold:
            self._log_opportunity(symbol, "A", "Sell Lighter / Buy GRVT", spread_a, lighter['bid'], grvt['ask'])
            await self.execute_trade(symbol, "Lighter", "GRVT", "SELL", "BUY", lighter['bid'], grvt['ask'])

        elif spread_b > self.spread_threshold:
            self._log_opportunity(symbol, "B", "Sell GRVT / Buy Lighter", spread_b, grvt['bid'], lighter['ask'])
            await self.execute_trade(symbol, "GRVT", "Lighter", "SELL", "BUY", grvt['bid'], lighter['ask'])

    def _log_opportunity(self, symbol, type_code, action, spread, sell_price, buy_price):
        pct = spread * 100
        msg = (
            f"{LogColors.GREEN}💰 [{symbol} 套利机会 {type_code}] 利润率: {pct:.4f}% {LogColors.RESET}\n"
            f"   👉 动作: {action}\n"
            f"   📉 买入价: {buy_price} | 📈 卖出价: {sell_price} | 差价: {sell_price - buy_price:.2f}"
        )
        print(msg)

    async def execute_trade(self, symbol, ex_sell, ex_buy, side_sell, side_buy, price_sell, price_buy):
        if self.is_trading: return
        self.is_trading = True

        try:
            symbol_pair = f"{symbol}-USDT"
            logger.info(f"⚡️ [EXECUTE] {symbol} | {ex_sell} Sell / {ex_buy} Buy")

            # 优先读取指定币种的配置，如果没有则读取 DEFAULT
            quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

            task_sell = self.adapters[ex_sell].create_order(
                symbol=symbol_pair, side=side_sell, amount=quantity, price=price_sell, order_type="LIMIT"
            )
            task_buy = self.adapters[ex_buy].create_order(
                symbol=symbol_pair, side=side_buy, amount=quantity, price=price_buy, order_type="LIMIT"
            )

            await asyncio.gather(task_sell, task_buy, return_exceptions=True)
            logger.info(f"✅ 交易指令已发送 (数量: {quantity})")

        except Exception as e:
            logger.error(f"❌ 交易失败: {e}")
        finally:
            await asyncio.sleep(Config.TRADE_COOLDOWN)
            self.is_trading = False