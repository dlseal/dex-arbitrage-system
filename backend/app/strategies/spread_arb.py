import asyncio
import logging
import time
from typing import Dict, Any


class LogColors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'


logger = logging.getLogger("Strategy")


class SpreadArbitrageStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "SpreadArb_v1"
        self.adapters = adapters

        # 🟢 核心修改 1: 数据结构改为 { symbol: { exchange: { bid, ask... } } }
        # 例如: self.books['BTC'] = { 'Lighter': {...}, 'GRVT': {...} }
        self.books: Dict[str, Dict[str, Dict]] = {}

        # 阈值设置 (建议设为 0.002 即 0.2% 以覆盖手续费)
        self.spread_threshold = 0.002
        self.is_active = True
        self.is_trading = False

    async def on_tick(self, tick_data: dict):
        if not self.is_active: return

        exchange = tick_data['exchange']
        symbol = tick_data['symbol']

        # 1. 初始化该币种的存储结构
        if symbol not in self.books:
            self.books[symbol] = {}

        # 2. 更新该币种、该交易所的报价
        self.books[symbol][exchange] = {
            'bid': tick_data['bid'],
            'ask': tick_data['ask'],
            'ts': time.time()
        }

        # 3. 只有当该币种在两个交易所都有数据时，才计算价差
        if 'Lighter' in self.books[symbol] and 'GRVT' in self.books[symbol]:
            await self._calculate_spread(symbol)

    async def _calculate_spread(self, symbol: str):
        # 获取该币种在两边的报价
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

        # --- 机会检测 (带 Symbol 标识) ---
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
            # 构造完整的交易对名称 (注意适配器内部可能需要的格式)
            symbol_pair = f"{symbol}-USDT"

            logger.info(f"⚡️ [EXECUTE] {symbol} | {ex_sell} Sell / {ex_buy} Buy")

            # 测试阶段使用极小数量
            quantity = 0.01 if symbol == 'SOL' else 0.0001

            # 实际上这里应该根据交易所 API 调整 order_type，建议先打 LIMIT 做 Maker 或 Taker
            # 为了保证成交，这里演示用 LIMIT 价格但其实是吃单逻辑
            task_sell = self.adapters[ex_sell].create_order(
                symbol=symbol_pair, side=side_sell, amount=quantity, price=price_sell, order_type="LIMIT"
            )
            task_buy = self.adapters[ex_buy].create_order(
                symbol=symbol_pair, side=side_buy, amount=quantity, price=price_buy, order_type="LIMIT"
            )

            await asyncio.gather(task_sell, task_buy, return_exceptions=True)
            logger.info(f"✅ 交易指令已发送")

        except Exception as e:
            logger.error(f"❌ 交易失败: {e}")
        finally:
            await asyncio.sleep(2)  # 冷却防止重复下单
            self.is_trading = False