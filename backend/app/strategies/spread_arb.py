import logging
import time
from typing import Dict


# 配置日志颜色，方便在刷屏中一眼看到机会
class LogColors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'


logger = logging.getLogger("Strategy")


class SpreadArbitrageStrategy:
    def __init__(self):
        self.name = "SpreadArb_v1"
        # 缓存最新的行情快照 { 'Lighter': {'bid': 0, 'ask': 0}, 'GRVT': ... }
        self.tickers: Dict[str, Dict[str, float]] = {}

        # 核心参数
        self.spread_threshold = -1.0 #测试参数，无论价差多少，都会被判定为“有机会”
        # self.spread_threshold = 0.0005  # 触发阈值：0.05% (万分之五)
        self.min_profit_usdt = 5.0  # 最小预估利润 (USDT)

        # 简单的状态控制
        self.is_active = True

    async def on_tick(self, tick_data: dict):
        """
        核心回调：每当有新价格进来，都会触发一次计算
        """
        if not self.is_active:
            return

        exchange = tick_data['exchange']
        symbol = tick_data['symbol']  # 假设都是 "BTC" 或 "BTC-USDT"

        # 1. 更新本地缓存
        if exchange not in self.tickers:
            self.tickers[exchange] = {}

        self.tickers[exchange] = {
            'bid': tick_data['bid'],
            'ask': tick_data['ask'],
            'ts': time.time()
        }

        # 2. 只有当两个交易所的数据都准备好时，才开始比价
        if 'Lighter' in self.tickers and 'GRVT' in self.tickers:
            await self._calculate_spread()

    async def _calculate_spread(self):
        """
        计算价差逻辑
        """
        # 获取最新的报价
        lighter = self.tickers['Lighter']
        grvt = self.tickers['GRVT']

        # 场景 A: Lighter 价格高，GRVT 价格低 (Lighter 卖，GRVT 买)
        # 利润 = Lighter.bid - GRVT.ask
        diff_a = lighter['bid'] - grvt['ask']
        spread_a = diff_a / grvt['ask']

        # 场景 B: GRVT 价格高，Lighter 价格低 (GRVT 卖，Lighter 买)
        # 利润 = GRVT.bid - Lighter.ask
        diff_b = grvt['bid'] - lighter['ask']
        spread_b = diff_b / lighter['ask']

        # --- 机会检测 ---

        # 机会 A 检测
        if spread_a > self.spread_threshold:
            self._log_opportunity("A", "Sell Lighter / Buy GRVT", spread_a, lighter['bid'], grvt['ask'])

        # 机会 B 检测
        elif spread_b > self.spread_threshold:
            self._log_opportunity("B", "Sell GRVT / Buy Lighter", spread_b, grvt['bid'], lighter['ask'])

    def _log_opportunity(self, type_code, action, spread, sell_price, buy_price):
        """
        打印漂亮的机会日志
        """
        pct = spread * 100
        msg = (
            f"{LogColors.GREEN}💰 [套利机会 {type_code}] 利润率: {pct:.4f}% {LogColors.RESET}\n"
            f"   👉 动作: {action}\n"
            f"   📉 买入价: {buy_price} | 📈 卖出价: {sell_price} | 差价: {sell_price - buy_price:.2f}"
        )
        print(msg)
        # TODO: 这里将调用 self.execute_trade()