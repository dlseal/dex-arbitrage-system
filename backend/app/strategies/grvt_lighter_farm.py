import asyncio
import logging
import time
from typing import Dict, Any, Set
from app.config import Config

logger = logging.getLogger("GL_Farm")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_SafeFarm_v6"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}
        self.order_create_time: Dict[str, float] = {}

        # 🆕 状态锁：如果正在对冲，绝对不允许挂新单
        self.is_hedging: Dict[str, bool] = {}

        self.busy_symbols: Set[str] = set()
        self.last_quote_time: Dict[str, float] = {}
        self.QUOTE_INTERVAL = 0.5

        self.farm_side = Config.FARM_SIDE
        logger.info(f"🛡️ SafeFarm 策略 v6 已启动 | 方向: {self.farm_side}")

    async def on_tick(self, event: dict):
        event_type = event.get('type', 'tick')

        if event_type == 'tick':
            await self._process_tick(event)
        elif event_type == 'trade':
            await self._process_trade_fill(event)

        # 定期检查任务 (如主动查单)
        if int(time.time()) % 2 == 0 and self.active_orders:
            for symbol, order_id in list(self.active_orders.items()):
                if not self.is_hedging.get(symbol, False):
                    asyncio.create_task(self._check_order_status_proactively(symbol, order_id))

    async def _check_order_status_proactively(self, symbol, order_id):
        # ... (保留您上一版的主动查询逻辑) ...
        try:
            order = await self.adapters['GRVT'].rest_client.fetch_order(id=order_id)
            status = order.get('status') or order.get('state')
            if status in ['closed', 'filled', 'FILLED']:
                # 只有当还没进入对冲状态时，才触发
                if not self.is_hedging.get(symbol, False):
                    logger.warning(f"🔎 [主动查询] 订单 {order_id} 已成交，触发对冲")
                    fake_event = {
                        'exchange': 'GRVT', 'symbol': symbol,
                        'side': self.farm_side,
                        'size': float(order.get('amount', 0)),
                        'price': float(order.get('average', 0) or order.get('price', 0))
                    }
                    await self._process_trade_fill(fake_event)
        except:
            pass

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']
        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        # 🆕 关键检查：如果正在对冲中，禁止一切挂单更新！
        if self.is_hedging.get(symbol, False):
            return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            await self._manage_maker_orders(symbol)

    async def _manage_maker_orders(self, symbol: str):
        # ... (保留您上一版的 Smart Maker 挂单逻辑) ...
        # 唯一区别是：一旦 self.is_hedging 为 True，这里根本不会执行
        # 这就是参考脚本中 "Wait for fill" 的逻辑变体

        now = time.time()
        if now - self.last_quote_time.get(symbol, 0) < self.QUOTE_INTERVAL: return

        # (为了节省篇幅，这里假设您保留了 calculate_price 和 place_order 的代码)
        # 请务必确保这里使用的是上一版优化过的 "Smart Quote" 逻辑
        pass

    async def _process_trade_fill(self, trade: dict):
        exchange = trade['exchange']
        symbol = trade['symbol']
        side = trade['side']
        size = trade['size']

        if exchange != 'GRVT': return

        # 1. 立即锁定状态
        if self.is_hedging.get(symbol, False):
            logger.warning(f"⚠️ 重复收到成交推送 {symbol}, 忽略")
            return

        self.is_hedging[symbol] = True  # 🔒 上锁
        logger.info(f"🚨 [成交触发] GRVT {side} {size} -> 🔒 进入强对冲模式")

        # 2. 清理本地挂单记录
        if symbol in self.active_orders:
            del self.active_orders[symbol]
            del self.active_order_prices[symbol]

        # 3. 启动后台对冲任务 (不阻塞主循环)
        asyncio.create_task(self._execute_hedge_loop(symbol, side, size))

    async def _execute_hedge_loop(self, symbol, grvt_side, size):
        """
        参考脚本的精髓：死循环重试，直到对冲成功。
        这样保证了“回合”的完整性。
        """
        hedge_side = 'SELL' if grvt_side.upper() == 'BUY' else 'BUY'
        symbol_pair = f"{symbol}-USDT"

        retry_count = 0
        max_retries = 20  # 这种高频刷量，重试次数可以多一点

        success = False

        while retry_count < max_retries:
            try:
                logger.info(f"🌊 [对冲执行] Lighter {hedge_side} {size} (第 {retry_count + 1} 次)")

                # 使用 Market Order 保证成交 (Taker)
                # Lighter 的 Market Order 需要指定 avg_execution_price，适配器里已处理
                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol_pair,
                    side=hedge_side,
                    amount=size,
                    order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ [对冲成功] Lighter OrderID: {order_id}")
                    success = True
                    break
                else:
                    logger.warning("⚠️ Lighter 下单返回 None，准备重试...")

            except Exception as e:
                logger.error(f"❌ 对冲异常: {e}")

            retry_count += 1
            await asyncio.sleep(0.5)  # 失败稍微等一下

        if success:
            logger.info(f"🎉 [回合结束] {symbol} 平仓完成，解锁挂单。")
        else:
            logger.critical(f"💀 [严重故障] {symbol} 对冲彻底失败！请人工介入！(系统保持锁定状态)")
            # 这里可以选择不解锁 self.is_hedging，迫使策略停止，防止风险扩大
            return

            # 4. 只有对冲成功，才解锁，允许挂新单
        await asyncio.sleep(Config.TRADE_COOLDOWN)
        self.is_hedging[symbol] = False  # 🔓 解锁