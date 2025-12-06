import asyncio
import logging
import time
from typing import Dict, Any, Set
from app.config import Config

logger = logging.getLogger("GL_Farm")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_v4_AutoFuse"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # --- 状态管理 ---
        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}

        # 繁忙状态锁 (One-Shot 模式)
        self.busy_symbols: Set[str] = set()

        # 风控：改单时间戳
        self.last_quote_time: Dict[str, float] = {}
        self.QUOTE_INTERVAL = 2.0

        # 🔥 核心风控：连续失败熔断机制
        self.consecutive_failures = 0
        self.max_failures = Config.MAX_CONSECUTIVE_FAILURES
        self.is_broken = False  # 熔断标志位

        # 异步锁
        self.hedge_lock = asyncio.Lock()
        self.farm_side = Config.FARM_SIDE

        logger.info(f"🛡️ 自动熔断策略 v4 已加载")
        logger.info(f"   - 熔断规则: 连续 {self.max_failures} 次对冲失败即停止")
        logger.info(f"   - 队列策略: 自动丢弃过期数据")
        logger.info(f"   - 交易方向: {self.farm_side}")

    async def on_tick(self, event: dict):
        # 🔥 全局熔断检查
        if self.is_broken:
            if int(time.time()) % 10 == 0:  # 每10秒打印一次提示，避免刷屏
                logger.error(
                    f"🛑 [系统已熔断] 连续失败次数过多 ({self.consecutive_failures})，策略已停止运行。请检查日志并重启。")
                await asyncio.sleep(1)
            return

        event_type = event.get('type', 'tick')

        if event_type == 'tick':
            await self._process_tick(event)
        elif event_type == 'trade':
            await self._process_trade_fill(event)

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if symbol in self.busy_symbols: return

        if 'Lighter' not in self.tickers[symbol] or 'GRVT' not in self.tickers[symbol]:
            return

        await self._manage_maker_orders(symbol)

    async def _manage_maker_orders(self, symbol: str):
        if symbol in self.busy_symbols: return

        now = time.time()
        if now - self.last_quote_time.get(symbol, 0) < self.QUOTE_INTERVAL:
            return

        lighter_book = self.tickers[symbol]['Lighter']
        qty = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # 计算价格
        target_price = 0.0
        ref_price = 0.0

        if self.farm_side == 'BUY':
            if lighter_book['bid'] <= 0: return
            ref_price = lighter_book['bid']
            target_price = ref_price * (1 + Config.MAX_SLIPPAGE_TOLERANCE)
            if (lighter_book['ask'] - lighter_book['bid']) / lighter_book['bid'] > 0.005: return
        else:
            if lighter_book['ask'] <= 0: return
            ref_price = lighter_book['ask']
            target_price = ref_price * (1 - Config.MAX_SLIPPAGE_TOLERANCE)
            if (lighter_book['ask'] - lighter_book['bid']) / lighter_book['bid'] > 0.005: return

        target_price = float(f"{target_price:.2f}")

        # 挂单
        current_order_id = self.active_orders.get(symbol)

        if not current_order_id:
            logger.info(f"➕ [挂单] {symbol} GRVT {self.farm_side} @ {target_price}")

            if symbol in self.busy_symbols: return

            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT",
                side=self.farm_side,
                amount=qty,
                price=target_price,
                order_type="LIMIT"
            )

            if new_id:
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = target_price
                self.last_quote_time[symbol] = now
        else:
            # 如果下单失败，也强制冷却 2 秒，防止日志刷屏和死循环
            logger.warning(f"⚠️ 下单失败，强制冷却 {self.QUOTE_INTERVAL}s")
            self.last_quote_time[symbol] = now
            last_price = self.active_order_prices.get(symbol, 0)
            if last_price > 0:
                deviation = abs(target_price - last_price) / last_price
                if deviation > Config.REQUOTE_THRESHOLD:
                    self.last_quote_time[symbol] = now
                    pass

    async def _process_trade_fill(self, trade: dict):
        exchange = trade['exchange']
        symbol = trade['symbol']
        side = trade['side']
        size = trade['size']

        if exchange != 'GRVT': return

        # 锁定
        self.busy_symbols.add(symbol)

        hedge_side = 'SELL' if side.upper() == 'BUY' else 'BUY'
        symbol_pair = f"{symbol}-USDT"

        async with self.hedge_lock:
            logger.info(f"🚨 [成交触发] GRVT {side} {size} {symbol} -> 🔒 锁定状态")

            # 清理旧单 (Cancel Remaining)
            if symbol in self.active_orders:
                # order_id = self.active_orders[symbol]
                # await self.adapters['GRVT'].cancel_order(order_id)
                del self.active_orders[symbol]
                del self.active_order_prices[symbol]

            # --- 对冲逻辑 ---
            logger.info(f"🌊 [开始对冲] Lighter {hedge_side} Market...")
            hedge_success = False
            for i in range(3):
                try:
                    await self.adapters['Lighter'].create_order(
                        symbol=symbol_pair,
                        side=hedge_side,
                        amount=size,
                        order_type="MARKET"
                    )
                    logger.info(f"✅ [对冲完成] Lighter Market {hedge_side}")
                    hedge_success = True
                    break
                except Exception as e:
                    logger.warning(f"⚠️ 对冲重试 {i + 1}/3: {e}")
                    await asyncio.sleep(0.5)

            # --- 结果判定与熔断计数 ---
            if hedge_success:
                # 🎉 成功：重置连续失败计数器
                if self.consecutive_failures > 0:
                    logger.info(f"✨ 连续失败计数器已重置 (之前失败: {self.consecutive_failures})")
                self.consecutive_failures = 0
            else:
                # ☠️ 失败：增加计数
                self.consecutive_failures += 1
                logger.error(f"❌ 对冲彻底失败！当前连续失败次数: {self.consecutive_failures}/{self.max_failures}")

                # 执行 Unwind (即使对冲失败也要尝试平仓)
                try:
                    unwind_side = 'SELL' if side.upper() == 'BUY' else 'BUY'
                    await self.adapters['GRVT'].create_order(
                        symbol=symbol_pair, side=unwind_side, amount=size, order_type="MARKET"
                    )
                    logger.warning(f"🛡️ 回滚/强平 完成")
                except Exception as e:
                    logger.critical(f"💀 回滚失败: {e}")

                # 🔥 检查是否触发熔断
                if self.consecutive_failures >= self.max_failures:
                    self.is_broken = True
                    logger.critical(f"🛑 [触发熔断] 连续失败次数达到阈值 ({self.max_failures})！系统停止开新单！")

            # --- 冷却与解锁 ---
            logger.info(f"⏳ [冷却中] ...")
            await asyncio.sleep(Config.TRADE_COOLDOWN)

            if symbol in self.busy_symbols:
                self.busy_symbols.remove(symbol)
            logger.info(f"🔓 [解锁] {symbol}")