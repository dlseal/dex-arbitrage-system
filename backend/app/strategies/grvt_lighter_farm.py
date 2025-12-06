import asyncio
import logging
import time
from typing import Dict, Any, Optional
from app.config import Config

logger = logging.getLogger("GL_Farm")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_Farm_v1"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 记录我们在 GRVT 的挂单状态
        self.active_orders: Dict[str, str] = {}  # {symbol: order_id}
        self.active_order_prices: Dict[str, float] = {}  # {symbol: price}

        # 1. 风险控制：改单时间戳 (用于防抖动)
        self.last_quote_time: Dict[str, float] = {}
        self.QUOTE_INTERVAL = 2.0  # 最小改单间隔 (秒)

        # 2. 风险控制：资金枯竭保护 (临时方案)
        self.trade_counter = 0
        self.MAX_TRADES_PER_SESSION = 50  # 运行 50 单后自动停止，防止单边资金耗尽

        # 锁，防止并发对冲
        self.hedge_lock = asyncio.Lock()

        logger.info(f"🚜 刷量策略已加载: GRVT(Maker) <-> Lighter(Taker)")
        logger.info(f"   - 单笔数量: {Config.TRADE_QUANTITIES}")
        logger.info(f"   - 滑点容忍: {Config.MAX_SLIPPAGE_TOLERANCE}")
        logger.info(f"   - 深度检查: 开启")

    async def on_tick(self, event: dict):
        """主入口"""
        # 熔断检查
        if self.trade_counter >= self.MAX_TRADES_PER_SESSION:
            if self.trade_counter == self.MAX_TRADES_PER_SESSION:
                logger.warning(
                    f"🛑 [熔断] 已达到单次运行最大交易次数 ({self.MAX_TRADES_PER_SESSION})。请检查余额并重启程序。")
                self.trade_counter += 1  # 防止重复打印
            return

        event_type = event.get('type', 'tick')

        if event_type == 'tick':
            await self._process_tick(event)
        elif event_type == 'trade':
            await self._process_trade_fill(event)

    async def _process_tick(self, tick: dict):
        """处理行情更新"""
        symbol = tick['symbol']
        exchange = tick['exchange']

        # 更新行情缓存
        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        # 只有当两边都有行情时才计算
        if 'Lighter' not in self.tickers[symbol] or 'GRVT' not in self.tickers[symbol]:
            return

        # 执行挂单逻辑
        await self._manage_maker_orders(symbol)

    async def _manage_maker_orders(self, symbol: str):
        """核心挂单逻辑 (带防抖和深度检查)"""

        # --- 风控 1: 防抖动 (Rate Limit Protection) ---
        now = time.time()
        last_time = self.last_quote_time.get(symbol, 0)
        if now - last_time < self.QUOTE_INTERVAL:
            return  # 还没到改单时间，跳过

        lighter_book = self.tickers[symbol]['Lighter']
        qty = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # --- 风控 2: Lighter 深度检查 (Depth Check) ---
        # 简单检查：虽然 Ticker 通常只给 Best Bid/Ask，但如果价格异常低，说明深度不够
        # 更严谨的做法需要 fetch_orderbook 获取 full depth。
        # 这里做一个简单的“价差保护”：如果 Lighter 的 Bid/Ask 价差超过 0.5%，说明流动性枯竭，不挂单
        spread_pct = (lighter_book['ask'] - lighter_book['bid']) / lighter_book['bid']
        if spread_pct > 0.005:  # 0.5%
            # logger.debug(f"⚠️ [深度不足] {symbol} Lighter 价差过大 ({spread_pct*100:.2f}%)，暂停挂单")
            return

        # 计算目标买单价格
        # 目标：GRVT_Bid = Lighter_Bid + Tolerance
        target_bid_price = lighter_book['bid'] * (1 + Config.MAX_SLIPPAGE_TOLERANCE)
        target_bid_price = float(f"{target_bid_price:.2f}")  # 简单精度处理

        current_order_id = self.active_orders.get(symbol)

        if not current_order_id:
            # --- 新挂单 ---
            logger.info(f"➕ [挂单] {symbol} GRVT Buy Limit @ {target_bid_price} (Ref Lighter: {lighter_book['bid']})")

            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT",
                side='BUY',
                amount=qty,
                price=target_bid_price,
                order_type="LIMIT"
            )

            if new_id:
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = target_bid_price
                self.last_quote_time[symbol] = now  # 更新时间戳

        else:
            # --- 检查重挂 (Re-quote) ---
            last_price = self.active_order_prices.get(symbol, 0)
            if last_price <= 0: return

            deviation = abs(target_bid_price - last_price) / last_price

            if deviation > Config.REQUOTE_THRESHOLD:
                # 价格偏离过大，这里为了简化，我们只做“记录”。
                # 生产环境应当：await cancel_order() -> await create_order()
                # 并在 active_orders 中更新 ID
                # logger.info(f"🔄 [需重挂] {symbol} 偏离 {deviation:.4f}")
                self.last_quote_time[symbol] = now  # 即使没动作，也更新时间防止日志刷屏
                pass

    async def _process_trade_fill(self, trade: dict):
        """处理成交：对冲 + 重试 + 回滚"""
        exchange = trade['exchange']
        symbol = trade['symbol']
        side = trade['side']
        size = trade['size']

        if exchange != 'GRVT': return

        hedge_side = 'SELL' if side.upper() == 'BUY' else 'BUY'
        symbol_pair = f"{symbol}-USDT"

        async with self.hedge_lock:
            logger.info(f"🚨 [成交触发] GRVT {side} {size} {symbol} @ {trade['price']} -> 正在 Lighter 对冲...")

            # 增加计数器
            self.trade_counter += 1

            # --- Phase 1: 尝试对冲 (Retry 3 times) ---
            hedge_success = False
            for i in range(3):
                try:
                    await self.adapters['Lighter'].create_order(
                        symbol=symbol_pair,
                        side=hedge_side,
                        amount=size,
                        order_type="MARKET"
                    )
                    logger.info(f"✅ [对冲完成] Lighter Market {hedge_side} (尝试第 {i + 1} 次成功)")
                    hedge_success = True
                    break
                except Exception as e:
                    logger.warning(f"⚠️ [对冲失败] 第 {i + 1} 次尝试 Lighter 报错: {e}")
                    await asyncio.sleep(0.5)

            # --- Phase 2: 回滚 (Unwind) ---
            if not hedge_success:
                logger.error(f"❌ [严重风险] Lighter 对冲彻底失败！正在尝试平掉 GRVT 仓位 (Unwind)...")
                try:
                    unwind_side = 'SELL' if side.upper() == 'BUY' else 'BUY'
                    # 市价强平 GRVT
                    await self.adapters['GRVT'].create_order(
                        symbol=symbol_pair,
                        side=unwind_side,
                        amount=size,
                        order_type="MARKET"
                    )
                    logger.warning(f"🛡️ [风控执行] GRVT 仓位已强平 (Unwind Done)。")
                except Exception as e:
                    logger.critical(f"💀 [灾难] GRVT 强平也失败了！请人工介入！错误: {e}")

            # 清理本地挂单记录 (因为已成交)
            if symbol in self.active_orders:
                del self.active_orders[symbol]
                del self.active_order_prices[symbol]