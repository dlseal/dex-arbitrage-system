import asyncio
import logging
import time
from typing import Dict, Any, Set, Optional
from app.config import Config

logger = logging.getLogger("GL_Farm")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_PingPong_v8"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        self.active_orders: Dict[str, str] = {}
        self.active_order_prices: Dict[str, float] = {}

        self.is_hedging: Dict[str, bool] = {}
        self.last_quote_time: Dict[str, float] = {}
        self.QUOTE_INTERVAL = Config.TRADE_COOLDOWN if hasattr(Config, 'TRADE_COOLDOWN') else 2.0

        # 🟢 新增：为每个币种单独管理方向
        # 初始方向读取配置文件，之后会自动翻转
        self.symbol_sides: Dict[str, str] = {}
        self.initial_side = Config.FARM_SIDE.upper()

        logger.info(f"🛡️ SafeFarm (Ping-Pong) 策略已启动 | 初始方向: {self.initial_side}")

    def _get_current_side(self, symbol: str) -> str:
        """获取当前币种的 Maker 方向"""
        return self.symbol_sides.get(symbol, self.initial_side)

    def _flip_side(self, symbol: str):
        """反转方向：BUY <-> SELL"""
        current = self._get_current_side(symbol)
        new_side = 'SELL' if current == 'BUY' else 'BUY'
        self.symbol_sides[symbol] = new_side
        logger.info(f"🔄 [方向翻转] {symbol}: {current} -> {new_side}")

    async def on_tick(self, event: dict):
        event_type = event.get('type', 'tick')

        if event_type == 'tick':
            await self._process_tick(event)
        elif event_type == 'trade':
            await self._process_trade_fill(event)

        # 定期主动查单
        if int(time.time()) % 5 == 0 and self.active_orders:
            for symbol, order_id in list(self.active_orders.items()):
                if not self.is_hedging.get(symbol, False):
                    asyncio.create_task(self._check_order_status_proactively(symbol, order_id))

    async def _check_order_status_proactively(self, symbol, order_id):
        try:
            order = await self.adapters['GRVT'].fetch_order(order_id)
            status = order.get('status') or order.get('state')

            if status in ['closed', 'filled', 'FILLED']:
                filled_size = float(order.get('amount', 0)) - float(order.get('remaining', 0))
                if filled_size > 0 and not self.is_hedging.get(symbol, False):
                    logger.warning(f"🔎 [主动查询] 订单 {order_id} 已成交，手动触发对冲")
                    # 使用当前记录的方向
                    current_side = self._get_current_side(symbol)
                    fake_event = {
                        'exchange': 'GRVT', 'symbol': symbol,
                        'side': current_side,
                        'size': filled_size,
                        'price': float(order.get('average', 0) or order.get('price', 0))
                    }
                    await self._process_trade_fill(fake_event)
            elif status in ['canceled', 'rejected', 'expired']:
                if symbol in self.active_orders and self.active_orders[symbol] == order_id:
                    del self.active_orders[symbol]
                    del self.active_order_prices[symbol]
        except Exception:
            pass

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        if self.is_hedging.get(symbol, False):
            return

        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            await self._manage_maker_orders(symbol)

    async def _manage_maker_orders(self, symbol: str):
        now = time.time()
        if now - self.last_quote_time.get(symbol, 0) < self.QUOTE_INTERVAL: return
        self.last_quote_time[symbol] = now

        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        # 获取当前应该交易的方向
        maker_side = self._get_current_side(symbol)

        # 计算目标价格 (传入方向)
        target_price = self._calculate_safe_maker_price(symbol, grvt_tick, lighter_tick, maker_side)

        if not target_price:
            return

        current_order_id = self.active_orders.get(symbol)
        current_price = self.active_order_prices.get(symbol)
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # 1. 挂新单
        if not current_order_id:
            logger.info(f"🆕 [挂单] {symbol} {maker_side} {quantity} @ {target_price}")
            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT",
                side=maker_side,
                amount=quantity,
                price=target_price,
                order_type="LIMIT"
            )
            # 兼容处理
            if new_id and new_id != "0x00":
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = target_price
            else:
                logger.warning(f"⚠️ [GRVT] 下单 ID 异常 ({new_id})")

        # 2. 改单检查
        else:
            price_diff_pct = abs(target_price - current_price) / current_price
            should_cancel = False

            if price_diff_pct > Config.REQUOTE_THRESHOLD:
                should_cancel = True

            # 抢占盘口检查
            if maker_side == 'BUY' and current_price < grvt_tick['bid']:
                should_cancel = True
            elif maker_side == 'SELL' and current_price > grvt_tick['ask']:
                should_cancel = True

            if should_cancel:
                logger.info(f"♻️ [调价] {symbol} {maker_side} 当前:{current_price} -> 目标:{target_price}")
                try:
                    await self.adapters['GRVT'].cancel_order(current_order_id)
                except Exception as e:
                    logger.error(f"撤单失败: {e}")

                if symbol in self.active_orders:
                    del self.active_orders[symbol]
                    del self.active_order_prices[symbol]

    def _calculate_safe_maker_price(self, symbol: str, grvt_tick: dict, lighter_tick: dict, side: str) -> Optional[
        float]:
        adapter = self.adapters['GRVT']
        contract_info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(contract_info['tick_size']) if contract_info else 0.01

        if side == 'BUY':
            raw_target = grvt_tick['ask'] - tick_size
            hedge_price = lighter_tick['bid']  # Lighter Sell price
            if hedge_price <= 0: return None
            # PnL: (Lighter卖 - GRVT买) / GRVT买
            expected_pnl_pct = (hedge_price - raw_target) / raw_target
        else:
            # SELL
            raw_target = grvt_tick['bid'] + tick_size
            hedge_price = lighter_tick['ask']  # Lighter Buy price
            if hedge_price <= 0: return None
            # PnL: (GRVT卖 - Lighter买) / Lighter买
            expected_pnl_pct = (raw_target - hedge_price) / hedge_price

        if expected_pnl_pct < Config.MAX_SLIPPAGE_TOLERANCE:
            # logger.debug(f"价格不安全 ({side}): PnL {expected_pnl_pct:.4%} < {Config.MAX_SLIPPAGE_TOLERANCE}")
            return None

        return raw_target

    async def _process_trade_fill(self, trade: dict):
        exchange = trade['exchange']
        symbol = trade['symbol']
        side = trade['side']  # GRVT 成交方向
        size = trade['size']

        if exchange != 'GRVT': return

        if self.is_hedging.get(symbol, False):
            return

        self.is_hedging[symbol] = True
        logger.info(f"🚨 [成交触发] GRVT {side} {size} -> 🔒 锁定策略，执行对冲")

        if symbol in self.active_orders:
            del self.active_orders[symbol]
            del self.active_order_prices[symbol]

        asyncio.create_task(self._execute_hedge_loop(symbol, side, size))

    async def _execute_hedge_loop(self, symbol, grvt_side, size):
        hedge_side = 'SELL' if grvt_side.upper() == 'BUY' else 'BUY'
        target_symbol = symbol

        retry_count = 0
        max_retries = 20
        success = False

        while retry_count < max_retries:
            try:
                if retry_count > Config.MAX_CONSECUTIVE_FAILURES:
                    logger.error("❌ 超过最大连续失败次数，暂停重试")

                lighter_tick = self.tickers.get(symbol, {}).get('Lighter')
                execution_price = 0.0

                if lighter_tick:
                    if hedge_side == 'BUY':
                        base_price = lighter_tick['ask'] if lighter_tick['ask'] > 0 else lighter_tick['bid']
                        execution_price = base_price * 1.05
                    else:
                        base_price = lighter_tick['bid'] if lighter_tick['bid'] > 0 else lighter_tick['ask']
                        execution_price = base_price * 0.95

                logger.info(f"🌊 [对冲] Lighter {hedge_side} {size} @ ~{execution_price:.2f} (第 {retry_count + 1} 次)")

                order_id = await self.adapters['Lighter'].create_order(
                    symbol=target_symbol,
                    side=hedge_side,
                    amount=size,
                    price=execution_price,
                    order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ [对冲成功] Lighter ID: {order_id}")
                    success = True
                    break
                else:
                    logger.warning("⚠️ Lighter 下单失败，重试中...")

            except Exception as e:
                logger.error(f"❌ 对冲异常: {e}")

            retry_count += 1
            await asyncio.sleep(0.5)

        if success:
            logger.info(f"🎉 [回合结束] {symbol} 对冲完成")

            # 🟢 核心改动：对冲成功后，翻转方向
            self._flip_side(symbol)

            await asyncio.sleep(Config.TRADE_COOLDOWN)
            self.is_hedging[symbol] = False
        else:
            logger.critical(f"💀 [严重] {symbol} 对冲失败，请人工介入！")