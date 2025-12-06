import asyncio
import logging
import time
from typing import Dict, Any, Set, Optional
from app.config import Config

logger = logging.getLogger("GL_Farm")


class GrvtLighterFarmStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "GrvtLighter_SafeFarm_v7"
        self.adapters = adapters
        self.tickers: Dict[str, Dict[str, Dict]] = {}

        # 记录当前活跃的 Maker 订单 {symbol: order_id}
        self.active_orders: Dict[str, str] = {}
        # 记录当前挂单的价格 {symbol: price}
        self.active_order_prices: Dict[str, float] = {}

        # 🆕 状态锁：如果正在对冲，绝对不允许挂新单
        self.is_hedging: Dict[str, bool] = {}

        self.last_quote_time: Dict[str, float] = {}
        # 挂单间隔，避免 API 频率过高
        self.QUOTE_INTERVAL = Config.TRADE_COOLDOWN if hasattr(Config, 'TRADE_COOLDOWN') else 2.0

        # 从配置读取方向 (BUY/SELL)
        self.farm_side = Config.FARM_SIDE
        logger.info(f"🛡️ SafeFarm 策略已启动 | Maker方向: {self.farm_side}")

    async def on_tick(self, event: dict):
        event_type = event.get('type', 'tick')

        if event_type == 'tick':
            await self._process_tick(event)
        elif event_type == 'trade':
            await self._process_trade_fill(event)

        # 定期主动查单，防止 WS 推送丢失导致死锁
        if int(time.time()) % 5 == 0 and self.active_orders:
            for symbol, order_id in list(self.active_orders.items()):
                if not self.is_hedging.get(symbol, False):
                    asyncio.create_task(self._check_order_status_proactively(symbol, order_id))

    async def _check_order_status_proactively(self, symbol, order_id):
        """主动查询订单状态，作为 WS 的兜底"""
        try:
            # 调用 GRVT REST API 查单
            order = await self.adapters['GRVT'].rest_client.fetch_order(id=order_id)
            status = order.get('status') or order.get('state')

            # 如果订单已成交或部分成交，但本地没收到推送，则手动触发
            if status in ['closed', 'filled', 'FILLED']:
                filled_size = float(order.get('amount', 0)) - float(order.get('remaining', 0))
                if filled_size > 0 and not self.is_hedging.get(symbol, False):
                    logger.warning(f"🔎 [主动查询] 发现订单 {order_id} 已成交，手动触发对冲")
                    fake_event = {
                        'exchange': 'GRVT',
                        'symbol': symbol,
                        'side': self.farm_side,  # 假设全部成交
                        'size': filled_size,
                        'price': float(order.get('average', 0) or order.get('price', 0))
                    }
                    await self._process_trade_fill(fake_event)
            elif status in ['canceled', 'rejected']:
                # 如果订单已取消，清理本地状态
                if symbol in self.active_orders and self.active_orders[symbol] == order_id:
                    del self.active_orders[symbol]
                    del self.active_order_prices[symbol]
        except Exception as e:
            # logger.debug(f"查单失败: {e}")
            pass

    async def _process_tick(self, tick: dict):
        symbol = tick['symbol']
        exchange = tick['exchange']

        if symbol not in self.tickers: self.tickers[symbol] = {}
        self.tickers[symbol][exchange] = tick

        # 🔒 关键检查：如果正在对冲中，禁止一切挂单更新！
        if self.is_hedging.get(symbol, False):
            return

        # 只有当两边都有行情时才计算挂单
        if 'Lighter' in self.tickers[symbol] and 'GRVT' in self.tickers[symbol]:
            await self._manage_maker_orders(symbol)

    async def _manage_maker_orders(self, symbol: str):
        """
        核心挂单逻辑：
        参考 hedge_mode_grvt.py 的 place_grvt_post_only_order 逻辑。
        """
        now = time.time()
        # 频率控制
        if now - self.last_quote_time.get(symbol, 0) < self.QUOTE_INTERVAL:
            return
        self.last_quote_time[symbol] = now

        grvt_tick = self.tickers[symbol]['GRVT']
        lighter_tick = self.tickers[symbol]['Lighter']

        # 1. 计算目标挂单价格 (Smart Quote)
        target_price = self._calculate_safe_maker_price(symbol, grvt_tick, lighter_tick)

        # 如果计算返回 0 或 None，说明当前不适合挂单（滑点过大或行情异常）
        if not target_price:
            return

        current_order_id = self.active_orders.get(symbol)
        current_price = self.active_order_prices.get(symbol)

        # 获取配置的下单数量
        quantity = Config.TRADE_QUANTITIES.get(symbol, Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001))

        # 2. 如果没有活跃订单 -> 下新单
        if not current_order_id:
            logger.info(f"🆕 [挂单] {symbol} {self.farm_side} {quantity} @ {target_price}")
            new_id = await self.adapters['GRVT'].create_order(
                symbol=f"{symbol}-USDT",
                side=self.farm_side,
                amount=quantity,
                price=target_price,
                order_type="LIMIT"  # 适配器内部已默认 Post-Only
            )
            if new_id:
                self.active_orders[symbol] = new_id
                self.active_order_prices[symbol] = target_price

        # 3. 如果有活跃订单 -> 检查是否需要改单 (Requote)
        else:
            # 计算价格偏差
            price_diff_pct = abs(target_price - current_price) / current_price

            # 如果偏差超过阈值，或者由于方向原因不再是最佳价格（例如买单价格低于新的 Best Bid）
            should_cancel = False

            # 基础阈值检查
            if price_diff_pct > Config.REQUOTE_THRESHOLD:
                should_cancel = True

            # 进阶检查：参考 hedge_mode_grvt.py 的逻辑
            # 如果我们要买，且当前挂单价 < 市场最新买一价，说明我们无法成交，需要撤单重挂以抢占盘口
            # (前提是新的价格依然在安全滑点范围内，target_price 已经经过计算保证了这一点)
            if self.farm_side == 'BUY' and current_price < grvt_tick['bid']:
                should_cancel = True
            elif self.farm_side == 'SELL' and current_price > grvt_tick['ask']:
                should_cancel = True

            if should_cancel:
                logger.info(
                    f"♻️ [调价] {symbol} 当前:{current_price} -> 目标:{target_price} (Diff: {price_diff_pct:.4%})")
                try:
                    await self.adapters['GRVT'].rest_client.cancel_order(id=current_order_id)
                except Exception as e:
                    logger.error(f"撤单失败: {e}")

                # 清除本地状态，下次循环会重新下单
                del self.active_orders[symbol]
                del self.active_order_prices[symbol]

    def _calculate_safe_maker_price(self, symbol: str, grvt_tick: dict, lighter_tick: dict) -> Optional[float]:
        """
        计算安全的 Maker 价格。
        逻辑：
        1. 尝试排在 GRVT 盘口第一位 (Best Bid + Tick 或 Best Ask - Tick)。
        2. 检查该价格相对于 Lighter 对冲价格的亏损是否在允许范围内 (MAX_SLIPPAGE_TOLERANCE)。
        """
        # 获取最小变动单位
        adapter = self.adapters['GRVT']
        contract_info = adapter.contract_map.get(f"{symbol}-USDT")
        tick_size = float(contract_info['tick_size']) if contract_info else 0.01

        # 1. 初步定格：压盘口
        if self.farm_side == 'BUY':
            # 买单：挂在买一价上面一格，或者如果买一价已经是我们自己，就维持
            # 简单策略：挂在 GRVT Best Ask - Tick (尝试成交) 或者 GRVT Best Bid + Tick (尝试排队)
            # 为了刷量，通常挂在 Spread 中间或紧贴对手价。
            # 参考 perp-dex-tools: order_price = best_ask - tick_size (Post Only)
            raw_target = grvt_tick['ask'] - tick_size
        else:
            # 卖单：挂在 GRVT Best Bid + Tick
            raw_target = grvt_tick['bid'] + tick_size

        # 2. 安全性检查 (对冲成本计算)
        # 如果我们在 GRVT 买入 (Maker)，需要在 Lighter 卖出 (Taker, 即 Lighter Bid 价格)
        # 成本 = (Lighter Bid - GRVT Buy Price) / GRVT Buy Price
        # 注意：Config.MAX_SLIPPAGE_TOLERANCE 通常是负数，例如 -0.0005 (-0.05%)

        hedge_price = 0
        expected_pnl_pct = 0

        if self.farm_side == 'BUY':
            # 对冲动作：Lighter Sell
            hedge_price = lighter_tick['bid']
            if hedge_price <= 0: return None
            # PnL: (卖出价 - 买入价) / 买入价
            expected_pnl_pct = (hedge_price - raw_target) / raw_target
        else:
            # 对冲动作：Lighter Buy
            hedge_price = lighter_tick['ask']
            if hedge_price <= 0: return None
            # PnL: (卖出价 - 买入价) / 买入价 (GRVT 是卖，Lighter 是买)
            expected_pnl_pct = (raw_target - hedge_price) / hedge_price

        # 3. 判定是否满足滑点容忍度
        if expected_pnl_pct < Config.MAX_SLIPPAGE_TOLERANCE:
            # 如果亏损超过容忍度，尝试调整 Maker 价格到刚好满足容忍度的位置
            # 这可能会导致价格偏离盘口较远，从而无法成交（Post-Only 会挂单成功但不会成交）
            # 但为了安全，不能亏太多
            # logger.debug(f"价格不安全: PnL {expected_pnl_pct:.4%} < 阈值 {Config.MAX_SLIPPAGE_TOLERANCE}")
            return None

        return raw_target

    async def _process_trade_fill(self, trade: dict):
        exchange = trade['exchange']
        symbol = trade['symbol']
        side = trade['side']
        size = trade['size']

        # 只关心 GRVT 的 Maker 成交
        if exchange != 'GRVT': return

        # 1. 立即锁定状态 (防止重复对冲)
        if self.is_hedging.get(symbol, False):
            # logger.warning(f"⚠️ 收到重复成交推送 {symbol}, 忽略")
            return

        self.is_hedging[symbol] = True  # 🔒 上锁
        logger.info(f"🚨 [成交触发] GRVT {side} {size} -> 🔒 锁定策略，执行对冲")

        # 2. 清理本地挂单记录 (既然成交了，单子肯定没了)
        if symbol in self.active_orders:
            del self.active_orders[symbol]
            del self.active_order_prices[symbol]

        # 3. 启动后台对冲任务
        asyncio.create_task(self._execute_hedge_loop(symbol, side, size))

    async def _execute_hedge_loop(self, symbol, grvt_side, size):
        """
        执行对冲逻辑：
        参考 hedge_mode.py，死循环直到对冲成功或熔断。
        """
        # GRVT 买 -> Lighter 卖； GRVT 卖 -> Lighter 买
        hedge_side = 'SELL' if grvt_side.upper() == 'BUY' else 'BUY'
        symbol_pair = f"{symbol}-USDT"

        retry_count = 0
        max_retries = 20  # 高频场景多试几次
        success = False

        while retry_count < max_retries:
            try:
                # 检查是否熔断
                if retry_count > Config.MAX_CONSECUTIVE_FAILURES:
                    logger.error("❌ 超过最大连续失败次数，暂停重试以防风控")
                    # 这里可以加入更复杂的熔断逻辑

                logger.info(f"🌊 [对冲] Lighter {hedge_side} {size} (第 {retry_count + 1} 次)")

                # Lighter 适配器已封装 create_order，传入 order_type="MARKET"
                # 注意：Lighter 的 Market Order 需要 API 支持，适配器里必须有相应实现
                order_id = await self.adapters['Lighter'].create_order(
                    symbol=symbol_pair,
                    side=hedge_side,
                    amount=size,
                    order_type="MARKET"
                )

                if order_id:
                    logger.info(f"✅ [对冲成功] Lighter ID: {order_id}")
                    success = True
                    break
                else:
                    logger.warning("⚠️ Lighter 下单失败 (返回 None)，0.5s 后重试...")

            except Exception as e:
                logger.error(f"❌ 对冲异常: {e}")

            retry_count += 1
            await asyncio.sleep(0.5)

        if success:
            logger.info(f"🎉 [回合结束] {symbol} 对冲完成，{Config.TRADE_COOLDOWN}s 后解锁")
            # 冷却后再解锁，防止连续快速开单
            await asyncio.sleep(Config.TRADE_COOLDOWN)
            self.is_hedging[symbol] = False  # 🔓 解锁
        else:
            logger.critical(f"💀 [严重] {symbol} 对冲彻底失败！请人工平仓！策略将保持锁定状态。")
            # 保持 is_hedging = True，迫使策略该币种停止运作，防止敞口扩大