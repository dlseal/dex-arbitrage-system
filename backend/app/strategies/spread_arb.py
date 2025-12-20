import asyncio
import logging
import time
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple

# 假设你的 settings 导入路径
from app.config import settings


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

        self.books: Dict[str, Dict[str, Dict]] = {}
        self.spread_threshold = settings.strategies.spread_arb.threshold
        self.trade_cooldown = settings.strategies.spread_arb.cooldown
        self.data_max_age = 5.0
        self.is_active = True
        self.is_trading = False

        # 激进补单的滑点设置 (参考 crypto-trading-open)
        self.recovery_slippage = 0.02  # 2% 激进滑点，确保补单成交

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
        # ... (保持原有的 on_tick 逻辑不变) ...
        if not self.is_active: return
        exchange = tick_data.get('exchange')
        symbol = tick_data.get('symbol')
        if exchange not in [self.ex_a, self.ex_b]: return
        if symbol not in self.books: self.books[symbol] = {}

        self.books[symbol][exchange] = {
            'bid': float(tick_data.get('bid', 0)),
            'ask': float(tick_data.get('ask', 0)),
            'ts': time.time()
        }
        if self.ex_a in self.books[symbol] and self.ex_b in self.books[symbol]:
            await self._check_opportunity(symbol)

    async def _check_opportunity(self, symbol: str):
        # ... (保持原有的 _check_opportunity 逻辑不变) ...
        if self.is_trading: return

        tick_a = self.books[symbol][self.ex_a]
        tick_b = self.books[symbol][self.ex_b]
        now = time.time()

        if (now - tick_a['ts'] > self.data_max_age) or (now - tick_b['ts'] > self.data_max_age):
            return

        if tick_a['bid'] <= 0 or tick_a['ask'] <= 0 or tick_b['bid'] <= 0 or tick_b['ask'] <= 0:
            return

        # Path A: Sell A, Buy B
        diff_path_a = tick_a['bid'] - tick_b['ask']
        spread_path_a = diff_path_a / tick_b['ask']

        # Path B: Sell B, Buy A
        diff_path_b = tick_b['bid'] - tick_a['ask']
        spread_path_b = diff_path_b / tick_a['ask']

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

    # ----------------------------------------------------------------
    # 🔥 核心优化区域：执行与风控逻辑
    # ----------------------------------------------------------------

    async def _execute_arb(self, symbol, ex_sell, ex_buy, price_sell, price_buy, spread, path_name):
        if self.is_trading: return
        self.is_trading = True

        try:
            quantity = settings.get_trade_qty(symbol)
            self._log_opportunity(symbol, path_name, spread, price_sell, price_buy, quantity)
            logger.info(f"🚀 [EXECUTE] {path_name} | Qty: {quantity}")

            # 1. 并发执行双边订单 (使用 return_exceptions=True 捕获单个失败)
            # 注意：对于套利，建议使用 IOC (Immediate or Cancel) 或 FOK，防止订单挂在盘口
            task_sell = self.adapters[ex_sell].create_order(
                symbol=symbol, side="SELL", amount=quantity, price=price_sell, order_type="LIMIT"
            )
            task_buy = self.adapters[ex_buy].create_order(
                symbol=symbol, side="BUY", amount=quantity, price=price_buy, order_type="LIMIT"
            )

            # results 包含 [order_id_sell, order_id_buy] 或 Exception
            results = await asyncio.gather(task_sell, task_buy, return_exceptions=True)
            res_sell, res_buy = results[0], results[1]

            # 2. 检查执行结果
            sell_ok = self._is_order_success(res_sell)
            buy_ok = self._is_order_success(res_buy)

            # 3. 完美情况：双边都成功 (或者都拿到了 OrderID，视为成功)
            if sell_ok and buy_ok:
                logger.info(f"✅ [SUCCESS] Double Leg Executed. SellID: {res_sell}, BuyID: {res_buy}")

            # 4. 致命情况：双边都失败
            elif not sell_ok and not buy_ok:
                logger.error(f"❌ [FAILED] Both legs failed. SellErr: {res_sell}, BuyErr: {res_buy}")

            # 5. 🔥 风险情况：单腿成交 (缺腿) -> 触发自动修复
            else:
                await self._handle_single_leg_failure(
                    symbol=symbol,
                    quantity=quantity,
                    ex_sell=ex_sell, res_sell=res_sell, sell_ok=sell_ok,
                    ex_buy=ex_buy, res_buy=res_buy, buy_ok=buy_ok
                )

        except Exception as e:
            logger.critical(f"❌ Critical Strategy Error: {e}", exc_info=True)
        finally:
            await asyncio.sleep(self.trade_cooldown)
            self.is_trading = False

    def _is_order_success(self, result: Any) -> bool:
        """简单的成功判断：不是异常且不是 None"""
        return result is not None and not isinstance(result, Exception)

    async def _handle_single_leg_failure(
            self, symbol, quantity,
            ex_sell, res_sell, sell_ok,
            ex_buy, res_buy, buy_ok
    ):
        """
        处理缺腿逻辑：
        1. 识别哪边失败了。
        2. 尝试在失败边进行 '激进补单' (Recovery Order)。
        3. 如果补单失败，强平成功边 (Emergency Close)。
        """
        # 确定谁成功，谁失败
        if sell_ok:
            # 卖出成功，买入失败 -> 需要补买入 (Recovery Buy)
            failed_ex = ex_buy
            failed_side = "BUY"
            ok_ex = ex_sell
            ok_side = "SELL"  # 成功的单是卖单，如果要平仓需要买回来
            base_price = self.books[symbol][failed_ex]['ask']  # 获取当前卖一价作为基准
        else:
            # 买入成功，卖出失败 -> 需要补卖出 (Recovery Sell)
            failed_ex = ex_sell
            failed_side = "SELL"
            ok_ex = ex_buy
            ok_side = "BUY"  # 成功的单是买单，如果要平仓需要卖出去
            base_price = self.books[symbol][failed_ex]['bid']  # 获取当前买一价作为基准

        logger.critical(
            f"🚨 [RISK ALERT] 单腿成交! {ok_ex} 成功, {failed_ex} 失败. "
            f"正在 {failed_ex} 尝试 {failed_side} 补单..."
        )

        # --- 第一阶段：尝试激进补单 ---
        recovery_success = await self._place_recovery_order(
            exchange=failed_ex,
            symbol=symbol,
            side=failed_side,
            quantity=quantity,
            base_price=base_price
        )

        if recovery_success:
            logger.info(f"✅ [RECOVERY] {failed_ex} 补单成功! 风险已解除。")
            return

        # --- 第二阶段：补单失败，执行紧急平仓 (回滚) ---
        logger.critical(f"❌ [RECOVERY FAILED] 补单失败，正在强平 {ok_ex} 以关闭敞口...")

        # 如果原来是 SELL，平仓就是 BUY；原来是 BUY，平仓就是 SELL
        close_side = "BUY" if ok_side == "SELL" else "SELL"

        # 平仓通常使用市价单，或者极度激进的限价单
        # 获取当前对手价用于激进平仓
        close_base_price = self.books[symbol][ok_ex]['ask' if close_side == "BUY" else 'bid']

        close_success = await self._place_recovery_order(
            exchange=ok_ex,
            symbol=symbol,
            side=close_side,
            quantity=quantity,
            base_price=close_base_price,
            is_emergency=True  # 紧急模式，滑点更大
        )

        if close_success:
            logger.warning(f"⚠️ [EMERGENCY CLOSE] 已平掉 {ok_ex} 的持仓，本次套利亏损手续费/滑点，但敞口已关闭。")
        else:
            logger.critical(f"☠️ [FATAL ERROR] 紧急平仓也失败了! 请立即人工介入检查 {ok_ex} 的 {symbol} 持仓!")

    async def _place_recovery_order(
            self, exchange: str, symbol: str, side: str,
            quantity: float, base_price: float, is_emergency: bool = False
    ) -> bool:
        """
        发送激进限价单 (Aggressive Limit Order)。
        对于 Lighter 这种不支持市价单或市价单不稳的 DEX，计算一个必定成交的限价。
        """
        adapter = self.adapters.get(exchange)
        if not adapter: return False

        # 1. 计算激进价格
        # 如果是紧急平仓，滑点放大到 5%，普通补单 2%
        slippage = 0.05 if is_emergency else self.recovery_slippage

        if side == "BUY":
            # 买入：挂高价 (Current Ask * 1.02)
            aggressive_price = base_price * (1 + slippage)
        else:
            # 卖出：挂低价 (Current Bid * 0.98)
            aggressive_price = base_price * (1 - slippage)

        logger.info(
            f"⚡ [AGGRESSIVE] {exchange} {side} {quantity} @ {aggressive_price:.4f} "
            f"(Base: {base_price}, Slip: {slippage * 100}%)"
        )

        try:
            # 2. 发送订单 (建议 Adapter 内部支持 order_type="MARKET" 时自动处理，或者这里强制 LIMIT)
            # 这里的 LIMIT 价格已经非常激进，相当于市价单
            result = await adapter.create_order(
                symbol=symbol,
                side=side,
                amount=quantity,
                price=aggressive_price,
                order_type="LIMIT"
            )

            # 3. 简单的结果检查
            if result and not isinstance(result, Exception):
                return True
            else:
                logger.error(f"Recovery order returned error: {result}")
                return False

        except Exception as e:
            logger.error(f"Recovery order exception: {e}")
            return False

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