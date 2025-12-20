import asyncio
import logging
import time
import math
import json
from collections import deque
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from typing import Dict, Any, Optional

from app.config import settings
from app.core.risk_controller import GlobalRiskController

logger = logging.getLogger("HFT_AS_OFI")


class OnlineStats:
    """Welford算法实现的流式方差计算器"""

    def __init__(self, window_size=100):
        self.window_size = window_size
        self.values = deque(maxlen=window_size)
        self.sum = 0.0
        self.sq_sum = 0.0

    def update(self, value: float):
        if len(self.values) == self.window_size:
            old_val = self.values.popleft()
            self.sum -= old_val
            self.sq_sum -= old_val * old_val

        self.values.append(value)
        self.sum += value
        self.sq_sum += value * value

    def get_std_dev(self) -> float:
        n = len(self.values)
        if n < 2: return 0.0
        mean = self.sum / n
        variance = (self.sq_sum / n) - (mean * mean)
        return math.sqrt(max(0.0, variance))


class EMACalculator:
    """指数移动平均计算器"""

    def __init__(self, alpha=0.2):
        self.alpha = alpha
        self.value = 0.0
        self.initialized = False

    def update(self, new_val):
        if not self.initialized:
            self.value = new_val
            self.initialized = True
        else:
            self.value = self.alpha * new_val + (1 - self.alpha) * self.value
        return self.value


class PriceQuantizer:
    def __init__(self, tick_size: float):
        self.tick_size = Decimal(str(tick_size))

    def quantize(self, price: float, rounding=ROUND_FLOOR) -> float:
        if price is None or price <= 0: return 0.0
        d_price = Decimal(str(price))
        quantized = (d_price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size
        return float(quantized)


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: GlobalRiskController = None):
        self.name = "AS_OFI_Pro_v3_Optimized"
        self.adapters = adapters
        self.risk_controller = risk_controller

        # 读取配置区域
        conf = settings.strategies.hft_mm

        self.exchange_name = conf.exchange
        if not settings.common.target_symbols:
            logger.error("❌ [HFT] 未配置 TARGET_SYMBOLS")
            self.is_active = False
            return

        self.symbol = settings.common.target_symbols[0]
        # 使用辅助方法获取数量
        self.quantity = settings.get_trade_qty(self.symbol)

        logger.info(f"🎯 HFT Strategy Init: {self.exchange_name} | {self.symbol} | Qty: {self.quantity}")

        self.risk_aversion = conf.risk_aversion
        self.ofi_sensitivity = conf.ofi_sensitivity
        self.min_spread_ticks = conf.min_spread_ticks
        self.update_threshold_ticks = conf.update_threshold_ticks
        self.max_pos_usd = conf.max_pos_usd
        self.volatility_factor = conf.volatility_factor

        self.tick_size = 0.0
        self.quantizer: Optional[PriceQuantizer] = None

        self.inventory = 0.0
        self.inv_lock = asyncio.Lock()

        # 使用配置的窗口大小
        self.mid_price_stats = OnlineStats(window_size=conf.window_size)
        self.ofi_ema = EMACalculator(alpha=0.2)
        self.prev_tick: Optional[Dict] = None

        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}
        self.pending_actions = {"BUY": False, "SELL": False}

        self.calc_lock = asyncio.Lock()
        self.is_active = True
        self.pos_sync_time = 0
        self.err_count = 0

        self._validate_adapter()
        asyncio.create_task(self._initial_setup())

    def _validate_adapter(self):
        if self.exchange_name not in self.adapters:
            logger.error(f"❌ [HFT] Adapter {self.exchange_name} Not Found!")
            self.is_active = False

    async def _initial_setup(self):
        await asyncio.sleep(1.0)
        await self._update_contract_info()
        if self.tick_size <= 0:
            logger.warning("⚠️ [HFT] Tick Size 仍未获取，将在 tick 中重试")

    async def on_tick(self, event: dict):
        if not self.is_active: return
        if event.get('exchange') != self.exchange_name: return

        evt_type = event.get('type')
        if evt_type == 'trade':
            await self._on_trade(event)
            return

        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            if self.calc_lock.locked():
                return
            await self._process_tick_logic(event)

    async def _process_tick_logic(self, tick: dict):
        async with self.calc_lock:
            try:
                current_ts = time.time()
                # tick['ts'] 通常是毫秒
                tick_ts = tick.get('ts', 0) / 1000.0

                # [优化] 使用 abs() 并放宽到 5秒，防止服务器时钟略慢导致数据全丢
                if abs(current_ts - tick_ts) > 5.0:
                    return

                if self.tick_size <= 0:
                    await self._update_contract_info()
                    if self.tick_size <= 0: return

                bid_p, ask_p = tick['bid'], tick['ask']
                if bid_p <= 0 or ask_p <= 0: return

                bid_v, ask_v = self._extract_volumes(tick)
                mid_price = (bid_p + ask_p) / 2.0

                self.mid_price_stats.update(mid_price)
                ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
                avg_ofi = self.ofi_ema.update(ofi)
                volatility = self.mid_price_stats.get_std_dev()
                if volatility <= 0: volatility = mid_price * 0.0001

                async with self.inv_lock:
                    current_inv = self.inventory

                pos_value = current_inv * mid_price
                bailout_threshold = self.max_pos_usd * 2.0

                if abs(pos_value) > bailout_threshold:
                    await self._execute_bailout(current_inv, pos_value)
                    return

                inv_risk = current_inv * self.risk_aversion * (volatility ** 2)
                ofi_impact = self.ofi_sensitivity * avg_ofi * self.tick_size
                reservation_price = mid_price + ofi_impact - inv_risk

                half_spread_base = (self.min_spread_ticks / 2.0) * self.tick_size
                half_spread_vol = self.volatility_factor * volatility
                half_spread = half_spread_base + half_spread_vol

                raw_bid = reservation_price - half_spread
                raw_ask = reservation_price + half_spread

                # [优化] 防止Post-Only互穿：更严格的边界检查
                # Polling 模式下，Bid可能已经上涨，所以我们不能只依赖 snapshot 的 price
                # 至少保持在 snapshot bid + tick
                raw_bid = min(raw_bid, ask_p - self.tick_size)
                raw_ask = max(raw_ask, bid_p + self.tick_size)

                target_bid = self.quantizer.quantize(raw_bid, rounding=ROUND_FLOOR)
                target_ask = self.quantizer.quantize(raw_ask, rounding=ROUND_CEILING)

                if target_ask - target_bid < self.tick_size:
                    target_bid = self.quantizer.quantize(mid_price - self.tick_size / 2, ROUND_FLOOR)
                    target_ask = target_bid + self.tick_size

                allow_buy = pos_value < self.max_pos_usd
                allow_sell = pos_value > -self.max_pos_usd

                asyncio.create_task(self._dispatch_orders(
                    target_bid, target_ask, allow_buy, allow_sell
                ))

                if current_ts - self.pos_sync_time > 15.0:
                    self.pos_sync_time = current_ts
                    asyncio.create_task(self._sync_position())

                self.err_count = 0

            except Exception as e:
                self.err_count += 1
                if self.err_count % 100 == 0:
                    logger.error(f"Logic Error: {e}")

    def _extract_volumes(self, tick):
        bid_v, ask_v = 1.0, 1.0
        if tick.get('bids_depth'): bid_v = float(tick['bids_depth'][0][1])
        if tick.get('asks_depth'): ask_v = float(tick['asks_depth'][0][1])
        if 'bid_volume' in tick: bid_v = float(tick['bid_volume'])
        if 'ask_volume' in tick: ask_v = float(tick['ask_volume'])
        return bid_v, ask_v

    def _calculate_ofi(self, bid_p, bid_v, ask_p, ask_v) -> float:
        if not self.prev_tick:
            self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bv': bid_v, 'av': ask_v}
            return 0.0

        e_bid = 0.0
        if bid_p > self.prev_tick['bid']:
            e_bid = bid_v
        elif bid_p < self.prev_tick['bid']:
            e_bid = -self.prev_tick['bv']
        else:
            e_bid = bid_v - self.prev_tick['bv']

        e_ask = 0.0
        if ask_p > self.prev_tick['ask']:
            e_ask = self.prev_tick['av']
        elif ask_p < self.prev_tick['ask']:
            e_ask = -ask_v
        else:
            e_ask = -(ask_v - self.prev_tick['av'])

        self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bv': bid_v, 'av': ask_v}
        return e_bid + e_ask

    async def _execute_bailout(self, inventory, value):
        side = 'SELL' if inventory > 0 else 'BUY'
        if self.pending_actions[side]: return

        self.pending_actions[side] = True
        logger.critical(f"🚨 [BAILOUT] {side} Size:{abs(inventory)} Val:{value:.0f}")

        try:
            # Bailout 不做风控检查，优先平仓
            # 简单的重试机制
            for i in range(3):
                try:
                    await self.adapters[self.exchange_name].create_order(
                        symbol=self.symbol,
                        side=side,
                        amount=abs(inventory),
                        order_type='MARKET'
                    )
                    break
                except Exception as inner_e:
                    if i == 2: raise inner_e
                    await asyncio.sleep(0.5)

            await asyncio.sleep(1.0)
        except Exception as e:
            logger.error(f"❌ Bailout Failed: {e}")
            # 如果 Bailout 失败，触发风控熔断
            if self.risk_controller:
                self.risk_controller.trigger_circuit_breaker(f"Bailout Failed: {e}")
        finally:
            self.pending_actions[side] = False

    async def _dispatch_orders(self, target_bid, target_ask, allow_buy, allow_sell):
        tasks = []
        if allow_buy:
            tasks.append(self._manage_order_side('BUY', target_bid))
        else:
            if self.active_orders['BUY']:
                tasks.append(self._cancel_order_side('BUY'))

        if allow_sell:
            tasks.append(self._manage_order_side('SELL', target_ask))
        else:
            if self.active_orders['SELL']:
                tasks.append(self._cancel_order_side('SELL'))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _manage_order_side(self, side, target_price):
        """
        下单管理：包含 Post-Only 失败后的自动重试逻辑 (Chase the Maker)
        """
        if self.pending_actions[side]: return

        # --- Pre-trade Risk Check ---
        if self.risk_controller:
            try:
                allowed = await self.risk_controller.check_trade_risk(self.symbol, self.quantity, target_price)
                if not allowed:
                    return
            except Exception:
                pass

        self.pending_actions[side] = True

        try:
            current_id = self.active_orders[side]
            current_price = self.active_prices[side]

            # 价格变动不大则忽略，减少 API 调用
            if current_id and current_price > 0:
                diff_ticks = abs(target_price - current_price) / self.tick_size
                if diff_ticks < self.update_threshold_ticks:
                    return

            adapter = self.adapters[self.exchange_name]
            if current_id:
                try:
                    await adapter.cancel_order(current_id, symbol=self.symbol)
                except Exception:
                    pass

            # 尝试下单，增加重试逻辑处理 Post-Only 失败 (Error 2008)
            retry_price = target_price
            for attempt in range(2):
                try:
                    new_id = await adapter.create_order(
                        symbol=self.symbol,
                        side=side,
                        amount=self.quantity,
                        price=retry_price,
                        params={"post_only": True}
                    )

                    if new_id:
                        self.active_orders[side] = new_id
                        self.active_prices[side] = retry_price
                        # 成功则退出重试循环
                        break
                    else:
                        self.active_orders[side] = None
                        self.active_prices[side] = 0.0
                        break

                except Exception as e:
                    err_str = str(e)
                    # 处理 "2008": Post-Only crosses book
                    # 处理 "post-only": 通用文本匹配
                    if "2008" in err_str or "post-only" in err_str.lower():
                        if attempt == 0:
                            # 第一次失败，计算新的安全价格
                            # 如果是买单，价格太高导致 Cross，需要降低价格
                            # 如果是卖单，价格太低导致 Cross，需要提高价格
                            offset = self.tick_size
                            if side == 'BUY':
                                retry_price = retry_price - offset
                                logger.warning(f"⚠️ [HFT] Post-Only Fail (BUY). Retrying @ {retry_price}")
                            else:
                                retry_price = retry_price + offset
                                logger.warning(f"⚠️ [HFT] Post-Only Fail (SELL). Retrying @ {retry_price}")
                            continue  # 进入下一次循环重试

                    # 其他错误直接抛出或记录，不再重试
                    logger.error(f"Quote {side} Error: {e}")
                    self.active_orders[side] = None
                    self.active_prices[side] = 0.0
                    break

        except Exception as e:
            logger.error(f"Quote {side} Critical Error: {e}")
        finally:
            self.pending_actions[side] = False

    async def _cancel_order_side(self, side):
        if self.pending_actions[side]: return
        self.pending_actions[side] = True

        try:
            oid = self.active_orders[side]
            if oid:
                await self.adapters[self.exchange_name].cancel_order(oid, symbol=self.symbol)
                self.active_orders[side] = None
                self.active_prices[side] = 0.0
        except Exception as e:
            logger.warning(f"Cancel {side} Error: {e}")
        finally:
            self.pending_actions[side] = False

    async def _on_trade(self, trade):
        if trade['symbol'] != self.symbol: return
        try:
            size = float(trade['size'])
            side = trade['side'].upper()
            async with self.inv_lock:
                if side == 'BUY':
                    self.inventory += size
                else:
                    self.inventory -= size
                current_inv = self.inventory
            logger.info(f"⚡️ [Fill] {side} {size} | Inv: {current_inv:.4f}")
        except Exception as e:
            logger.error(f"On Trade Error: {e}")

    async def _sync_position(self):
        try:
            adapter = self.adapters[self.exchange_name]
            positions = []

            # 兼容多种适配器的获取持仓方式
            if hasattr(adapter, 'fetch_positions') and callable(adapter.fetch_positions):
                if asyncio.iscoroutinefunction(adapter.fetch_positions):
                    positions = await adapter.fetch_positions()
                else:
                    positions = await asyncio.to_thread(adapter.fetch_positions)
            elif hasattr(adapter, 'rest_client') and adapter.rest_client:
                loop = asyncio.get_running_loop()
                positions = await loop.run_in_executor(
                    None,
                    lambda: adapter.rest_client.fetch_positions(
                        params={'sub_account_id': getattr(adapter, 'trading_account_id', None)}
                    )
                )

            found_size = 0.0
            for p in positions:
                inst = p.get('instrument') or p.get('symbol') or ""
                if self.symbol in inst:
                    size = float(p.get('size', 0) or p.get('contracts', 0))
                    side = p.get('side', '').upper()
                    if side == 'SHORT' and size > 0:
                        size = -size
                    elif side == 'LONG' and size < 0:
                        size = abs(size)
                    found_size = size
                    break

            async with self.inv_lock:
                if abs(self.inventory - found_size) > (self.quantity * 0.1):
                    logger.warning(f"⚠️ [Sync] Inv fix: {self.inventory:.4f} -> {found_size:.4f}")
                    self.inventory = found_size

        except Exception as e:
            logger.error(f"Sync Pos Error: {e}")

    async def _update_contract_info(self):
        try:
            adapter = self.adapters[self.exchange_name]
            contract_map = getattr(adapter, 'contract_map', {}) or getattr(adapter, 'market_config', {})

            found = None
            for k, v in contract_map.items():
                if self.symbol in k:
                    found = v
                    break

            if found:
                if 'tick_size' in found:
                    self.tick_size = float(found['tick_size'])
                elif 'price_mul' in found:
                    # Lighter 的情况
                    self.tick_size = 0.01

                if self.tick_size > 0:
                    self.quantizer = PriceQuantizer(self.tick_size)
                    logger.info(f"📏 Tick Size Updated: {self.tick_size}")
        except Exception as e:
            logger.error(f"Update Contract Info Error: {e}")