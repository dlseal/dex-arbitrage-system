# backend/app/strategies/hft_market_making.py
import asyncio
import logging
import time
import math
from collections import deque
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

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
        # 防止精度误差导致负数
        variance = max(0.0, (self.sq_sum / n) - (mean * mean))
        return math.sqrt(variance)


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


@dataclass
class MarketSnapshot:
    """用于在锁内捕获瞬间状态，传递给异步执行器"""
    mid_price: float
    bid_p: float
    ask_p: float
    volatility: float
    avg_ofi: float
    tick_ts: float


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: GlobalRiskController = None):
        self.name = "AS_OFI_Pro_v4_Async"
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
        self.quantity = settings.get_trade_qty(self.symbol)

        logger.info(f"🎯 HFT Strategy Init: {self.exchange_name} | {self.symbol} | Qty: {self.quantity}")

        self.risk_aversion = conf.risk_aversion
        self.ofi_sensitivity = conf.ofi_sensitivity
        self.min_spread_ticks = conf.min_spread_ticks
        self.update_threshold_ticks = conf.update_threshold_ticks
        self.max_pos_usd = conf.max_pos_usd
        self.volatility_factor = conf.volatility_factor

        self.max_dist_pct = 0.002
        self.max_skew_usd = 50.0

        self.tick_size = 0.0
        self.quantizer: Optional[PriceQuantizer] = None

        self.inventory = 0.0
        self.inv_lock = asyncio.Lock()  # 仅保护库存更新

        # 状态统计 (受 state_lock 保护)
        self.mid_price_stats = OnlineStats(window_size=conf.window_size)
        self.ofi_ema = EMACalculator(alpha=0.2)
        self.prev_tick: Optional[Dict] = None
        self.state_lock = asyncio.Lock()  # [新增] 保护统计指标计算

        # 交易状态
        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}
        self.pending_actions = {"BUY": False, "SELL": False}

        # [关键] 执行控制标志：实现"丢弃旧数据"模式
        self.is_trading = False

        self.is_active = True
        self.pos_sync_time = 0
        self.last_log_ts = 0

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
        """
        [入口] 高并发入口，必须是非阻塞的
        """
        if not self.is_active: return
        if event.get('exchange') != self.exchange_name: return

        evt_type = event.get('type')
        if evt_type == 'trade':
            await self._on_trade(event)
            return

        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            # 使用 create_task 确保 on_tick 快速返回，不阻塞引擎
            # 但真正的逻辑在 _process_tick_entry 中进行流控
            await self._process_tick_entry(event)

    async def _process_tick_entry(self, tick: dict):
        """
        [流控核心]
        1. 必须执行：更新统计数据 (State Update)
        2. 尝试执行：下单逻辑 (Execution)。如果正忙，则直接放弃本次 Tick 的下单机会。
        """
        current_ts = time.time()
        tick_ts = tick.get('ts', 0) / 1000.0
        lag = current_ts - tick_ts

        # 1. 严重延迟检查 (比如网络断了刚恢复)，此时连统计都不更新
        if lag > 2.0:
            return

        if self.tick_size <= 0:
            await self._update_contract_info()
            if self.tick_size <= 0: return

        # 2. [原子操作] 更新统计指标并获取快照
        snapshot: Optional[MarketSnapshot] = None

        async with self.state_lock:
            # 这一步非常快 (纯内存计算)，不会阻塞后续 Ticks 的排队
            snapshot = self._update_market_stats(tick, tick_ts)

            # 如果当前正在进行网络IO (is_trading)，则放弃本次执行
            # 这就是"Conflation" (合并/丢弃) 的核心
            if self.is_trading:
                return

                # 如果不忙，则抢占 flag
            self.is_trading = True

        # 3. [耗时操作] 执行交易逻辑 (释放锁后运行)
        try:
            if snapshot:
                await self._execute_strategy_logic(snapshot)
        finally:
            # 无论成功失败，必须释放 flag
            self.is_trading = False

    def _update_market_stats(self, tick: dict, tick_ts: float) -> Optional[MarketSnapshot]:
        """更新指标并返回快照 (在 state_lock 内调用)"""
        bid_p, ask_p = tick['bid'], tick['ask']
        if bid_p <= 0 or ask_p <= 0: return None

        bid_v, ask_v = self._extract_volumes(tick)
        mid_price = (bid_p + ask_p) / 2.0

        # Welford 更新
        self.mid_price_stats.update(mid_price)

        # OFI 计算
        ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
        avg_ofi = self.ofi_ema.update(ofi)

        volatility = self.mid_price_stats.get_std_dev()
        # 默认波动率兜底
        if volatility <= 0: volatility = mid_price * 0.00005

        return MarketSnapshot(
            mid_price=mid_price,
            bid_p=bid_p,
            ask_p=ask_p,
            volatility=volatility,
            avg_ofi=avg_ofi,
            tick_ts=tick_ts
        )

    async def _execute_strategy_logic(self, snap: MarketSnapshot):
        """核心交易执行逻辑 (串行执行，不会并发)"""
        try:
            # 获取当前库存快照
            async with self.inv_lock:
                current_inv = self.inventory

            pos_value = current_inv * snap.mid_price

            # 1. 熔断检查
            bailout_threshold = self.max_pos_usd * 2.0
            if abs(pos_value) > bailout_threshold:
                await self._execute_bailout(current_inv, pos_value)
                return

            # 2. AS + OFI 模型计算
            # 限制 Skew 幅度
            raw_inv_risk = current_inv * self.risk_aversion * (snap.volatility ** 2)
            inv_risk = max(min(raw_inv_risk, self.max_skew_usd), -self.max_skew_usd)

            ofi_impact = self.ofi_sensitivity * snap.avg_ofi * self.tick_size
            reservation_price = snap.mid_price + ofi_impact - inv_risk

            # 计算半价差
            half_spread_base = (self.min_spread_ticks / 2.0) * self.tick_size
            half_spread_vol = self.volatility_factor * snap.volatility
            half_spread = half_spread_base + half_spread_vol

            raw_bid = reservation_price - half_spread
            raw_ask = reservation_price + half_spread

            # 3. 价格边界与 Post-Only 保护
            max_dist = snap.mid_price * self.max_dist_pct
            min_safe_bid = snap.mid_price - max_dist
            max_safe_ask = snap.mid_price + max_dist

            final_bid = min(raw_bid, snap.ask_p - self.tick_size)
            final_bid = max(final_bid, min_safe_bid)

            final_ask = max(raw_ask, snap.bid_p + self.tick_size)
            final_ask = min(final_ask, max_safe_ask)

            target_bid = self.quantizer.quantize(final_bid, rounding=ROUND_FLOOR)
            target_ask = self.quantizer.quantize(final_ask, rounding=ROUND_CEILING)

            # 最小价差保护
            if target_ask - target_bid < self.tick_size:
                center = self.quantizer.quantize(snap.mid_price, ROUND_FLOOR)
                target_bid = center - self.tick_size
                target_ask = center + self.tick_size

            # 4. 日志采样 (使用快照的时间计算延迟)
            now = time.time()
            if now - self.last_log_ts > 5.0:
                calc_lag = now - snap.tick_ts
                logger.info(
                    f"🧮 [Calc] Lag:{calc_lag * 1000:.1f}ms | Mid:{snap.mid_price:.1f} Vol:{snap.volatility:.2f} "
                    f"Skew:{inv_risk:.2f} | Mkt:{snap.bid_p:.0f}/{snap.ask_p:.0f}"
                )
                self.last_log_ts = now

            allow_buy = pos_value < self.max_pos_usd
            allow_sell = pos_value > -self.max_pos_usd

            # 5. 执行下单 (IO 操作)
            await self._dispatch_orders(target_bid, target_ask, allow_buy, allow_sell)

            # 定时同步持仓
            if now - self.pos_sync_time > 15.0:
                self.pos_sync_time = now
                asyncio.create_task(self._sync_position())

        except Exception as e:
            logger.error(f"Execution Logic Error: {e}")

    # --- 以下辅助方法保持大致不变 ---

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
        except Exception as e:
            logger.error(f"❌ Bailout Failed: {e}")
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
        if self.pending_actions[side]: return

        # Pre-trade Risk
        if self.risk_controller:
            try:
                allowed = await self.risk_controller.check_trade_risk(self.symbol, self.quantity, target_price)
                if not allowed: return
            except Exception:
                pass

        self.pending_actions[side] = True
        try:
            current_id = self.active_orders[side]
            current_price = self.active_prices[side]

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

            retry_price = target_price
            # 重试逻辑保持不变
            for attempt in range(2):
                try:
                    new_id = await adapter.create_order(
                        symbol=self.symbol, side=side, amount=self.quantity,
                        price=retry_price, params={"post_only": True}
                    )
                    if new_id:
                        self.active_orders[side] = new_id
                        self.active_prices[side] = retry_price
                        break
                    else:
                        self.active_orders[side] = None
                        self.active_prices[side] = 0.0
                        break
                except Exception as e:
                    err_str = str(e).lower()
                    if "2008" in err_str or "post-only" in err_str:
                        if attempt == 0:
                            safe_pad = self.tick_size * 2.0
                            retry_price = retry_price - safe_pad if side == 'BUY' else retry_price + safe_pad
                            continue

                    self.active_orders[side] = None
                    self.active_prices[side] = 0.0
                    break
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
            if hasattr(adapter, 'fetch_positions'):
                if asyncio.iscoroutinefunction(adapter.fetch_positions):
                    positions = await adapter.fetch_positions()
                else:
                    loop = asyncio.get_running_loop()
                    positions = await loop.run_in_executor(None, adapter.fetch_positions)

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
                    self.tick_size = 0.01

                if self.tick_size > 0:
                    self.quantizer = PriceQuantizer(self.tick_size)
                    logger.info(f"📏 Tick Size Updated: {self.tick_size}")
        except Exception:
            pass