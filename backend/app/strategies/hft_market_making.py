# backend/app/strategies/hft_market_making.py
import asyncio
import logging
import time
import math
from collections import deque
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


class FastPriceQuantizer:
    """
    [性能优化] 极速价格量化器
    完全移除 Decimal，改用浮点乘法预计算
    """

    def __init__(self, tick_size: float):
        self.tick_size = float(tick_size)
        # 预计算倒数，乘法比除法快得多
        self.inv_tick = 1.0 / self.tick_size if self.tick_size > 0 else 0.0
        self.epsilon = 1e-9

    def quantize(self, price: float, mode: str = 'FLOOR') -> float:
        if price <= 0 or self.inv_tick == 0: return 0.0

        # 使用 epsilon 防止浮点精度误差 (如 0.9999999 -> 0)
        scaled = price * self.inv_tick

        if mode == 'FLOOR':
            return math.floor(scaled + self.epsilon) * self.tick_size
        elif mode == 'CEILING':
            return math.ceil(scaled - self.epsilon) * self.tick_size
        else:
            return round(scaled) * self.tick_size


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: GlobalRiskController = None):
        self.name = "AS_OFI_Pro_v5_Performance"
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
        self.quantizer: Optional[FastPriceQuantizer] = None

        self.inventory = 0.0
        self.inv_lock = asyncio.Lock()  # 仅保护库存写入

        # 状态统计
        self.mid_price_stats = OnlineStats(window_size=conf.window_size)
        self.ofi_ema = EMACalculator(alpha=0.2)
        self.prev_tick: Optional[Dict] = None

        # 交易状态
        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}
        self.pending_actions = {"BUY": False, "SELL": False}

        # 锁只保护计算逻辑，不包含 IO
        self.calc_lock = asyncio.Lock()

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
        if not self.is_active: return

        # 快速路径过滤
        if event.get('type') != 'tick' or event.get('symbol') != self.symbol:
            if event.get('type') == 'trade':
                await self._on_trade(event)
            return

        # [性能优化] 尝试获取锁，如果拿不到(上一帧还没算完)，直接丢弃，不要排队
        if self.calc_lock.locked():
            return

        await self._process_tick_logic(event)

    async def _process_tick_logic(self, tick: dict):
        # 准备数据容器，用于在锁外打印日志
        log_payload = None

        async with self.calc_lock:
            try:
                # 1. 基础检查
                if self.tick_size <= 0:
                    await self._update_contract_info()
                    if self.tick_size <= 0: return

                # 提取数据 (假定 adapter 已经传来了 float)
                bid_p = float(tick['bid'])
                ask_p = float(tick['ask'])
                if bid_p <= 0 or ask_p <= 0: return

                bid_v, ask_v = self._extract_volumes(tick)
                mid_price = (bid_p + ask_p) * 0.5  # 乘法比除法快

                # 2. 更新统计 (内存计算，微秒级)
                self.mid_price_stats.update(mid_price)
                ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
                avg_ofi = self.ofi_ema.update(ofi)

                volatility = self.mid_price_stats.get_std_dev()
                if volatility <= 0: volatility = mid_price * 0.00005

                # 3. 获取库存 (无需锁，读取原子变量)
                current_inv = self.inventory
                pos_value = current_inv * mid_price

                # 4. 熔断检查
                if abs(pos_value) > self.max_pos_usd * 2.0:
                    # 启动后台任务去平仓，不阻塞当前计算
                    asyncio.create_task(self._execute_bailout(current_inv, pos_value))
                    return

                # 5. AS + OFI 核心模型 (纯浮点运算)
                # 限制 skew 范围 (-50 ~ 50)
                raw_inv_risk = current_inv * self.risk_aversion * (volatility ** 2)
                inv_risk = 50.0 if raw_inv_risk > 50.0 else (-50.0 if raw_inv_risk < -50.0 else raw_inv_risk)

                ofi_impact = self.ofi_sensitivity * avg_ofi * self.tick_size
                reservation_price = mid_price + ofi_impact - inv_risk

                half_spread = ((self.min_spread_ticks * 0.5) * self.tick_size) + \
                              (self.volatility_factor * volatility)

                raw_bid = reservation_price - half_spread
                raw_ask = reservation_price + half_spread

                # 价格钳制
                max_dist = mid_price * self.max_dist_pct
                min_safe_bid = mid_price - max_dist
                max_safe_ask = mid_price + max_dist

                final_bid = min(raw_bid, ask_p - self.tick_size)
                final_bid = max(final_bid, min_safe_bid)

                final_ask = max(raw_ask, bid_p + self.tick_size)
                final_ask = min(final_ask, max_safe_ask)

                # 量化 (FastPriceQuantizer)
                target_bid = self.quantizer.quantize(final_bid, 'FLOOR')
                target_ask = self.quantizer.quantize(final_ask, 'CEILING')

                # 最小价差保护
                if target_ask - target_bid < self.tick_size:
                    center = self.quantizer.quantize(mid_price, 'FLOOR')
                    target_bid = center - self.tick_size
                    target_ask = center + self.tick_size

                # 6. 收集日志数据 (不在此处 IO)
                current_ts = time.time()
                if current_ts - self.last_log_ts > 5.0:
                    log_payload = {
                        'tick_ts': tick.get('ts', 0) / 1000.0,
                        'mid': mid_price,
                        'vol': volatility,
                        'skew': inv_risk,
                        'bp': bid_p,
                        'ap': ask_p
                    }
                    self.last_log_ts = current_ts

                allow_buy = pos_value < self.max_pos_usd
                allow_sell = pos_value > -self.max_pos_usd

                # 7. 派发订单 (IO 放入后台)
                asyncio.create_task(self._dispatch_orders(
                    target_bid, target_ask, allow_buy, allow_sell
                ))

                # 定时同步
                if current_ts - self.pos_sync_time > 15.0:
                    self.pos_sync_time = current_ts
                    asyncio.create_task(self._sync_position())

            except Exception as e:
                # 仅在严重错误时打印
                logger.error(f"Logic Error: {e}")

        # 锁已释放，安全打印日志
        if log_payload:
            lag = time.time() - log_payload['tick_ts']
            logger.info(
                f"🧮 [Calc] Lag:{lag * 1000:.1f}ms | Mid:{log_payload['mid']:.1f} Vol:{log_payload['vol']:.2f} "
                f"Skew:{log_payload['skew']:.2f} | Mkt:{log_payload['bp']:.0f}/{log_payload['ap']:.0f}"
            )

    def _extract_volumes(self, tick):
        # 确保返回浮点数
        bid_v = float(tick.get('bid_volume', 0) or 0)
        ask_v = float(tick.get('ask_volume', 0) or 0)

        # 兼容 depth 格式
        if bid_v == 0 and tick.get('bids_depth'):
            bid_v = float(tick['bids_depth'][0][1])
        if ask_v == 0 and tick.get('asks_depth'):
            ask_v = float(tick['asks_depth'][0][1])

        return max(0.1, bid_v), max(0.1, ask_v)

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

        # Pre-trade Risk Check
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
                    self.quantizer = FastPriceQuantizer(self.tick_size)
                    logger.info(f"📏 Tick Size Updated: {self.tick_size}")
        except Exception:
            pass