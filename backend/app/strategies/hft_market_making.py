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
    """[性能优化] 极速价格量化器"""

    def __init__(self, tick_size: float):
        self.tick_size = float(tick_size)
        self.inv_tick = 1.0 / self.tick_size if self.tick_size > 0 else 0.0

    def quantize(self, price: float, rounding=None) -> float:
        if price <= 0 or self.inv_tick == 0: return 0.0
        scaled = price * self.inv_tick
        return round(scaled) * self.tick_size


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: GlobalRiskController = None):
        self.name = "AS_OFI_Pro_v5_Opt"
        self.adapters = adapters
        self.risk_controller = risk_controller

        # --- 配置加载 ---
        conf = settings.strategies.hft_mm
        self.exchange_name = conf.exchange

        if not settings.common.target_symbols:
            logger.error("❌ [HFT] No TARGET_SYMBOLS configured.")
            self.is_active = False
            return

        self.symbol = settings.common.target_symbols[0]
        self.quantity = settings.get_trade_qty(self.symbol)

        # 风险参数
        self.risk_aversion = conf.risk_aversion
        self.ofi_sensitivity = conf.ofi_sensitivity
        self.min_spread_ticks = conf.min_spread_ticks
        self.update_threshold_ticks = conf.update_threshold_ticks

        # [Strict] 熔断阈值
        self.max_pos_usd = conf.max_pos_usd
        # 软限制: 90% 停止同向开仓
        self.soft_limit_usd = self.max_pos_usd * 0.9
        # 硬限制: 110% 触发强制平仓 (原先是 200%)
        self.hard_limit_usd = self.max_pos_usd * 1.1

        self.volatility_factor = conf.volatility_factor
        self.max_dist_pct = 0.002
        self.max_skew_usd = 50.0

        # --- 内部状态 ---
        self.tick_size = 0.0
        self.quantizer: Optional[FastPriceQuantizer] = None

        self.inventory = 0.0
        self.inv_lock = asyncio.Lock()  # 保护库存并发写

        # 统计指标
        self.mid_price_stats = OnlineStats(window_size=conf.window_size)
        self.ofi_ema = EMACalculator(alpha=0.2)
        self.prev_tick: Optional[Dict] = None

        # 订单状态
        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}
        self.pending_actions = {"BUY": False, "SELL": False}
        self.is_bailout_active = False  # 熔断状态机

        # 计算锁
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
        """初始化：获取合约详情，初次同步持仓"""
        await asyncio.sleep(1.0)
        await self._update_contract_info()

        # 启动时强制同步一次持仓
        await self._sync_position(force=True)

        if self.tick_size <= 0:
            logger.warning("⚠️ [HFT] Tick Size unknown, waiting for ticks...")

    async def on_tick(self, event: dict):
        if not self.is_active: return

        # 1. 基础过滤
        if event.get('type') != 'tick' or event.get('symbol') != self.symbol:
            if event.get('type') == 'trade':
                await self._on_trade(event)
            return

        # 2. 时效性检查 (HFT 关键: 丢弃超过 500ms 的旧数据)
        now_ms = time.time() * 1000
        tick_ts = event.get('ts', now_ms)
        latency = now_ms - tick_ts
        if latency > 2000:
            logger.warning(f"⚠️ [HFT] Tick too old: {latency:.0f}ms > 2000ms. Dropping.")
            return

        # 3. 熔断模式下不处理 Tick，专心平仓
        if self.is_bailout_active:
            return

        # 4. 非阻塞尝试获取锁
        if self.calc_lock.locked():
            return

        await self._process_tick_logic(event)

    async def _process_tick_logic(self, tick: dict):
        log_payload = None

        async with self.calc_lock:
            try:
                # --- 数据准备 ---
                if self.tick_size <= 0:
                    await self._update_contract_info()
                    if self.tick_size <= 0: return

                bid_p = float(tick['bid'])
                ask_p = float(tick['ask'])
                if bid_p <= 0 or ask_p <= 0: return

                bid_v, ask_v = self._extract_volumes(tick)
                mid_price = (bid_p + ask_p) * 0.5

                # --- 模型更新 ---
                self.mid_price_stats.update(mid_price)
                ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
                avg_ofi = self.ofi_ema.update(ofi)

                volatility = self.mid_price_stats.get_std_dev()
                if volatility <= 0: volatility = mid_price * 0.00005

                # --- 风控检查 ---
                current_inv = self.inventory
                pos_value = current_inv * mid_price

                # [Strict] 熔断检查: 超过硬限制 -> 触发 Bailout
                if abs(pos_value) > self.hard_limit_usd:
                    logger.critical(
                        f"🚨 [RISK] Pos ${pos_value:.1f} > HardLimit ${self.hard_limit_usd}. TRIGGER BAILOUT.")
                    self.is_bailout_active = True
                    asyncio.create_task(self._execute_strict_bailout(current_inv))
                    return

                # --- 报价计算 ---
                raw_inv_risk = current_inv * self.risk_aversion * (volatility ** 2)
                # 限制偏斜度
                inv_risk = max(-self.max_skew_usd, min(self.max_skew_usd, raw_inv_risk))

                ofi_impact = self.ofi_sensitivity * avg_ofi * self.tick_size
                reservation_price = mid_price + ofi_impact - inv_risk

                half_spread = ((self.min_spread_ticks * 0.5) * self.tick_size) + \
                              (self.volatility_factor * volatility)

                raw_bid = reservation_price - half_spread
                raw_ask = reservation_price + half_spread

                # 价格保护带
                max_dist = mid_price * self.max_dist_pct
                min_safe_bid = mid_price - max_dist
                max_safe_ask = mid_price + max_dist

                final_bid = max(min(raw_bid, ask_p - self.tick_size), min_safe_bid)
                final_ask = min(max(raw_ask, bid_p + self.tick_size), max_safe_ask)

                target_bid = self.quantizer.quantize(final_bid, 'FLOOR')
                target_ask = self.quantizer.quantize(final_ask, 'CEILING')

                # 最小价差保护
                if target_ask - target_bid < self.tick_size:
                    center = self.quantizer.quantize(mid_price, 'FLOOR')
                    target_bid = center - self.tick_size
                    target_ask = center + self.tick_size

                # --- 订单派发逻辑 ---

                # 软限制: 接近上限时，禁止同向开仓
                allow_buy = pos_value < self.soft_limit_usd
                allow_sell = pos_value > -self.soft_limit_usd

                asyncio.create_task(self._dispatch_orders(
                    target_bid, target_ask, allow_buy, allow_sell
                ))

                # --- 状态同步 ---
                # [Strict] 缩短同步周期至 3s
                current_ts = time.time()
                if current_ts - self.pos_sync_time > 3.0:
                    self.pos_sync_time = current_ts
                    asyncio.create_task(self._sync_position())

                # 日志采样
                if current_ts - self.last_log_ts > 5.0:
                    log_payload = {
                        'tick_ts': tick.get('ts', 0) / 1000.0,
                        'mid': mid_price, 'vol': volatility, 'inv': current_inv,
                        'skew': inv_risk, 'bp': bid_p, 'ap': ask_p
                    }
                    self.last_log_ts = current_ts

            except Exception as e:
                logger.error(f"Logic Error: {e}")

        if log_payload:
            lag = time.time() - log_payload['tick_ts']
            logger.info(
                f"🧮 [Calc] Lag:{lag * 1000:.1f}ms | Inv:{log_payload['inv']:.4f} | "
                f"Vol:{log_payload['vol']:.2f} | Mkt:{log_payload['bp']:.0f}/{log_payload['ap']:.0f}"
            )

    async def _execute_strict_bailout(self, inventory: float):
        """
        [PROD] 严格的平仓流程
        1. Cancel All (原子级)
        2. Sync Position (确认真实持仓)
        3. Market Close (一次性)
        """
        adapter = self.adapters[self.exchange_name]
        side = 'SELL' if inventory > 0 else 'BUY'

        logger.warning("🛑 [BAILOUT] Phase 1: Cancelling all orders...")

        # 1. 撤单
        try:
            # 优先调用适配器实现的原子撤单
            if hasattr(adapter, 'cancel_all_orders'):
                await adapter.cancel_all_orders(symbol=self.symbol)
            else:
                # 降级方案
                if self.active_orders['BUY']: await self._cancel_order_side('BUY')
                if self.active_orders['SELL']: await self._cancel_order_side('SELL')
        except Exception as e:
            logger.error(f"❌ Bailout Cancel Failed: {e}")

        await asyncio.sleep(0.5)  # 等待交易所撮合落地

        # 2. 确认持仓 (从 API 获取最新，而非依赖本地)
        logger.warning("🛑 [BAILOUT] Phase 2: Verifying position...")
        real_size = await self._sync_position(force=True)
        if real_size is None:
            real_size = inventory  # 如果同步失败，回退到本地记录

        if abs(real_size) < (self.quantity * 0.1):
            logger.info("✅ [BAILOUT] Position already closed.")
            self.is_bailout_active = False
            return

        # 3. 市价平仓
        close_side = 'SELL' if real_size > 0 else 'BUY'
        close_qty = abs(real_size)
        logger.warning(f"🛑 [BAILOUT] Phase 3: Market Close {close_side} {close_qty}...")

        try:
            for i in range(5):
                try:
                    await adapter.create_order(
                        symbol=self.symbol,
                        side=close_side,
                        amount=close_qty,
                        order_type='MARKET'
                    )
                    logger.info("✅ [BAILOUT] Close Order Sent.")
                    break
                except Exception as ex:
                    logger.error(f"Retry {i + 1} Failed: {ex}")
                    await asyncio.sleep(0.5)
        except Exception as e:
            logger.critical(f"❌❌❌ BAILOUT FAILED: {e}")
        finally:
            await asyncio.sleep(2.0)
            await self._sync_position(force=True)
            self.is_bailout_active = False  # 恢复正常

    async def _sync_position(self, force=False):
        """
        [PROD] 安全的仓位同步
        返回: 最新持仓数量 (float) 或 None (如果失败)
        """
        try:
            adapter = self.adapters[self.exchange_name]

            # 检查适配器能力，防止调用不存在的方法导致误判为0
            if not hasattr(adapter, 'fetch_positions'):
                if force: logger.error("❌ Adapter missing fetch_positions!")
                return None

            positions = await adapter.fetch_positions(symbols=[self.symbol])

            # 只有在明确返回列表时才处理，避免 Exception 导致的数据置空
            found_size = 0.0
            found = False

            for p in positions:
                # 模糊匹配 symbol
                p_sym = p.get('symbol', '') or p.get('instrument', '')
                if self.symbol in p_sym or p_sym in self.symbol:
                    size = float(p.get('size', 0) or p.get('contracts', 0))
                    side = p.get('side', '').upper()
                    if side == 'SHORT' and size > 0:
                        size = -size
                    elif side == 'LONG' and size < 0:
                        size = abs(size)
                    found_size = size
                    found = True
                    break

            # 即使 positions 为空列表，也意味着持仓为 0 (前提是调用成功)
            # 如果 positions 是 None，说明调用失败，不做处理

            async with self.inv_lock:
                diff = abs(self.inventory - found_size)
                if diff > (self.quantity * 0.1):
                    logger.warning(f"⚠️ [Sync] Correction: {self.inventory:.4f} -> {found_size:.4f}")
                    self.inventory = found_size

            return found_size

        except Exception as e:
            logger.error(f"Sync Pos Error: {e}")
            return None

    # --- 辅助方法 ---
    def _extract_volumes(self, tick):
        bid_v = float(tick.get('bid_volume', 0) or 0)
        ask_v = float(tick.get('ask_volume', 0) or 0)
        if bid_v == 0 and tick.get('bids_depth'):
            bid_v = float(tick['bids_depth'][0][1])
        if ask_v == 0 and tick.get('asks_depth'):
            ask_v = float(tick['asks_depth'][0][1])
        return max(0.1, bid_v), max(0.1, ask_v)

    def _calculate_ofi(self, bid_p, bid_v, ask_p, ask_v) -> float:
        """
        修改版 OFI 计算逻辑 (针对刷量优化 - Normalized OFI)
        原版返回的是净成交量的绝对值，容易造成价格跳动过大。
        修改版返回的是 "OFI率" (区间约 -1 到 1)，让信号更平滑。
        """
        if not self.prev_tick:
            self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bv': bid_v, 'av': ask_v}
            return 0.0

        e_bid = 0.0
        # 这里的逻辑保持不变：计算买单流的变化
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

        # --- 关键修改开始 ---
        raw_ofi = e_bid + e_ask

        # 计算当前盘口的总深度 (买单量 + 卖单量)
        current_depth = bid_v + ask_v

        # 归一化处理 (Normalization)
        # 为什么要改：防止大户挂单导致你的机器人价格瞬间飞走。
        # 效果：无论盘口是 0.1 BTC 还是 100 BTC，OFI 输出都在 -1 到 1 之间。
        if current_depth > 0:
            normalized_ofi = raw_ofi / current_depth
        else:
            normalized_ofi = 0.0

        # 额外加一道锁：为了刷量，我们不希望预测信号太强
        # 强制截断在 -1 和 1 之间，防止极端数据干扰
        return max(min(normalized_ofi, 1.0), -1.0)

    async def _dispatch_orders(self, target_bid, target_ask, allow_buy, allow_sell):
        tasks = []
        if allow_buy:
            tasks.append(self._manage_order_side('BUY', target_bid))
        else:
            if self.active_orders['BUY']: tasks.append(self._cancel_order_side('BUY'))

        if allow_sell:
            tasks.append(self._manage_order_side('SELL', target_ask))
        else:
            if self.active_orders['SELL']: tasks.append(self._cancel_order_side('SELL'))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _manage_order_side(self, side, target_price):
        if self.pending_actions[side]: return

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

            # 阈值过滤，避免频繁改单
            if current_id and current_price > 0:
                diff_ticks = abs(target_price - current_price) / self.tick_size
                if diff_ticks < self.update_threshold_ticks:
                    return

            adapter = self.adapters[self.exchange_name]
            # Cancel old
            if current_id:
                try:
                    await adapter.cancel_order(current_id, symbol=self.symbol)
                except Exception:
                    pass

            # Create new
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
                    # 自动处理 Post-Only 拒单，尝试让一步价格
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

    async def _update_contract_info(self):
        try:
            adapter = self.adapters[self.exchange_name]
            contract_map = getattr(adapter, 'contract_map', {})
            # 尝试多种 key 匹配
            found = contract_map.get(self.symbol)
            if not found:
                for k, v in contract_map.items():
                    if self.symbol in k:
                        found = v
                        break

            if found:
                if 'tick_size' in found:
                    self.tick_size = float(found['tick_size'])
                if self.tick_size > 0:
                    self.quantizer = FastPriceQuantizer(self.tick_size)
                    logger.info(f"📏 Tick Size Updated: {self.tick_size}")
        except Exception:
            pass