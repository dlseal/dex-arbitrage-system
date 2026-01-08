# backend/app/strategies/hft_market_making.py
import asyncio
import logging
import time
import math
from collections import deque
from typing import Dict, Any, Optional

from app.config import settings
from app.core.risk_controller import GlobalRiskController

logger = logging.getLogger("HFT_FARM_PRO")


class OnlineStats:
    """Welford算法实现的流式方差计算器 (高性能版)"""
    __slots__ = ('window_size', 'values', 'sum', 'sq_sum')

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
        # 防止浮点数误差导致负数
        variance = max(0.0, (self.sq_sum / n) - (mean * mean))
        return math.sqrt(variance)


class EMACalculator:
    """指数移动平均计算器"""
    __slots__ = ('alpha', 'value', 'initialized')

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
    """极速价格量化器"""
    __slots__ = ('tick_size', 'inv_tick')

    def __init__(self, tick_size: float):
        self.tick_size = float(tick_size)
        self.inv_tick = 1.0 / self.tick_size if self.tick_size > 0 else 0.0

    def quantize(self, price: float, rounding: str = 'ROUND') -> float:
        """
        :param rounding: 'ROUND', 'FLOOR' (Bid), 'CEILING' (Ask)
        """
        if price <= 0 or self.inv_tick == 0: return 0.0
        scaled = price * self.inv_tick

        if rounding == 'FLOOR':
            return math.floor(scaled) * self.tick_size
        elif rounding == 'CEILING':
            return math.ceil(scaled) * self.tick_size
        else:
            return round(scaled) * self.tick_size


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any], risk_controller: GlobalRiskController = None):
        self.name = "HFT_Farm_Guard_v1"
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

        # --- 🛡️ 强制安全参数覆盖 (Emergency Overrides) ---
        # 无论配置文件怎么写，代码强制执行最小安全标准
        # 刷量策略核心：宁可不成交，不可被套利
        self.min_spread_ticks = max(conf.min_spread_ticks, 16)  # 强制至少 16 ticks (约 0.016%)
        if self.min_spread_ticks < 20:
            logger.warning(f"⚠️ Spread config too low. Forced upgrade to {self.min_spread_ticks} ticks.")

        self.risk_aversion = max(conf.risk_aversion, 0.5)  # 强制高风险厌恶
        self.update_threshold_ticks = max(conf.update_threshold_ticks, 10)  # 强制防抖动

        self.ofi_sensitivity = conf.ofi_sensitivity
        self.volatility_factor = conf.volatility_factor

        # 资金限制
        self.max_pos_usd = conf.max_pos_usd
        self.soft_limit_usd = self.max_pos_usd * 0.8  # 80% 停止同向
        self.hard_limit_usd = self.max_pos_usd * 1.05  # 105% 强制熔断

        # --- 内部状态 ---
        self.tick_size = 0.0
        self.quantizer: Optional[FastPriceQuantizer] = None

        self.inventory = 0.0
        self.inv_lock = asyncio.Lock()

        # 统计模型
        self.mid_price_stats = OnlineStats(window_size=max(conf.window_size, 100))
        self.ofi_ema = EMACalculator(alpha=0.1)  # 更平滑
        self.prev_tick: Optional[Dict] = None

        # 订单状态管理
        # 格式: {'order_id': str, 'price': float, 'ts': float}
        self.active_orders = {"BUY": None, "SELL": None}
        self.pending_actions = {"BUY": False, "SELL": False}
        self.is_bailout_active = False

        self.calc_lock = asyncio.Lock()
        self.is_active = True
        self.pos_sync_time = 0
        self.last_log_ts = 0
        self.trade_count_session = 0

        self._validate_adapter()
        asyncio.create_task(self._initial_setup())

    def _validate_adapter(self):
        if self.exchange_name not in self.adapters:
            logger.error(f"❌ [HFT] Adapter {self.exchange_name} Not Found!")
            self.is_active = False

    async def _initial_setup(self):
        await asyncio.sleep(1.0)
        await self._update_contract_info()
        await self._sync_position(force=True)
        logger.info(
            f"✅ Strategy Started. Safe Spread: {self.min_spread_ticks} ticks | Risk Aversion: {self.risk_aversion}")

    async def on_tick(self, event: dict):
        if not self.is_active or self.is_bailout_active: return

        if event.get('type') != 'tick' or event.get('symbol') != self.symbol:
            if event.get('type') == 'trade':
                await self._on_trade(event)
            return

        # 🛡️ 严格的时效性检查
        now_ms = time.time() * 1000
        tick_ts = event.get('ts', now_ms)
        latency = now_ms - tick_ts

        # 如果数据延迟超过 400ms，视为危险数据，暂停做市
        if latency > 400:
            return

        if self.calc_lock.locked():
            return

        await self._process_tick_logic(event)

    async def _process_tick_logic(self, tick: dict):
        log_payload = None

        async with self.calc_lock:
            try:
                if self.tick_size <= 0:
                    await self._update_contract_info()
                    if self.tick_size <= 0: return

                bid_p = float(tick['bid'])
                ask_p = float(tick['ask'])
                # 防止异常数据
                if bid_p <= 0 or ask_p <= 0 or bid_p > ask_p: return

                bid_v, ask_v = self._extract_volumes(tick)
                mid_price = (bid_p + ask_p) * 0.5

                # --- 1. 更新统计模型 ---
                self.mid_price_stats.update(mid_price)
                ofi = self._calculate_normalized_ofi(bid_p, bid_v, ask_p, ask_v)
                avg_ofi = self.ofi_ema.update(ofi)

                volatility = self.mid_price_stats.get_std_dev()
                # 波动率保底，防止静止市场 spread 收缩过窄
                min_vol = mid_price * 0.0001  # 1 bp floor
                volatility = max(volatility, min_vol)

                # --- 2. 风控检查 ---
                current_inv = self.inventory
                pos_value = current_inv * mid_price

                # 熔断检测
                if abs(pos_value) > self.hard_limit_usd:
                    logger.critical(f"🚨 [RISK] Pos ${pos_value:.0f} > ${self.hard_limit_usd}. BAILOUT START.")
                    self.is_bailout_active = True
                    asyncio.create_task(self._execute_smart_bailout(current_inv))
                    return

                # --- 3. 核心定价模型 (Farm Optimized) ---

                # A. 激进库存偏斜 (Inventory Skew)
                # 逻辑：Inventory * RiskAversion * Volatility * Boost
                # 当持仓接近上限时，Boost 因子指数级增加
                pos_ratio = abs(pos_value) / self.max_pos_usd
                skew_boost = 1.0 + (pos_ratio * 2.0)  # 0%持仓->1x, 100%持仓->3x

                skew = -1.0 * current_inv * self.risk_aversion * volatility * skew_boost

                # 限制 Skew 最大不超过 20 ticks，防止报错
                max_skew = 20 * self.tick_size
                skew = max(-max_skew, min(max_skew, skew))

                # B. OFI 信号微调 (仅在低持仓时启用 OFI 预测，重仓时完全服从库存控制)
                ofi_impact = 0.0
                if pos_ratio < 0.5:
                    ofi_impact = self.ofi_sensitivity * avg_ofi * self.tick_size

                reservation_price = mid_price + skew + ofi_impact

                # C. 动态价差 (Dynamic Spread)
                # 基础价差 + 波动率加成
                # Log 阻尼防止剧烈波动时的价差无限扩大
                half_spread_ticks = (self.min_spread_ticks * 0.5) + \
                                    (self.volatility_factor * math.log1p(volatility / self.tick_size))

                half_spread = half_spread_ticks * self.tick_size

                # 计算原始目标价
                raw_bid = reservation_price - half_spread
                raw_ask = reservation_price + half_spread

                # D. 安全截断 (Clamping)
                # 无论模型怎么算，买单不能高于当前买一价（避免直接吃单 Taker）
                # 卖单同理。因为我们要赚 Maker Rebate。
                # 注意：GRVT 部分模式允许 Post-Only 自动转为挂单，但这里我们在应用层做第一道防线。

                # 额外退让：如果是刷量，我们不需要去抢最前排，我们在买一价后面排队更安全
                # 仅当持仓需要平仓时，才尝试压盘口

                target_bid = min(raw_bid, bid_p)
                target_ask = max(raw_ask, ask_p)

                # 量化价格
                final_bid = self.quantizer.quantize(target_bid, 'FLOOR')
                final_ask = self.quantizer.quantize(target_ask, 'CEILING')

                # 最小价差保护 (防止自成交或交叉)
                if final_ask <= final_bid:
                    center = self.quantizer.quantize(mid_price)
                    final_bid = center - self.tick_size * int(self.min_spread_ticks / 2)
                    final_ask = center + self.tick_size * int(self.min_spread_ticks / 2)

                # --- 4. 订单执行 (Sticky Logic) ---

                # 计算是否允许开仓
                can_buy = pos_value < self.soft_limit_usd
                can_sell = pos_value > -self.soft_limit_usd

                asyncio.create_task(self._dispatch_sticky_orders(
                    final_bid, final_ask, can_buy, can_sell
                ))

                # --- 5. 定时任务 ---
                curr_time = time.time()
                # 3秒同步一次持仓，防止 WS 丢包
                if curr_time - self.pos_sync_time > 3.0:
                    self.pos_sync_time = curr_time
                    asyncio.create_task(self._sync_position())

                # 5秒打印一次心跳
                if curr_time - self.last_log_ts > 5.0:
                    self.last_log_ts = curr_time
                    log_payload = {
                        'mid': mid_price, 'vol': volatility, 'inv': current_inv,
                        'skew': skew, 'spread_t': half_spread_ticks * 2
                    }

            except Exception as e:
                logger.error(f"Logic Error: {e}", exc_info=False)

        if log_payload:
            logger.info(
                f"📊 [Stat] Mid:{log_payload['mid']:.1f} | Vol:{log_payload['vol']:.2f} | "
                f"Inv:{log_payload['inv']:.4f} | Skew:{log_payload['skew']:.2f} | "
                f"Sprd:{log_payload['spread_t']:.1f}t"
            )

    async def _dispatch_sticky_orders(self, target_bid, target_ask, can_buy, can_sell):
        """
        防抖动订单派发逻辑
        只有当 目标价格 与 当前订单价格 偏差超过阈值时才修改，
        或者 仓位风险极高时强制修改。
        """
        tasks = []

        # BUY Side
        if can_buy:
            tasks.append(self._manage_sticky_side('BUY', target_bid))
        else:
            if self.active_orders['BUY']: tasks.append(self._cancel_side('BUY'))

        # SELL Side
        if can_sell:
            tasks.append(self._manage_sticky_side('SELL', target_ask))
        else:
            if self.active_orders['SELL']: tasks.append(self._cancel_side('SELL'))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _manage_sticky_side(self, side, target_price):
        if self.pending_actions[side]: return

        current_order = self.active_orders[side]  # {'id', 'price', 'ts'}

        # 1. 检查是否需要移动订单
        if current_order:
            price_diff_ticks = abs(target_price - current_order['price']) / self.tick_size

            # 如果价格偏差在阈值内，且订单存在时间小于 10秒 (防止订单太旧)，则不动
            # 刷量核心：由于 Maker 费率为负，我们愿意多挂一会儿等待成交，而不是频繁改单消耗 gas/rate limit
            is_urgent = (self.inventory > 0 and side == 'SELL') or (self.inventory < 0 and side == 'BUY')
            threshold = self.update_threshold_ticks * 0.5 if is_urgent else self.update_threshold_ticks

            if price_diff_ticks < threshold:
                return  # Sticky! Don't move.

        # 2. 执行改单
        self.pending_actions[side] = True
        try:
            adapter = self.adapters[self.exchange_name]

            # 先撤旧单
            if current_order:
                try:
                    await adapter.cancel_order(current_order['id'], symbol=self.symbol)
                except Exception:
                    pass  # 忽略撤单失败（可能是已成交）
                self.active_orders[side] = None

            # 再下新单 (带重试逻辑)
            # 如果是刷量，我们尽量拆小单，这里假设 config 已配置好 quantity
            final_price = target_price

            # 尝试两次下单
            for i in range(2):
                try:
                    # ⚠️ 必须开启 Post-Only
                    new_id = await adapter.create_order(
                        symbol=self.symbol,
                        side=side,
                        amount=self.quantity,
                        price=final_price,
                        params={"post_only": True}
                    )

                    if new_id:
                        self.active_orders[side] = {
                            'id': new_id,
                            'price': final_price,
                            'ts': time.time()
                        }
                        break
                except Exception as e:
                    err_msg = str(e).lower()
                    # 处理 Post-Only 碰撞 (Maker 变 Taker)
                    if "post-only" in err_msg or "maker" in err_msg:
                        # 如果是买单，价格太高了，往后退 5 ticks
                        # 如果是卖单，价格太低了，往后退 5 ticks
                        safe_pad = 5 * self.tick_size
                        if side == 'BUY':
                            final_price -= safe_pad
                        else:
                            final_price += safe_pad
                        continue  # Retry with safer price
                    else:
                        logger.error(f"Order Failed {side}: {e}")
                        break

        finally:
            self.pending_actions[side] = False

    async def _cancel_side(self, side):
        if self.pending_actions[side]: return
        self.pending_actions[side] = True
        try:
            order = self.active_orders[side]
            if order:
                await self.adapters[self.exchange_name].cancel_order(order['id'], symbol=self.symbol)
                self.active_orders[side] = None
        except Exception:
            self.active_orders[side] = None
        finally:
            self.pending_actions[side] = False

    async def _execute_smart_bailout(self, start_inv):
        """
        智能平仓逻辑：
        不直接市价砸盘，而是挂在对手价的一定比例位置，尝试快速 Limit 成交。
        """
        adapter = self.adapters[self.exchange_name]
        logger.warning("🛑 Bailout: Canceling all orders...")

        # 1. Cancel All
        try:
            if hasattr(adapter, 'cancel_all_orders'):
                await adapter.cancel_all_orders(symbol=self.symbol)
            self.active_orders = {"BUY": None, "SELL": None}
        except Exception:
            pass

        await asyncio.sleep(0.5)

        # 2. Aggressive Loop
        for i in range(15):  # 最多尝试 15 次
            current_inv = await self._sync_position(force=True)
            if current_inv is None: current_inv = start_inv  # Fallback

            if abs(current_inv) < (self.quantity * 0.5):
                logger.info("✅ Bailout Complete.")
                self.is_bailout_active = False
                return

            # 获取最新价格
            ticker = await adapter.fetch_ticker(self.symbol)
            if not ticker:
                await asyncio.sleep(1)
                continue

            side = 'SELL' if current_inv > 0 else 'BUY'
            size = abs(current_inv)

            # 激进定价：买一价往下打 0.1% (保证作为 Taker 哪怕滑点也能走掉)
            # 但不使用 Market 单，防止插针
            slippage = 0.001
            if side == 'SELL':
                price = float(ticker['bid']) * (1 - slippage)
                price = self.quantizer.quantize(price, 'FLOOR')
            else:
                price = float(ticker['ask']) * (1 + slippage)
                price = self.quantizer.quantize(price, 'CEILING')

            logger.warning(f"🛑 Bailout Round {i}: {side} {size} @ {price}")
            try:
                # 这里不加 post_only，必须走
                await adapter.create_order(self.symbol, side, size, price)
            except Exception as e:
                logger.error(f"Bailout order failed: {e}")

            await asyncio.sleep(2.0)

        logger.critical("❌ Bailout timed out. Please handle manually!")
        self.is_bailout_active = False  # 恢复策略，试图通过做市平仓

    async def _sync_position(self, force=False):
        try:
            adapter = self.adapters[self.exchange_name]
            positions = await adapter.fetch_positions(symbols=[self.symbol])

            # GRVT adapter return logic check
            found_size = 0.0
            for p in positions:
                # 适配不同的 adapter 返回格式
                sym = p.get('symbol') or p.get('instrument')
                if sym and self.symbol in sym:
                    sz = float(p.get('size', 0) or p.get('contracts', 0) or p.get('amount', 0))
                    # 部分交易所返回 side=SHORT size=10, 需转为 -10
                    side = str(p.get('side', '')).upper()
                    if side == 'SHORT':
                        sz = -abs(sz)
                    elif side == 'LONG':
                        sz = abs(sz)
                    found_size = sz
                    break

            async with self.inv_lock:
                # 只有偏差较大时才打印日志，避免刷屏
                if force or abs(self.inventory - found_size) > (self.quantity * 0.5):
                    if force:
                        logger.info(f"🔄 Sync Pos: {found_size}")
                    else:
                        logger.warning(f"⚠️ Pos Correction: {self.inventory} -> {found_size}")
                self.inventory = found_size
            return found_size
        except Exception as e:
            if force: logger.error(f"Sync Error: {e}")
            return None

    def _calculate_normalized_ofi(self, bid_p, bid_v, ask_p, ask_v) -> float:
        """归一化 OFI 计算: 限制在 -1 到 1 之间"""
        if not self.prev_tick:
            self.prev_tick = {'b': bid_p, 'a': ask_p, 'bv': bid_v, 'av': ask_v}
            return 0.0

        prev = self.prev_tick
        e_bid = 0.0
        if bid_p > prev['b']:
            e_bid = bid_v
        elif bid_p < prev['b']:
            e_bid = -prev['bv']
        else:
            e_bid = bid_v - prev['bv']

        e_ask = 0.0
        if ask_p > prev['a']:
            e_ask = prev['av']
        elif ask_p < prev['a']:
            e_ask = -ask_v
        else:
            e_ask = -(ask_v - prev['av'])

        self.prev_tick = {'b': bid_p, 'a': ask_p, 'bv': bid_v, 'av': ask_v}

        raw_ofi = e_bid + e_ask
        depth = bid_v + ask_v
        if depth <= 0: return 0.0

        return max(-1.0, min(1.0, raw_ofi / depth))

    def _extract_volumes(self, tick):
        # 兼容不同交易所的数据结构
        bv = float(tick.get('bid_volume') or 0)
        av = float(tick.get('ask_volume') or 0)
        # 如果顶层没有 volume，尝试从 depth 取
        if bv <= 0 and 'bids' in tick and tick['bids']:
            bv = float(tick['bids'][0][1])
        if av <= 0 and 'asks' in tick and tick['asks']:
            av = float(tick['asks'][0][1])
        return max(0.0001, bv), max(0.0001, av)

    async def _on_trade(self, trade):
        """Websocket Trade 推送更新持仓 (低延迟)"""
        if self.symbol not in trade.get('symbol', ''): return
        try:
            # 只有这里明确是"我"的成交才更新，普通公有流 trade 不更新 inventory
            # 但通常 public trade stream 不包含 user info。
            # 如果这是 UserDataStream 的 fill 事件：
            if trade.get('type') == 'fill' or 'orderId' in trade:
                sz = float(trade.get('size', 0) or trade.get('amount', 0))
                side = trade.get('side', '').upper()
                async with self.inv_lock:
                    if side == 'BUY':
                        self.inventory += sz
                    else:
                        self.inventory -= sz
                self.trade_count_session += 1
                if self.trade_count_session % 10 == 0:
                    logger.info(f"⚡️ Fill #{self.trade_count_session} | Inv: {self.inventory:.4f}")
        except Exception:
            pass

    async def _update_contract_info(self):
        try:
            adapter = self.adapters[self.exchange_name]
            # 假设 adapter 有 get_instrument 或类似方法，这里做通用处理
            if hasattr(adapter, 'fetch_exchange_info'):
                # 这是一个假设的调用，具体取决于 adapter 实现
                pass

            # 如果无法动态获取，使用硬编码兜底 (针对 BTC)
            if self.tick_size <= 0:
                if 'BTC' in self.symbol:
                    self.tick_size = 0.1
                elif 'ETH' in self.symbol:
                    self.tick_size = 0.01
                else:
                    self.tick_size = 0.01

            if self.tick_size > 0 and self.quantizer is None:
                self.quantizer = FastPriceQuantizer(self.tick_size)
        except Exception:
            pass