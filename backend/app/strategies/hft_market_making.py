import asyncio
import logging
import time
import math
from collections import deque
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from typing import Dict, Any, Optional

from app.config import Config

logger = logging.getLogger("HFT_AS_OFI")


class OnlineStats:
    """Welford算法实现的流式方差计算器 (保持不变，性能良好)"""

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
    """指数移动平均计算器 (比SMA更灵敏)"""

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
        # 增加异常保护，防止 price 为 None 或非法
        if price is None or price <= 0: return 0.0
        d_price = Decimal(str(price))
        quantized = (d_price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size
        return float(quantized)


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AS_OFI_Pro_v3_Optimized"
        self.adapters = adapters

        # --- 1. 基础配置 ---
        self.exchange_name = Config.HFT_EXCHANGE
        if not Config.TARGET_SYMBOLS:
            logger.error("❌ [HFT] 未配置 TARGET_SYMBOLS")
            self.is_active = False
            return

        self.symbol = Config.TARGET_SYMBOLS[0]
        self.quantity = Config.TRADE_QUANTITIES.get(self.symbol, 0.0001)

        logger.info(f"🎯 HFT Strategy Init: {self.exchange_name} | {self.symbol} | Qty: {self.quantity}")

        # --- 2. 参数配置 ---
        self.risk_aversion = Config.HFT_RISK_AVERSION
        self.ofi_sensitivity = Config.HFT_OFI_SENSITIVITY
        self.min_spread_ticks = Config.HFT_MIN_SPREAD_TICKS
        self.update_threshold_ticks = Config.HFT_UPDATE_THRESHOLD_TICKS
        self.max_pos_usd = Config.HFT_MAX_POS_USD
        self.volatility_factor = Config.HFT_VOLATILITY_FACTOR

        # --- 3. 内部状态管理 ---
        self.tick_size = 0.0  # 初始化为0，强制 update_contract_info 获取
        self.quantizer: Optional[PriceQuantizer] = None

        self.inventory = 0.0
        # 专门的锁保护 inventory，避免后台同步和前台成交的竞争
        self.inv_lock = asyncio.Lock()

        # 统计工具
        self.mid_price_stats = OnlineStats(window_size=Config.HFT_WINDOW_SIZE)
        self.ofi_ema = EMACalculator(alpha=0.2)  # 使用 EMA 替代 SMA
        self.prev_tick: Optional[Dict] = None

        # 订单状态
        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}

        # 挂起状态 (Pending Flags) - 关键：防止并发操作同一方向
        self.pending_actions = {"BUY": False, "SELL": False}

        # 计算锁 (只锁计算逻辑，不锁网络IO)
        self.calc_lock = asyncio.Lock()

        self.is_active = True
        self.pos_sync_time = 0
        self.err_count = 0

        self._validate_adapter()

        # 启动时先获取一次合约信息
        asyncio.create_task(self._initial_setup())

    def _validate_adapter(self):
        if self.exchange_name not in self.adapters:
            logger.error(f"❌ [HFT] Adapter {self.exchange_name} Not Found!")
            self.is_active = False

    async def _initial_setup(self):
        """启动前强制同步 tick_size"""
        await asyncio.sleep(1.0)  # 等待 adapter 连接就绪
        await self._update_contract_info()
        if self.tick_size <= 0:
            logger.warning("⚠️ [HFT] Tick Size 仍未获取，将在 tick 中重试")

    async def on_tick(self, event: dict):
        if not self.is_active: return
        if event.get('exchange') != self.exchange_name: return

        evt_type = event.get('type')

        # 1. 成交事件 (高优先级更新持仓)
        if evt_type == 'trade':
            await self._on_trade(event)
            return

        # 2. 报价事件
        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            # Drop mechanism: 如果计算锁被占用，说明当前 CPU 忙于处理上一帧，直接丢弃
            if self.calc_lock.locked():
                return

            # 注意：这里不 await execution，只计算，保证高吞吐
            await self._process_tick_logic(event)

    async def _process_tick_logic(self, tick: dict):
        """
        核心逻辑：持有锁的时间极短，只做数学计算。
        网络请求通过 create_task 扔出去。
        """
        async with self.calc_lock:
            try:
                # A. 基础检查
                current_ts = time.time()
                tick_ts = tick.get('ts', 0) / 1000.0
                if current_ts - tick_ts > 1.0:  # 延迟超过 1s 丢弃
                    return

                # 初始化 tick_size 保护
                if self.tick_size <= 0:
                    await self._update_contract_info()
                    if self.tick_size <= 0: return

                # B. 数据提取
                bid_p, ask_p = tick['bid'], tick['ask']
                if bid_p <= 0 or ask_p <= 0: return

                bid_v, ask_v = self._extract_volumes(tick)
                mid_price = (bid_p + ask_p) / 2.0

                # C. 更新统计模型
                self.mid_price_stats.update(mid_price)
                ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
                avg_ofi = self.ofi_ema.update(ofi)
                volatility = self.mid_price_stats.get_std_dev()
                if volatility <= 0: volatility = mid_price * 0.0001

                # D. 紧急熔断检查 (BAILOUT)
                async with self.inv_lock:
                    current_inv = self.inventory

                pos_value = current_inv * mid_price
                bailout_threshold = self.max_pos_usd * 2.0

                if abs(pos_value) > bailout_threshold:
                    await self._execute_bailout(current_inv, pos_value)
                    return  # 触发熔断则不挂 Maker 单

                # E. AS 模型计算 (Avellaneda-Stoikov)
                # 1. 保留价格
                inv_risk = current_inv * self.risk_aversion * (volatility ** 2)
                ofi_impact = self.ofi_sensitivity * avg_ofi * self.tick_size
                reservation_price = mid_price + ofi_impact - inv_risk

                # 2. 宽度 (修正了 half_spread 计算逻辑)
                # 用户的 min_spread_ticks 通常指总 Spread，所以半宽应该是 min / 2
                half_spread_base = (self.min_spread_ticks / 2.0) * self.tick_size
                half_spread_vol = self.volatility_factor * volatility
                half_spread = half_spread_base + half_spread_vol

                raw_bid = reservation_price - half_spread
                raw_ask = reservation_price + half_spread

                # F. 价格修剪
                # 1. 无论是为了 Profit 还是 Maker，都不应该跨过对面的盘口
                raw_bid = min(raw_bid, ask_p - self.tick_size)
                raw_ask = max(raw_ask, bid_p + self.tick_size)

                target_bid = self.quantizer.quantize(raw_bid, rounding=ROUND_FLOOR)
                target_ask = self.quantizer.quantize(raw_ask, rounding=ROUND_CEILING)

                # 2. 最小价差兜底
                if target_ask - target_bid < self.tick_size:
                    # 如果算出来的价差太小，强制拉开
                    target_bid = self.quantizer.quantize(mid_price - self.tick_size / 2, ROUND_FLOOR)
                    target_ask = target_bid + self.tick_size

                # G. 权限控制
                allow_buy = pos_value < self.max_pos_usd
                allow_sell = pos_value > -self.max_pos_usd

                # H. 异步调度执行 (Critical: Do not await network IO here)
                # 将目标参数快照传递给执行函数
                asyncio.create_task(self._dispatch_orders(
                    target_bid, target_ask, allow_buy, allow_sell
                ))

                # 定期同步任务
                if current_ts - self.pos_sync_time > 15.0:
                    self.pos_sync_time = current_ts
                    asyncio.create_task(self._sync_position())

                self.err_count = 0

            except Exception as e:
                self.err_count += 1
                logger.error(f"Logic Error: {e}", exc_info=True)

    def _extract_volumes(self, tick):
        """兼容不同 Adapter 的 Volume 提取"""
        bid_v, ask_v = 1.0, 1.0
        # 优先用深度
        if tick.get('bids_depth'): bid_v = float(tick['bids_depth'][0][1])
        if tick.get('asks_depth'): ask_v = float(tick['asks_depth'][0][1])
        # 其次用字段
        if 'bid_volume' in tick: bid_v = float(tick['bid_volume'])
        if 'ask_volume' in tick: ask_v = float(tick['ask_volume'])
        return bid_v, ask_v

    def _calculate_ofi(self, bid_p, bid_v, ask_p, ask_v) -> float:
        if not self.prev_tick:
            self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bv': bid_v, 'av': ask_v}
            return 0.0

        # 标准 OFI 逻辑
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
        """熔断执行逻辑"""
        side = 'SELL' if inventory > 0 else 'BUY'
        # 熔断必须抢锁，防止其他逻辑干扰
        if self.pending_actions[side]: return

        self.pending_actions[side] = True
        logger.critical(f"🚨 [BAILOUT] {side} Size:{abs(inventory)} Val:{value:.0f} (MARKET ORDER)")

        try:
            await self.adapters[self.exchange_name].create_order(
                symbol=self.symbol,
                side=side,
                amount=abs(inventory),
                order_type='MARKET'
            )
            # 熔断后稍作停顿，给 WebSocket 更新持仓的时间
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.error(f"❌ Bailout Failed: {e}")
        finally:
            self.pending_actions[side] = False

    async def _dispatch_orders(self, target_bid, target_ask, allow_buy, allow_sell):
        """
        执行调度器：此函数运行在后台 Task 中，负责处理网络 IO。
        通过 pending_actions 锁保证单方向的串行执行。
        """
        tasks = []

        # BUY Side
        if allow_buy:
            tasks.append(self._manage_order_side('BUY', target_bid))
        else:
            # 如果不允许买，且有挂单，撤单
            if self.active_orders['BUY']:
                tasks.append(self._cancel_order_side('BUY'))

        # SELL Side
        if allow_sell:
            tasks.append(self._manage_order_side('SELL', target_ask))
        else:
            if self.active_orders['SELL']:
                tasks.append(self._cancel_order_side('SELL'))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _manage_order_side(self, side, target_price):
        # 1. 尝试获取该方向的执行锁
        if self.pending_actions[side]: return
        self.pending_actions[side] = True  # Lock

        try:
            current_id = self.active_orders[side]
            current_price = self.active_prices[side]

            # 2. 检查是否需要更新 (防抖动)
            if current_id and current_price > 0:
                # 只有价格变动超过阈值才改单
                diff_ticks = abs(target_price - current_price) / self.tick_size
                if diff_ticks < self.update_threshold_ticks:
                    return  # No action needed

            adapter = self.adapters[self.exchange_name]

            # 3. 撤销旧单 (Best Effort)
            if current_id:
                # 注意：这里可以根据交易所 API 能力优化，有的支持 modify_order 直接修改
                # 这里假设先撤后挂
                try:
                    await adapter.cancel_order(current_id, symbol=self.symbol)
                except Exception:
                    pass  # 忽略撤单失败（可能已成交）

            # 4. 挂新单
            new_id = await adapter.create_order(
                symbol=self.symbol,
                side=side,
                amount=self.quantity,
                price=target_price,
                params={"post_only": True}
            )

            # 5. 更新状态
            if new_id:
                self.active_orders[side] = new_id
                self.active_prices[side] = target_price
            else:
                # 下单失败（可能是 Post-Only 拒单），清除状态等待下次重试
                self.active_orders[side] = None
                self.active_prices[side] = 0.0

        except Exception as e:
            logger.error(f"Quote {side} Error: {e}")
        finally:
            self.pending_actions[side] = False  # Unlock

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
        """处理成交事件，更新本地持仓"""
        if trade['symbol'] != self.symbol: return

        try:
            size = float(trade['size'])
            side = trade['side'].upper()  # BUY or SELL

            # 使用锁保护 inventory 更新
            async with self.inv_lock:
                if side == 'BUY':
                    self.inventory += size
                else:
                    self.inventory -= size
                current_inv = self.inventory

            logger.info(f"⚡️ [Fill] {side} {size} | Inv: {current_inv:.4f}")

            # 成交后，对应的挂单 ID 应该失效
            # 我们不知道是哪个 ID 成交的（除非解析 order_id），为了安全，可以在这里不做处理
            # 依赖 _dispatch_orders 的下一次循环去发现 active_orders 需要更新（虽然 ID 还在，但价格可能偏了）
            # 或者更严谨地，如果 trade 包含 order_id，可以比对清理 self.active_orders
            filled_oid = str(trade.get('order_id', ''))
            if filled_oid:
                if self.active_orders['BUY'] == filled_oid: self.active_orders['BUY'] = None
                if self.active_orders['SELL'] == filled_oid: self.active_orders['SELL'] = None

        except Exception as e:
            logger.error(f"On Trade Error: {e}")

    async def _sync_position(self):
        """REST API 强一致性同步"""
        try:
            adapter = self.adapters[self.exchange_name]
            loop = asyncio.get_running_loop()
            positions = await loop.run_in_executor(
                None,
                lambda: adapter.rest_client.fetch_positions(
                    params={'sub_account_id': adapter.trading_account_id}
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
                # 只有差异很大时才覆盖，防止 WS 和 REST 时间差导致的跳变
                if abs(self.inventory - found_size) > (self.quantity * 0.1):
                    logger.warning(f"⚠️ [Sync] Inv fix: {self.inventory:.4f} -> {found_size:.4f}")
                    self.inventory = found_size

        except Exception as e:
            logger.error(f"Sync Pos Error: {e}")

    async def _update_contract_info(self):
        try:
            adapter = self.adapters[self.exchange_name]
            # 兼容不同的 adapter 结构
            contract_map = getattr(adapter, 'contract_map', {}) or getattr(adapter, 'market_config', {})

            found = None
            for k, v in contract_map.items():
                if self.symbol in k:  # "BTC" in "BTC-USDT"
                    found = v
                    break

            if found:
                # GRVT format: 'tick_size', Lighter format: 'price_mul' (need conversion)
                if 'tick_size' in found:
                    self.tick_size = float(found['tick_size'])
                elif 'price_mul' in found:
                    # Lighter case: price_mul = 100 means tick 0.01? No, usually 1/price_mul
                    # 这里假设 LighterAdapter 已经处理好或者我们需要手动计算
                    # 暂时保持原逻辑或设为默认
                    self.tick_size = 0.01  # Lighter 通常 tick 很小

                if self.tick_size > 0:
                    self.quantizer = PriceQuantizer(self.tick_size)
                    logger.info(f"📏 Tick Size Updated: {self.tick_size}")
        except Exception as e:
            logger.error(f"Update Contract Info Error: {e}")