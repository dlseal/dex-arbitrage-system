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


class PriceQuantizer:
    """处理价格精度，避免浮点数误差"""

    def __init__(self, tick_size: float):
        self.tick_size = Decimal(str(tick_size))

    def quantize(self, price: float, rounding=ROUND_FLOOR) -> float:
        d_price = Decimal(str(price))
        # 严格按照 tick_size 对齐
        quantized = (d_price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size
        return float(quantized)


class HFTMarketMakingStrategy:
    def __init__(self, adapters: Dict[str, Any]):
        self.name = "AS_OFI_Pro_v2_Fixed"
        self.adapters = adapters

        # 1. 基础配置
        self.exchange_name = Config.HFT_EXCHANGE

        if not Config.TARGET_SYMBOLS:
            logger.error("❌ 未配置 TARGET_SYMBOLS，无法启动策略")
            self.is_active = False
            return

        self.symbol = Config.TARGET_SYMBOLS[0]
        # 默认数量
        self.quantity = Config.TRADE_QUANTITIES.get(
            self.symbol,
            Config.TRADE_QUANTITIES.get("DEFAULT", 0.0001)
        )

        logger.info(f"🎯 HFT 策略启动: {self.exchange_name} | {self.symbol} | Qty: {self.quantity}")

        # 2. AS 模型参数
        self.risk_aversion = Config.HFT_RISK_AVERSION
        self.ofi_sensitivity = Config.HFT_OFI_SENSITIVITY
        self.min_spread_ticks = Config.HFT_MIN_SPREAD_TICKS
        self.update_threshold_ticks = Config.HFT_UPDATE_THRESHOLD_TICKS
        self.max_pos_usd = Config.HFT_MAX_POS_USD
        self.volatility_factor = Config.HFT_VOLATILITY_FACTOR

        # 3. 内部状态
        self.tick_size = 0.01
        self.quantizer = PriceQuantizer(self.tick_size)
        self.inventory = 0.0

        # 统计工具
        self.mid_price_stats = OnlineStats(window_size=Config.HFT_WINDOW_SIZE)
        self.ofi_window = deque(maxlen=50)

        # 上一帧数据 (用于 OFI 计算)
        self.prev_tick: Optional[Dict] = None

        # 订单状态管理
        self.active_orders = {"BUY": None, "SELL": None}
        self.active_prices = {"BUY": 0.0, "SELL": 0.0}

        # 挂起状态锁 (防止重复下单)
        self.pending_actions = {"BUY": False, "SELL": False}

        self.lock = asyncio.Lock()
        self.is_active = True
        self.pos_sync_time = 0
        self.err_count = 0

        self._validate_adapter()

    def _validate_adapter(self):
        if self.exchange_name not in self.adapters:
            logger.error(f"❌ Adapter {self.exchange_name} 未加载！")
            self.is_active = False

    async def on_tick(self, event: dict):
        if not self.is_active: return

        # 路由事件
        if event.get('exchange') != self.exchange_name: return

        evt_type = event.get('type')

        # 1. 成交事件 -> 更新持仓
        if evt_type == 'trade':
            await self._on_trade(event)
            return

        # 2. 报价事件 -> 计算信号 & 调整挂单
        if evt_type == 'tick' and event.get('symbol') == self.symbol:
            # Drop mechanism: 丢弃积压的帧
            if self.lock.locked(): return

            async with self.lock:
                try:
                    await self._process_tick(event)
                    self.err_count = 0  # 成功执行则重置错误计数
                except Exception as e:
                    self.err_count += 1
                    logger.error(f"Tick Process Error: {e}", exc_info=True)
                    if self.err_count > 10:
                        logger.critical("❌ 连续报错超过10次，策略由于安全原因暂停")
                        self.is_active = False

    async def _process_tick(self, tick: dict):
        """
        处理 Tick 数据的主逻辑
        包含：延迟检查 -> 数据提取 -> 🚨硬止损检查 -> 信号计算 -> 挂单执行
        """
        current_ts = time.time()
        tick_ts = tick.get('ts', 0) / 1000.0

        # --- A. 延迟熔断 (Latency Guard) ---
        # 如果数据延迟超过 800ms，视为不新鲜，存在套利风险
        latency = current_ts - tick_ts
        if latency > 0.8:
            if latency > 3.0: logger.warning(f"⚠️ [High Latency] {latency * 1000:.0f}ms. Skipping.")
            return

        # 初始化 Tick Size (仅运行一次)
        if self.tick_size == 0.01:
            await self._update_contract_info()

        # 提取基础数据
        bid_p = tick['bid']
        ask_p = tick['ask']

        # --- B. 增强型 Volume 提取 (兼容不同 Adapter 格式) ---
        bid_v = 1.0
        ask_v = 1.0

        # 1. 尝试从深度快照提取 (Lighter Adapter 格式)
        if 'bids_depth' in tick and tick['bids_depth']:
            try:
                bid_v = float(tick['bids_depth'][0][1])
            except:
                pass
        if 'asks_depth' in tick and tick['asks_depth']:
            try:
                ask_v = float(tick['asks_depth'][0][1])
            except:
                pass

        # 2. 尝试从字段提取 (GRVT Adapter 修改后格式)
        if 'bid_volume' in tick: bid_v = float(tick['bid_volume'])
        if 'ask_volume' in tick: ask_v = float(tick['ask_volume'])

        mid_price = (bid_p + ask_p) / 2.0
        self.mid_price_stats.update(mid_price)

        # =========================================================
        # 🚨 [新增功能] 硬止损 (Hard Bailout / Emergency Stop)
        # =========================================================
        # 设定熔断阈值为最大持仓限制的 2 倍
        # 例如：最大持仓 $1000，如果因单边行情持仓堆积到 $2000，立即市价跑路
        bailout_threshold = self.max_pos_usd * 2.0
        current_pos_value = self.inventory * mid_price

        # 情况1: 多头爆仓 (库存积压，价格暴跌，需市价卖出)
        if current_pos_value > bailout_threshold:
            logger.critical(
                f"🚨 [BAILOUT] 多头严重超限! Val:${current_pos_value:.0f} > Lim:${bailout_threshold:.0f} -> 触发市价清仓!")

            # 1. 标记方向锁，防止后续逻辑或其他线程干扰
            self.pending_actions['SELL'] = True

            # 2. 发送市价卖单 (Market Sell)
            # 注意：使用 asyncio.create_task 异步发送，确保不阻塞
            asyncio.create_task(self.adapters[self.exchange_name].create_order(
                symbol=self.symbol,
                side='SELL',
                amount=abs(self.inventory),
                order_type='MARKET'
            ))

            # 3. ⛔ 立即返回，跳过后续所有 AS 计算和挂单逻辑
            return

            # 情况2: 空头爆仓 (持有大量空单，价格暴涨，需市价买回)
        elif current_pos_value < -bailout_threshold:
            logger.critical(
                f"🚨 [BAILOUT] 空头严重超限! Val:${current_pos_value:.0f} < -${bailout_threshold:.0f} -> 触发市价清仓!")

            self.pending_actions['BUY'] = True

            asyncio.create_task(self.adapters[self.exchange_name].create_order(
                symbol=self.symbol,
                side='BUY',
                amount=abs(self.inventory),
                order_type='MARKET'
            ))
            return
            # =========================================================

        # --- C. 计算 OFI (Order Flow Imbalance) ---
        ofi = self._calculate_ofi(bid_p, bid_v, ask_p, ask_v)
        self.ofi_window.append(ofi)
        avg_ofi = sum(self.ofi_window) / max(1, len(self.ofi_window))

        # --- D. Avellaneda-Stoikov 模型计算 ---
        volatility = self.mid_price_stats.get_std_dev()
        # 防止波动率为0导致Spread消失
        if volatility <= 0: volatility = mid_price * 0.0001

        # 1. 计算保留价格 (Reservation Price)
        # 考虑库存风险 (inv_risk) 和 OFI 信号冲击 (ofi_impact)
        inv_risk = self.inventory * self.risk_aversion * (volatility ** 2)
        ofi_impact = self.ofi_sensitivity * avg_ofi * self.tick_size

        reservation_price = mid_price + ofi_impact - inv_risk

        # 2. 计算最优 Spread
        half_spread = (self.min_spread_ticks * self.tick_size) + (self.volatility_factor * volatility)

        raw_bid = reservation_price - half_spread
        raw_ask = reservation_price + half_spread

        # --- E. 价格修剪与量化 (Sanity Check) ---
        # 1. 防倒挂：Maker 买价不能高于市场卖一，卖价不能低于市场买一
        raw_bid = min(raw_bid, ask_p - self.tick_size)
        raw_ask = max(raw_ask, bid_p + self.tick_size)

        # 2. 精度对齐：使用 Decimal 工具类量化价格
        target_bid = self.quantizer.quantize(raw_bid, rounding=ROUND_FLOOR)
        target_ask = self.quantizer.quantize(raw_ask, rounding=ROUND_CEILING)

        # 3. 最小价差保护：防止 Ask <= Bid
        if target_ask - target_bid < self.tick_size:
            target_ask = target_bid + self.tick_size

        # 4. 正常持仓限制 (Soft Limit)
        # 如果未触发硬止损，但超过正常 MaxPos，则只允许平仓方向挂单
        pos_value = self.inventory * mid_price
        allow_buy = pos_value < self.max_pos_usd
        allow_sell = pos_value > -self.max_pos_usd

        # --- F. 执行挂单 ---
        await self._execute_quotes(target_bid, target_ask, allow_buy, allow_sell)

        # 定期同步持仓 (每15秒)
        if current_ts - self.pos_sync_time > 15.0:
            asyncio.create_task(self._sync_position())
            self.pos_sync_time = current_ts

    def _calculate_ofi(self, bid_p, bid_v, ask_p, ask_v) -> float:
        """
        标准 OFI 计算 (Cont et al.)
        """
        if not self.prev_tick:
            self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bid_v': bid_v, 'ask_v': ask_v}
            return 0.0

        prev_bid_p = self.prev_tick['bid']
        prev_bid_v = self.prev_tick['bid_v']
        prev_ask_p = self.prev_tick['ask']
        prev_ask_v = self.prev_tick['ask_v']

        # Bid Side Flux
        if bid_p > prev_bid_p:
            e_bid = bid_v
        elif bid_p < prev_bid_p:
            e_bid = -prev_bid_v
        else:
            e_bid = bid_v - prev_bid_v

        # Ask Side Flux
        if ask_p > prev_ask_p:
            e_ask = prev_ask_v
        elif ask_p < prev_ask_p:
            e_ask = -ask_v
        else:
            e_ask = -(ask_v - prev_ask_v)

            # 更新状态
        self.prev_tick = {'bid': bid_p, 'ask': ask_p, 'bid_v': bid_v, 'ask_v': ask_v}

        return e_bid + e_ask

    async def _execute_quotes(self, target_bid, target_ask, allow_buy, allow_sell):
        tasks = []

        # 买单逻辑
        if allow_buy:
            tasks.append(self._manage_single_side('BUY', target_bid))
        else:
            if self.active_orders['BUY']:
                tasks.append(self._cancel_order('BUY'))

        # 卖单逻辑
        if allow_sell:
            tasks.append(self._manage_single_side('SELL', target_ask))
        else:
            if self.active_orders['SELL']:
                tasks.append(self._cancel_order('SELL'))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _manage_single_side(self, side, target_price):
        # 1. 锁检查：如果该方向正在操作中，跳过
        if self.pending_actions[side]: return

        current_id = self.active_orders[side]
        current_price = self.active_prices[side]

        # 2. 检查是否需要更新 (Threshold Check)
        if current_id and current_price > 0:
            diff_ticks = abs(target_price - current_price) / self.tick_size
            if diff_ticks < self.update_threshold_ticks:
                return  # 变动太小，忽略

        # 锁定状态
        self.pending_actions[side] = True

        try:
            adapter = self.adapters[self.exchange_name]

            # A. 如果有旧单，先尝试安全撤销 (Fire & Forget)
            if current_id:
                asyncio.create_task(self._safe_cancel(side, current_id))

            # B. 发送新单 (Post Only)
            new_id = await adapter.create_order(
                symbol=self.symbol,
                side=side,
                amount=self.quantity,
                price=target_price,
                params={"post_only": True}
            )

            if new_id:
                self.active_orders[side] = new_id
                self.active_prices[side] = target_price
            else:
                self.active_orders[side] = None
                self.active_prices[side] = 0.0

        except Exception as e:
            logger.error(f"Quote {side} Error: {e}")
            self.active_orders[side] = None
        finally:
            self.pending_actions[side] = False

    async def _safe_cancel(self, side, order_id):
        """安全撤单逻辑"""
        try:
            await self.adapters[self.exchange_name].cancel_order(order_id, symbol=self.symbol)
        except Exception as e:
            if "not found" in str(e).lower() or "filled" in str(e).lower():
                pass
            else:
                logger.warning(f"Cancel {side} warning: {e}")

    async def _cancel_order(self, side):
        oid = self.active_orders[side]
        if oid and not self.pending_actions[side]:
            self.pending_actions[side] = True
            try:
                await self._safe_cancel(side, oid)
                self.active_orders[side] = None
                self.active_prices[side] = 0.0
            finally:
                self.pending_actions[side] = False

    async def _on_trade(self, trade):
        if trade['symbol'] != self.symbol: return

        size = float(trade['size'])
        if trade['side'].upper() == 'BUY':
            self.inventory += size
        else:
            self.inventory -= size

        logger.info(f"⚡️ Fill {trade['side']} {size} | Inv: {self.inventory:.4f}")

    async def _sync_position(self):
        """API 强一致性同步"""
        try:
            adapter = self.adapters[self.exchange_name]
            if hasattr(adapter, 'rest_client'):
                loop = asyncio.get_running_loop()
                positions = await loop.run_in_executor(
                    None,
                    lambda: adapter.rest_client.fetch_positions(
                        params={'sub_account_id': adapter.trading_account_id}
                    )
                )

                target_found = False
                for p in positions:
                    inst = p.get('instrument') or p.get('symbol') or ""
                    if self.symbol in inst:
                        size = float(p.get('size', 0) or p.get('contracts', 0))
                        side = p.get('side', '').upper()
                        # 修正空单符号
                        if side == 'SHORT' and size > 0:
                            size = -size
                        elif side == 'LONG' and size < 0:
                            size = abs(size)

                        self.inventory = size
                        target_found = True
                        break

                if not target_found:
                    self.inventory = 0.0

        except Exception as e:
            logger.error(f"Sync Pos Error: {e}")

    async def _update_contract_info(self):
        try:
            adapter = self.adapters[self.exchange_name]
            if hasattr(adapter, 'contract_map'):
                found = None
                for k, v in adapter.contract_map.items():
                    if self.symbol in k:
                        found = v
                        break

                if found:
                    raw_tick = found.get('tick_size', 0.01)
                    self.tick_size = float(raw_tick)
                    self.quantizer = PriceQuantizer(self.tick_size)
                    logger.info(f"📏 Tick Size Updated: {self.tick_size}")
        except Exception as e:
            logger.error(f"Tick Size Update Failed: {e}")