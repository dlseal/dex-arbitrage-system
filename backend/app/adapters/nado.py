# backend/app/adapters/nado.py
import asyncio
import logging
import os
import time
import traceback
from decimal import Decimal
from typing import Dict, Optional, List, Any
from cryptography.fernet import Fernet
from app.config import settings

# --- Nado Protocol Imports ---
try:
    from nado_protocol.client import create_nado_client, NadoClientMode
    from nado_protocol.utils.subaccount import SubaccountParams

    # 兼容性处理
    try:
        from nado_protocol.utils.subaccount import Subaccount
    except ImportError:
        Subaccount = SubaccountParams

    from nado_protocol.engine_client.types import OrderParams
    from nado_protocol.utils.bytes32 import subaccount_to_hex
    from nado_protocol.utils.expiration import get_expiration_timestamp
    from nado_protocol.utils.math import to_x18, from_x18
    from nado_protocol.utils.nonce import gen_order_nonce
    from nado_protocol.utils.order import build_appendix, OrderType
    from nado_protocol.engine_client.types.execute import CancelOrdersParams
except ImportError:
    print("⚠️ Warning: nado_protocol not found. NadoAdapter will not work.")
    create_nado_client = None

from .base import BaseExchange

logger = logging.getLogger("NadoAdapter")


class NadoAdapter(BaseExchange):
    def __init__(self, private_key: str = None, mode: str = "MAINNET", subaccount_name: str = "default",
                 symbols: List[str] = None):
        super().__init__("Nado")

        # 密钥解密
        encrypted_key_secret = settings.encrypted_nado_key
        encrypted_key = encrypted_key_secret.get_secret_value() if encrypted_key_secret else None
        master_key = os.getenv('MASTER_KEY')

        if encrypted_key and master_key:
            try:
                cipher = Fernet(master_key.encode())
                self.private_key = cipher.decrypt(encrypted_key.encode()).decode()
                logger.info("🔓 Private key decrypted successfully")
            except Exception as e:
                logger.error(f"❌ Decryption failed: {e} (Check Master Key)")
                raise ValueError("Invalid Master Key")
        else:
            self.private_key = private_key
            if not self.private_key and settings.nado_private_key:
                self.private_key = settings.nado_private_key.get_secret_value()

        self.mode_str = mode.upper()
        self.subaccount_name = subaccount_name
        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]

        self.client = None
        self.owner = None
        self.contract_map = {}

        self._local_positions_cache = {}

        # [新增] WAF 熔断机制
        self.waf_cool_down_until = 0.0

        self._check_env()

    def _check_env(self):
        if not self.private_key:
            logger.error("❌ [Nado] NADO_PRIVATE_KEY is missing!")

    async def initialize(self):
        if not create_nado_client:
            raise ImportError("nado_protocol library is not installed.")

        try:
            mode_map = {
                'MAINNET': NadoClientMode.MAINNET,
                'DEVNET': NadoClientMode.DEVNET,
            }
            client_mode = mode_map.get(self.mode_str, NadoClientMode.MAINNET)

            logger.info(f"⏳ [Nado] Connecting to {self.mode_str}...")
            self.client = create_nado_client(client_mode, self.private_key)
            self.owner = self.client.context.engine_client.signer.address
            logger.info(f"✅ [Nado] Connected! Owner: {self.owner[:6]}... | Subaccount: {self.subaccount_name}")

            await self._load_markets()
            self.is_connected = True

        except Exception as e:
            logger.error(f"❌ [Nado] Init Failed: {e}")
            raise e

    def _check_waf_status(self):
        """检查是否处于熔断冷却期"""
        if time.time() < self.waf_cool_down_until:
            remaining = self.waf_cool_down_until - time.time()
            # 只有剩余时间较长时才频繁打印，防止日志刷屏
            if remaining > 1.0 and int(remaining) % 5 == 0:
                logger.warning(f"🛡️ [WAF Protection] Cooling down... {remaining:.1f}s left")
            return False
        return True

    def _handle_waf_error(self, e: Exception, context: str):
        """处理 Cloudflare 错误，触发熔断"""
        err_str = str(e)
        if "<!DOCTYPE html>" in err_str or "Just a moment" in err_str or "challenge-platform" in err_str:
            # 触发 10秒 熔断
            self.waf_cool_down_until = time.time() + 10.0
            logger.error(f"🚫 [Cloudflare Blocked] {context} failed. Triggering 10s cool-down.")
            return True
        return False

    async def _load_markets(self):
        try:
            loop = asyncio.get_running_loop()
            symbols_map = await loop.run_in_executor(None, self.client.market.get_all_product_symbols)
            all_markets = await loop.run_in_executor(None, self.client.market.get_all_engine_markets)
            perp_products = all_markets.perp_products

            self.contract_map.clear()
            for target in self.target_symbols:
                nado_symbol_str = f"{target.upper()}-PERP"
                product_id = None

                # 兼容对象或字典属性访问
                for sym_obj in symbols_map:
                    s_str = sym_obj.symbol if hasattr(sym_obj, 'symbol') else str(sym_obj)
                    if s_str == nado_symbol_str:
                        product_id = sym_obj.product_id if hasattr(sym_obj, 'product_id') else sym_obj
                        break

                if product_id is None:
                    continue

                current_market = None
                for market in perp_products:
                    if market.product_id == product_id:
                        current_market = market
                        break

                if current_market:
                    tick_size = float(from_x18(current_market.book_info.price_increment_x18))
                    min_size = float(from_x18(current_market.book_info.size_increment))
                    self.contract_map[target] = {
                        'id': int(product_id),
                        'tick_size': tick_size,
                        'min_size': min_size
                    }
                    logger.info(f"   - [Nado] Loaded {target} (ID: {product_id}) | Tick: {tick_size}")

        except Exception as e:
            logger.error(f"❌ [Nado] Market Sync Error: {e}")
            raise e

    async def close_position(self, symbol: str):
        if not self._check_waf_status(): return False
        if symbol not in self.contract_map: return False

        try:
            pid = self.contract_map[symbol]['id']
            subaccount_obj = SubaccountParams(subaccount_owner=self.owner, subaccount_name=self.subaccount_name)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self.client.context.engine_client.close_position(
                    subaccount=subaccount_obj, product_id=int(pid)
                )
            )
            logger.info(f"✅ [Nado] Close Position Sent for {symbol}")
            return True
        except Exception as e:
            if not self._handle_waf_error(e, "ClosePos"):
                logger.error(f"❌ [Nado] Close Pos Error: {e}")
            return False

    async def _fetch_orderbook_impl(self, symbol: str) -> Dict[str, float]:
        if not self._check_waf_status(): return {}
        if symbol not in self.contract_map: return {}

        try:
            pid = self.contract_map[symbol]['id']
            loop = asyncio.get_running_loop()

            if hasattr(self.client.market, 'get_orderbook'):
                ob = await loop.run_in_executor(
                    None,
                    lambda: self.client.market.get_orderbook(product_id=pid)
                )
            elif hasattr(self.client.market, 'get_market_liquidity'):
                ob = await loop.run_in_executor(
                    None,
                    lambda: self.client.market.get_market_liquidity(product_id=pid, depth=10)
                )
            else:
                return {}

            if not ob: return {}

            # 兼容 SDK 返回对象或列表的情况
            best_bid = float(ob.bids[0].price) / 1e18 if hasattr(ob.bids[0], 'price') else float(ob.bids[0][0]) / 1e18
            best_ask = float(ob.asks[0].price) / 1e18 if hasattr(ob.asks[0], 'price') else float(ob.asks[0][0]) / 1e18

            # 使用当前时间，确保不报 Lag
            return {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'ask': best_ask,
                'ts': int(time.time() * 1000)
            }

        except Exception as e:
            if self._handle_waf_error(e, "FetchOB"):
                return {}  # 静默返回
            return {}

    async def _create_order_impl(self, symbol: str, side: str, amount: float, price: Optional[float],
                                 order_type: str, **kwargs) -> str:
        if not self._check_waf_status():
            raise RuntimeError("WAF Cool-down active")

        if symbol not in self.contract_map:
            raise ValueError(f"Unknown symbol: {symbol}")

        market_info = self.contract_map[symbol]
        pid = market_info['id']
        tick_size = market_info['tick_size']

        final_price = 0.0
        if price:
            d_price = Decimal(str(price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)
        else:
            bbo = await self.fetch_orderbook(symbol)
            if not bbo or bbo['ask'] == 0: raise ValueError("Orderbook unavailable")
            final_price = bbo['ask'] * 1.05 if side.upper() == 'BUY' else bbo['bid'] * 0.95

            d_price = Decimal(str(final_price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)

        is_buy = (side.upper() == 'BUY')
        is_post_only = True if order_type != "MARKET" and kwargs.get('post_only') is not False else False

        # [修复] OrderType 设置
        # 参考代码确认了 Post-Only 的用法。
        # 针对 Taker (Bailout) 单，如果 OrderType.GTC 不存在，标准写法通常是 OrderType.LIMIT。
        try:
            if is_post_only:
                appendix = build_appendix(order_type=OrderType.POST_ONLY)
            else:
                # 尝试使用 LIMIT (标准限价单)，如果不存在则回退
                t_type = getattr(OrderType, 'LIMIT', None)
                if not t_type:
                    t_type = getattr(OrderType, 'IOC', None)  # 再次尝试 IOC

                if t_type:
                    appendix = build_appendix(order_type=t_type)
                else:
                    # 极少数情况：如果都没有，可能 build_appendix 不传参数就是标准单
                    # 或者我们使用默认值
                    appendix = build_appendix()
        except Exception as e:
            # 最后的兜底，防止因为枚举问题导致无法平仓
            logger.warning(f"⚠️ [Nado] OrderType resolve failed: {e}, using default appendix")
            appendix = build_appendix(order_type=OrderType.POST_ONLY) if is_post_only else build_appendix()

        # 参考代码使用了更长的过期时间 (30天)，这里我们也适当延长
        expiration = get_expiration_timestamp(60 * 60 * 24 * 30)

        order_params = OrderParams(
            sender=SubaccountParams(subaccount_owner=self.owner, subaccount_name=self.subaccount_name),
            priceX18=to_x18(final_price),
            amount=to_x18(amount) if is_buy else -to_x18(amount),
            expiration=expiration,
            nonce=gen_order_nonce(),
            appendix=appendix
        )

        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.market.place_order({"product_id": int(pid), "order": order_params})
            )
            if not result or not result.data: raise RuntimeError("No data")
            return str(result.data.digest)

        except Exception as e:
            if self._handle_waf_error(e, "PlaceOrder"):
                raise RuntimeError("Cloudflare Blocked")

            # [Fix 2008 Error Noise]
            # 检查 Post-Only 错误，记录为 Warning 而非 Error，减少日志噪音
            err_str = str(e)
            if "2008" in err_str or "post-only" in err_str:
                logger.warning(f"⚠️ [Nado] Post-Only Rejected: {symbol} @ {final_price}")

            raise e

    async def _cancel_order_impl(self, order_id: str, symbol: str) -> bool:
        if not self._check_waf_status(): return False

        try:
            pid = self.contract_map.get(symbol, {}).get('id')
            if not pid: return False

            sender_hex = subaccount_to_hex(SubaccountParams(
                subaccount_owner=self.owner,
                subaccount_name=self.subaccount_name,
            ))
            cancel_params = CancelOrdersParams(
                productIds=[int(pid)], digests=[order_id], sender=sender_hex
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: self.client.market.cancel_orders(cancel_params))
            return True
        except Exception as e:
            if self._handle_waf_error(e, "CancelOrder"):
                return False
            logger.warning(f"⚠️ [Nado] Cancel Failed: {e}")
            return False

    # [修复] 增加 symbols 参数以兼容策略调用
    async def fetch_positions(self, symbols: List[str] = None) -> List[Dict]:
        if not self._check_waf_status(): return []

        try:
            resolved_subaccount = subaccount_to_hex(self.client.context.signer.address, self.subaccount_name)
            loop = asyncio.get_running_loop()
            account_data = await loop.run_in_executor(
                None, lambda: self.client.context.engine_client.get_subaccount_info(resolved_subaccount)
            )
            if not account_data or not hasattr(account_data, 'perp_balances'): return []

            positions = []
            for pos in account_data.perp_balances:
                pid = pos.product_id
                symbol = next((s for s, info in self.contract_map.items() if info['id'] == pid), None)

                # 如果指定了 symbols，进行过滤
                if symbols and symbol not in symbols:
                    continue

                if symbol:
                    size = float(from_x18(pos.balance.amount))
                    if abs(size) > 0:
                        positions.append({'symbol': symbol, 'size': size, 'side': 'BUY' if size > 0 else 'SELL'})
            return positions
        except Exception as e:
            if self._handle_waf_error(e, "FetchPos"):
                return []
            # 记录详细错误以便调试
            logger.error(f"❌ [Nado] Fetch Pos Error: {e}")
            return []

    # 🟢 [调整] 降频后的双轮询循环
    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        logger.info("📡 [Nado] Starting Safe Polling Stream (Anti-WAF Mode)...")

        last_pos_poll = 0
        pos_interval = 2.0  # [降频] 持仓轮询: 2.0s (原1.0s)

        # [降频] 行情轮询目标间隔: 0.5s (原0.1s)
        # 这对于 Cloudflare 保护的 API 来说是比较安全的上限
        tick_interval = 0.5

        while self.is_connected:
            # 如果正在熔断冷却中，暂停轮询
            if not self._check_waf_status():
                await asyncio.sleep(1.0)
                continue

            start_ts = time.time()
            tasks = []

            try:
                # 1. 任务: 获取 Orderbook
                for symbol in self.target_symbols:
                    tasks.append(self._safe_fetch_and_push(symbol, tick_queue))

                # 2. 任务: 获取持仓
                if start_ts - last_pos_poll > pos_interval:
                    tasks.append(self._poll_positions_and_emit_trades(event_queue))
                    last_pos_poll = start_ts

                if tasks:
                    await asyncio.gather(*tasks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Poll Loop Error: {e}")
                await asyncio.sleep(1.0)
                continue

            # 智能休眠 (控制 tick 频率)
            elapsed = time.time() - start_ts
            sleep_time = max(0.05, tick_interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def _poll_positions_and_emit_trades(self, event_queue: asyncio.Queue):
        try:
            current_positions = await self.fetch_positions()
            new_pos_map = {p['symbol']: float(p['size']) for p in current_positions}

            for symbol in self.target_symbols:
                old_size = self._local_positions_cache.get(symbol, 0.0)
                new_size = new_pos_map.get(symbol, 0.0)

                diff = new_size - old_size
                if abs(diff) > 1e-6:
                    trade_side = 'BUY' if diff > 0 else 'SELL'
                    trade_size = abs(diff)

                    trade_event = {
                        'type': 'trade',
                        'exchange': self.name,
                        'symbol': symbol,
                        'side': trade_side,
                        'size': trade_size,
                        'price': 0.0,
                        'ts': time.time() * 1000
                    }
                    logger.info(
                        f"⚡️ [Fill Detected] {symbol} {trade_side} {trade_size} (Pos: {old_size} -> {new_size})")
                    await event_queue.put(trade_event)

                    self._local_positions_cache[symbol] = new_size

        except Exception:
            pass

    async def _safe_fetch_and_push(self, symbol: str, tick_queue: asyncio.Queue):
        try:
            tick = await self._fetch_orderbook_impl(symbol)
            if tick and tick.get('bid') > 0:
                tick['type'] = 'tick'
                try:
                    tick_queue.put_nowait(tick)
                except asyncio.QueueFull:
                    pass
        except Exception:
            pass