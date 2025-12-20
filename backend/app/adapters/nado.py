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

        # 密钥解密逻辑
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

        # [新增] 用于检测数据是否僵死的缓存
        # 格式: {symbol: {'data': tick_dict, 'hash': str, 'local_ts': float}}
        self.last_tick_cache = {}

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
            logger.debug(traceback.format_exc())
            raise e

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

                for sym_obj in symbols_map:
                    s_str = sym_obj.symbol if hasattr(sym_obj, 'symbol') else str(sym_obj)
                    if s_str == nado_symbol_str:
                        product_id = sym_obj.product_id if hasattr(sym_obj, 'product_id') else sym_obj
                        break

                if product_id is None:
                    logger.warning(f"⚠️ [Nado] Symbol {nado_symbol_str} not found.")
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
                else:
                    logger.warning(f"⚠️ [Nado] Details not found for {target}")

        except Exception as e:
            logger.error(f"❌ [Nado] Market Sync Error: {e}")
            raise e

    async def close_position(self, symbol: str):
        if symbol not in self.contract_map:
            return False
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
            logger.error(f"❌ [Nado] Close Pos Error: {e}")
            return False

    async def _fetch_orderbook_impl(self, symbol: str) -> Dict[str, float]:
        if symbol not in self.contract_map:
            return {}

        try:
            pid = self.contract_map[symbol]['id']
            loop = asyncio.get_running_loop()

            # 1. 尝试获取最新 Orderbook
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

            if not ob:
                return {}

            # 2. 解析数据
            best_bid, best_bid_vol = 0.0, 0.0
            if hasattr(ob, 'bids') and len(ob.bids) > 0:
                item = ob.bids[0]
                best_bid = float(item.price) / 1e18 if hasattr(item, 'price') else float(item[0]) / 1e18
                best_bid_vol = float(item.amount) / 1e18 if hasattr(item, 'amount') else float(item[1]) / 1e18

            best_ask, best_ask_vol = 0.0, 0.0
            if hasattr(ob, 'asks') and len(ob.asks) > 0:
                item = ob.asks[0]
                best_ask = float(item.price) / 1e18 if hasattr(item, 'price') else float(item[0]) / 1e18
                best_ask_vol = float(item.amount) / 1e18 if hasattr(item, 'amount') else float(item[1]) / 1e18

            # 3. [关键修复] 陈旧数据检测 (Stale Data Detection)
            # 生成数据指纹 (价格+数量)
            data_fingerprint = f"{best_bid:.6f}_{best_bid_vol:.6f}_{best_ask:.6f}_{best_ask_vol:.6f}"
            current_time = time.time()

            # 检查是否有缓存
            cached = self.last_tick_cache.get(symbol)

            final_ts = 0

            if cached and cached['hash'] == data_fingerprint:
                # ⚠️ 数据完全没变：使用旧的时间戳！
                # 这样如果数据一直不变，ts 就会一直停留在过去
                # 策略层的 lag check 就会报警并停止交易
                final_ts = cached['local_ts']
            else:
                # ✅ 数据变了：更新时间戳
                final_ts = current_time * 1000  # 毫秒
                self.last_tick_cache[symbol] = {
                    'hash': data_fingerprint,
                    'local_ts': final_ts
                }

            return {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'bid_volume': best_bid_vol,
                'ask': best_ask,
                'ask_volume': best_ask_vol,
                'ts': int(final_ts)  # 这里可能返回旧时间
            }

        except Exception as e:
            logger.error(f"Fetch OB Error: {e}")
            return {}

    async def _create_order_impl(self, symbol: str, side: str, amount: float, price: Optional[float],
                                 order_type: str, **kwargs) -> str:
        # ... (保持原有逻辑不变，此处省略以节省篇幅，请保留原文件中的完整实现) ...
        # 建议直接复制你原文件中 _create_order_impl 的内容到这里
        # 为确保完整性，以下是核心调用部分：
        if symbol not in self.contract_map:
            raise ValueError(f"Unknown symbol: {symbol}")

        market_info = self.contract_map[symbol]
        pid = market_info['id']
        tick_size = market_info['tick_size']

        # 价格处理
        final_price = 0.0
        if price:
            d_price = Decimal(str(price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)
        else:
            # 市价单兜底逻辑
            bbo = await self.fetch_orderbook(symbol)
            if not bbo or bbo['ask'] == 0:
                raise ValueError("Orderbook unavailable")
            final_price = bbo['ask'] * 1.05 if side.upper() == 'BUY' else bbo['bid'] * 0.95
            d_price = Decimal(str(final_price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)

        is_buy = (side.upper() == 'BUY')
        is_post_only = True if order_type != "MARKET" and kwargs.get('post_only') is not False else False
        appendix = build_appendix(order_type=OrderType.POST_ONLY) if is_post_only else build_appendix(
            order_type=OrderType.GTC)

        order_params = OrderParams(
            sender=SubaccountParams(subaccount_owner=self.owner, subaccount_name=self.subaccount_name),
            priceX18=to_x18(final_price),
            amount=to_x18(amount) if is_buy else -to_x18(amount),
            expiration=get_expiration_timestamp(60 * 60 * 24),
            nonce=gen_order_nonce(),
            appendix=appendix
        )

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.client.market.place_order({"product_id": int(pid), "order": order_params})
        )
        if not result or not result.data:
            raise RuntimeError("Order placement returned no data")
        return str(result.data.digest)

    async def _cancel_order_impl(self, order_id: str, symbol: str) -> bool:
        # ... (保持原逻辑不变) ...
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
            logger.warning(f"⚠️ [Nado] Cancel Failed: {e}")
            return False

    async def fetch_positions(self) -> List[Dict]:
        # ... (保持原逻辑不变) ...
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
                if symbol:
                    size = float(from_x18(pos.balance.amount))
                    if abs(size) > 0:
                        positions.append({'symbol': symbol, 'size': size, 'side': 'BUY' if size > 0 else 'SELL'})
            return positions
        except Exception:
            return []

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        logger.info("📡 [Nado] Starting Aggressive Polling Stream... (Interval: 0.1s)")
        while self.is_connected:
            start_ts = time.time()
            try:
                tasks = [self._safe_fetch_and_push(symbol, tick_queue) for symbol in self.target_symbols]
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Poll Loop Error: {e}")
                await asyncio.sleep(1.0)
                continue

            elapsed = time.time() - start_ts
            # 如果请求耗时超过0.1s，说明网络慢或计算慢，就不休眠了，尽快下一次
            sleep_time = max(0.01, 0.1 - elapsed)
            await asyncio.sleep(sleep_time)

    async def _safe_fetch_and_push(self, symbol: str, tick_queue: asyncio.Queue):
        try:
            tick = await self.fetch_orderbook(symbol)
            # 只有当 bid 有效时才推送
            if tick and tick.get('bid') > 0:
                tick['type'] = 'tick'
                try:
                    tick_queue.put_nowait(tick)
                except asyncio.QueueFull:
                    pass
        except Exception:
            pass