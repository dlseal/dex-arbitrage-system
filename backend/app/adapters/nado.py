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
    # [新增] 引入 Subaccount 类 (如果 SDK 中 Subaccount 和 SubaccountParams 是同一个或在同模块)
    from nado_protocol.utils.subaccount import SubaccountParams

    # 尝试导入 Subaccount，如果不存在则使用 SubaccountParams 替代 (通常它们是兼容的)
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

        # 1. 优先尝试解密
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
        self._check_env()

    def _check_env(self):
        if not self.private_key:
            logger.error("❌ [Nado] NADO_PRIVATE_KEY is missing!")

    async def initialize(self):
        """Initialize Nado Client and Sync Markets"""
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
                    logger.warning(f"⚠️ [Nado] Symbol {nado_symbol_str} not found on exchange.")
                    continue

                current_market = None
                for market in perp_products:
                    if market.product_id == product_id:
                        current_market = market
                        break

                if current_market:
                    tick_size_x18 = current_market.book_info.price_increment_x18
                    min_size_x18 = current_market.book_info.size_increment

                    tick_size = float(from_x18(tick_size_x18))
                    min_size = float(from_x18(min_size_x18))

                    self.contract_map[target] = {
                        'id': int(product_id),
                        'tick_size': tick_size,
                        'min_size': min_size
                    }
                    logger.info(f"   - [Nado] Loaded {target} (ID: {product_id}) | Tick: {tick_size} | Min: {min_size}")
                else:
                    logger.warning(f"⚠️ [Nado] Market details not found for {target}")

        except Exception as e:
            logger.error(f"❌ [Nado] Market Sync Error: {e}")
            raise e

    # --- [新增] Nado 原生平仓接口 ---
    async def close_position(self, symbol: str):
        """
        调用 Nado SDK 原生 close_position 接口进行市价全平
        """
        if symbol not in self.contract_map:
            logger.error(f"❌ Close Position Failed: Unknown symbol {symbol}")
            return False

        try:
            pid = self.contract_map[symbol]['id']

            # 构造 Subaccount 对象
            # 注意：根据 SDK 定义，Subaccount 可能需要特定的构造方式
            # 这里复用 owner 和 subaccount_name
            subaccount_obj = SubaccountParams(
                subaccount_owner=self.owner,
                subaccount_name=self.subaccount_name
            )

            logger.info(f"🚨 [Nado] Executing Market Close for {symbol} (ID: {pid})...")

            loop = asyncio.get_running_loop()

            # 调用 self.client.context.engine_client.close_position
            # 放在 executor 中运行以防阻塞
            response = await loop.run_in_executor(
                None,
                lambda: self.client.context.engine_client.close_position(
                    subaccount=subaccount_obj,
                    product_id=int(pid)
                )
            )

            if response and hasattr(response, 'status') and response.status == "success":
                logger.info(f"✅ [Nado] Position Closed Successfully for {symbol}!")
                return True
            else:
                # 有些版本可能只返回 hash，视具体 SDK 而定，只要不报错通常即发送成功
                logger.info(f"✅ [Nado] Close Position Request Sent for {symbol}. Resp: {response}")
                return True

        except Exception as e:
            logger.error(f"❌ [Nado] Close Position CRITICAL ERROR: {e}")
            traceback.print_exc()
            return False

    # --- Standard Methods ---

    async def _fetch_orderbook_impl(self, symbol: str) -> Dict[str, float]:
        if symbol not in self.contract_map:
            return {}

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

            if not ob:
                return {}

            best_bid = 0.0
            best_bid_vol = 0.0
            if hasattr(ob, 'bids') and len(ob.bids) > 0:
                bid_item = ob.bids[0]
                if hasattr(bid_item, 'price'):
                    best_bid = float(bid_item.price) / 1e18
                    best_bid_vol = float(bid_item.amount) / 1e18
                else:
                    best_bid = float(bid_item[0]) / 1e18
                    best_bid_vol = float(bid_item[1]) / 1e18

            best_ask = 0.0
            best_ask_vol = 0.0
            if hasattr(ob, 'asks') and len(ob.asks) > 0:
                ask_item = ob.asks[0]
                if hasattr(ask_item, 'price'):
                    best_ask = float(ask_item.price) / 1e18
                    best_ask_vol = float(ask_item.amount) / 1e18
                else:
                    best_ask = float(ask_item[0]) / 1e18
                    best_ask_vol = float(ask_item[1]) / 1e18

            return {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'bid_volume': best_bid_vol,
                'ask': best_ask,
                'ask_volume': best_ask_vol,
                'ts': int(time.time() * 1000)
            }

        except Exception as e:
            logger.error(f"Fetch OB Error: {e}")
            return {}

    async def _create_order_impl(self, symbol: str, side: str, amount: float, price: Optional[float],
                                 order_type: str, **kwargs) -> str:
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
            if not bbo:
                raise ValueError("Orderbook unavailable for market order")
            final_price = bbo['ask'] * 1.05 if side.upper() == 'BUY' else bbo['bid'] * 0.95

            d_price = Decimal(str(final_price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)

        is_buy = (side.upper() == 'BUY')
        is_post_only = True if order_type != "MARKET" and kwargs.get('post_only') is not False else False
        appendix = build_appendix(order_type=OrderType.POST_ONLY) if is_post_only else build_appendix(
            order_type=OrderType.GTC)

        try:
            order_params = OrderParams(
                sender=SubaccountParams(
                    subaccount_owner=self.owner,
                    subaccount_name=self.subaccount_name,
                ),
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

        except Exception as e:
            logger.error(f"❌ [Nado] Place Order Error: {e}")
            raise e

    async def _cancel_order_impl(self, order_id: str, symbol: str) -> bool:
        try:
            pid = self.contract_map.get(symbol, {}).get('id')
            if not pid:
                if self.contract_map:
                    pid = list(self.contract_map.values())[0]['id']
                else:
                    return False

            sender_hex = subaccount_to_hex(SubaccountParams(
                subaccount_owner=self.owner,
                subaccount_name=self.subaccount_name,
            ))

            cancel_params = CancelOrdersParams(
                productIds=[int(pid)],
                digests=[order_id],
                sender=sender_hex
            )

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self.client.market.cancel_orders(cancel_params)
            )
            return True

        except Exception as e:
            logger.warning(f"⚠️ [Nado] Cancel Failed: {e}")
            return False

    async def get_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return {}

    async def fetch_positions(self) -> List[Dict]:
        try:
            resolved_subaccount = subaccount_to_hex(self.client.context.signer.address, self.subaccount_name)
            loop = asyncio.get_running_loop()
            account_data = await loop.run_in_executor(
                None,
                lambda: self.client.context.engine_client.get_subaccount_info(resolved_subaccount)
            )

            if not account_data or not hasattr(account_data, 'perp_balances'):
                return []

            positions = []
            for pos in account_data.perp_balances:
                pid = pos.product_id
                symbol = next((s for s, info in self.contract_map.items() if info['id'] == pid), None)
                if symbol:
                    size = float(from_x18(pos.balance.amount))
                    if abs(size) > 0:
                        positions.append({'symbol': symbol, 'size': size, 'side': 'BUY' if size > 0 else 'SELL'})
            return positions
        except Exception as e:
            logger.error(f"❌ [Nado] Fetch Pos Error: {e}")
            return []

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        """
        [Optimized] 激进的高频轮询模式
        1. 使用 asyncio.gather 并发查询所有币种
        2. 极短的休眠间隔 (0.1s)
        """
        logger.info("📡 [Nado] Starting Aggressive Polling Stream... (Interval: 0.1s)")

        while self.is_connected:
            start_ts = time.time()
            try:
                # 1. 构造任务列表：对所有目标币种并发 fetch_orderbook
                tasks = [self._safe_fetch_and_push(symbol, tick_queue) for symbol in self.target_symbols]

                # 2. 并发执行
                await asyncio.gather(*tasks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Poll Loop Error: {e}")
                await asyncio.sleep(1.0)
                continue

            # 3. 极速休眠控制
            # 计算刚才消耗的时间，保持 0.1s 的节奏，如果请求慢了就不休眠直接下一次
            elapsed = time.time() - start_ts
            sleep_time = max(0.05, 0.1 - elapsed)
            await asyncio.sleep(sleep_time)

    async def _safe_fetch_and_push(self, symbol: str, tick_queue: asyncio.Queue):
        """单个币种的安全抓取，失败不影响整体"""
        try:
            # 直接调用内部 fetch_orderbook (BaseExchange 提供的包装方法)
            # 注意：如果 backoff 限制了频率，这里会自动等待，防止被封 IP
            tick = await self.fetch_orderbook(symbol)
            if tick and tick.get('bid') > 0:
                tick['type'] = 'tick'
                # 使用 put_nowait 防止队列满时阻塞 Loop
                try:
                    tick_queue.put_nowait(tick)
                except asyncio.QueueFull:
                    pass
        except Exception:
            # 吞掉单个币种的错误，防止影响 gather
            pass