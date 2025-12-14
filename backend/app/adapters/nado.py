import asyncio
import logging
import os
import time
import traceback
from decimal import Decimal
from typing import Dict, Optional, List, Any
from cryptography.fernet import Fernet

# --- Nado Protocol Imports ---
try:
    from nado_protocol.client import create_nado_client, NadoClientMode
    from nado_protocol.utils.subaccount import SubaccountParams
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
        encrypted_key = os.getenv('ENCRYPTED_NADO_KEY')
        master_key = os.getenv('MASTER_KEY')

        if encrypted_key and master_key:
            try:
                cipher = Fernet(master_key.encode())
                # 解密得到原始私钥字符串
                self.private_key = cipher.decrypt(encrypted_key.encode()).decode()
                logger.info("🔓 私钥解密成功")
            except Exception as e:
                logger.error(f"❌ 私钥解密失败: {e} (请检查 Master Key 是否正确)")
                # 解密失败不仅要报错，还应该阻止程序继续运行，防止用空密钥发单
                raise ValueError("Invalid Master Key")
        else:
            # 2. 回退到明文读取 仅开发使用
            self.private_key = private_key or os.getenv("NADO_PRIVATE_KEY")
        self.mode_str = mode.upper()
        self.subaccount_name = subaccount_name
        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]

        self.client = None
        self.owner = None

        # Maps symbol (e.g., 'BTC') -> {'id': int, 'tick_size': float, 'min_size': float}
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
            # 1. Initialize Client
            mode_map = {
                'MAINNET': NadoClientMode.MAINNET,
                'DEVNET': NadoClientMode.DEVNET,
            }
            client_mode = mode_map.get(self.mode_str, NadoClientMode.MAINNET)

            logger.info(f"⏳ [Nado] Connecting to {self.mode_str}...")

            # Using official SDK to create client
            self.client = create_nado_client(client_mode, self.private_key)
            self.owner = self.client.context.engine_client.signer.address

            logger.info(f"✅ [Nado] Connected! Owner: {self.owner[:6]}... | Subaccount: {self.subaccount_name}")

            # 2. Sync Markets
            await self._load_markets()
            self.is_connected = True

        except Exception as e:
            logger.error(f"❌ [Nado] Init Failed: {e}")
            logger.debug(traceback.format_exc())
            raise e

    async def _load_markets(self):
        """Fetch Contract Attributes (IDs, Tick Sizes)"""
        try:
            # Get all symbols and markets
            # Note: Assuming SDK methods are synchronous, wrapping them might be needed if they block
            loop = asyncio.get_running_loop()

            symbols_map = await loop.run_in_executor(None, self.client.market.get_all_product_symbols)
            all_markets = await loop.run_in_executor(None, self.client.market.get_all_engine_markets)
            perp_products = all_markets.perp_products

            self.contract_map.clear()

            for target in self.target_symbols:
                nado_symbol_str = f"{target.upper()}-PERP"
                product_id = None

                # 1. Find Product ID
                for sym_obj in symbols_map:
                    s_str = sym_obj.symbol if hasattr(sym_obj, 'symbol') else str(sym_obj)
                    if s_str == nado_symbol_str:
                        product_id = sym_obj.product_id if hasattr(sym_obj, 'product_id') else sym_obj
                        break

                if product_id is None:
                    logger.warning(f"⚠️ [Nado] Symbol {nado_symbol_str} not found on exchange.")
                    continue

                # 2. Find Market Info (Tick Size)
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

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """Fetch BBO with Volume for HFT"""
        if symbol not in self.contract_map:
            return {}

        try:
            pid = self.contract_map[symbol]['id']

            loop = asyncio.get_running_loop()
            ob = await loop.run_in_executor(
                None,
                lambda: self.client.context.engine_client.get_orderbook(ticker_id=pid, depth=1)
            )

            if not ob or not ob.bids or not ob.asks:
                return {}

            # Nado returns list of [price, size]
            best_bid = float(ob.bids[0][0])
            best_bid_vol = float(ob.bids[0][1])  # 获取买一量

            best_ask = float(ob.asks[0][0])
            best_ask_vol = float(ob.asks[0][1])  # 获取卖一量

            return {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'bid_volume': best_bid_vol,  # HFT 需要此字段
                'ask': best_ask,
                'ask_volume': best_ask_vol,  # HFT 需要此字段
                'ts': int(time.time() * 1000)
            }
        except Exception as e:
            # Suppress frequent logs for polling errors
            return {}

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT", params: Dict = None) -> Optional[str]:
        """
        Place Order using Nado SDK OrderParams
        """
        if symbol not in self.contract_map:
            logger.error(f"❌ [Nado] Unknown symbol: {symbol}")
            return None

        market_info = self.contract_map[symbol]
        pid = market_info['id']
        tick_size = market_info['tick_size']

        # 1. Price Logic
        final_price = 0.0
        if price:
            d_price = Decimal(str(price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)
        else:
            # If Market order (price=None), we need to fetch BBO to set a marketable limit price
            # or rely on SDK market order if supported. Here we simulate market with aggressive limit.
            bbo = await self.fetch_orderbook(symbol)
            if not bbo:
                logger.error("❌ [Nado] Cannot place market order: Orderbook unavailable")
                return None

            if side.upper() == 'BUY':
                final_price = bbo['ask'] * 1.05
            else:
                final_price = bbo['bid'] * 0.95

            # Re-quantize
            d_price = Decimal(str(final_price))
            d_tick = Decimal(str(tick_size))
            final_price = float((d_price / d_tick).quantize(Decimal("1")) * d_tick)

        # 2. Params Construction
        is_buy = (side.upper() == 'BUY')

        # Handle Post-Only
        is_post_only = True
        if order_type == "MARKET":
            is_post_only = False
        if params and params.get('post_only') is False:
            is_post_only = False

        appendix = build_appendix(order_type=OrderType.POST_ONLY) if is_post_only else build_appendix(
            order_type=OrderType.GTC)

        try:
            order_params = OrderParams(
                sender=SubaccountParams(
                    subaccount_owner=self.owner,
                    subaccount_name=self.subaccount_name,
                ),
                priceX18=to_x18(final_price),
                # Nado: Buy is positive, Sell is negative
                amount=to_x18(amount) if is_buy else -to_x18(amount),
                expiration=get_expiration_timestamp(60 * 60 * 24),  # 24 Hours
                nonce=gen_order_nonce(),
                appendix=appendix
            )

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.market.place_order({"product_id": int(pid), "order": order_params})
            )

            if not result or not result.data:
                logger.error(f"❌ [Nado] Order Failed (No Data returned)")
                return None

            order_id = result.data.digest
            return str(order_id)

        except Exception as e:
            logger.error(f"❌ [Nado] Place Order Error: {e}")
            return None

    async def cancel_order(self, order_id: str, symbol: str = None):
        """Cancel Order using CancelOrdersParams"""
        try:
            pid = self.contract_map.get(symbol, {}).get('id')
            if not pid:
                # Fallback: try to find PID if not provided, or use first available
                if self.contract_map:
                    pid = list(self.contract_map.values())[0]['id']
                else:
                    logger.warning("⚠️ [Nado] Cancel: No market ID found")
                    return

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
            # logger.info(f"🗑️ [Nado] Cancel sent for {order_id}")

        except Exception as e:
            logger.warning(f"⚠️ [Nado] Cancel Failed: {e}")

    async def fetch_positions(self) -> List[Dict]:
        """
        Fetch positions for Inventory Strategy
        Returns list of dicts: [{'symbol': 'BTC', 'size': 1.5, ...}]
        """
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

                # Reverse lookup symbol from pid
                symbol = None
                for s, info in self.contract_map.items():
                    if info['id'] == pid:
                        symbol = s
                        break

                if symbol:
                    raw_size = pos.balance.amount
                    size = float(from_x18(raw_size))
                    if abs(size) > 0:
                        positions.append({
                            'symbol': symbol,
                            'size': size,
                            'side': 'BUY' if size > 0 else 'SELL'
                        })
            return positions

        except Exception as e:
            logger.error(f"❌ [Nado] Fetch Pos Error: {e}")
            return []

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        """
        Simulate WebSocket by polling Orderbook.
        Note: Nado SDK might support WS, but for stability we use Polling as per reference fallback.
        """
        logger.info("📡 [Nado] Starting Polling Stream...")

        while self.is_connected:
            try:
                for symbol in self.target_symbols:
                    tick = await self.fetch_orderbook(symbol)
                    if tick:
                        tick['type'] = 'tick'
                        tick_queue.put_nowait(tick)

                # Poll interval (adjust as needed)
                await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Poll Loop Error: {e}")
                await asyncio.sleep(5.0)

    async def close(self):
        self.is_connected = False
