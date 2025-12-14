import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Dict, Optional, List, Any

# Nado Protocol Imports
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
    # Fallback to prevent crash if library is missing during development
    print("⚠️ Warning: nado_protocol not found. NadoAdapter will not work.")
    create_nado_client = None

from .base import BaseExchange

logger = logging.getLogger("NadoAdapter")


class NadoAdapter(BaseExchange):
    def __init__(self, private_key: str, mode: str = "MAINNET", subaccount_name: str = "default",
                 symbols: List[str] = None):
        super().__init__("Nado")
        self.private_key = private_key
        self.mode_str = mode.upper()
        self.subaccount_name = subaccount_name
        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]

        self.client = None
        self.owner = None
        self.contract_map = {}  # Map symbol (e.g., 'BTC') -> product_id (int)
        self.tick_sizes = {}  # Map symbol -> tick_size (float)

    async def initialize(self):
        if not create_nado_client:
            raise ImportError("nado_protocol library is not installed.")

        try:
            # Map mode string to Enum
            mode_map = {
                'MAINNET': NadoClientMode.MAINNET,
                'DEVNET': NadoClientMode.DEVNET,
            }
            client_mode = mode_map.get(self.mode_str, NadoClientMode.MAINNET)

            logger.info(f"⏳ [Nado] Connecting to {self.mode_str}...")

            # Initialize SDK
            self.client = create_nado_client(client_mode, self.private_key)
            self.owner = self.client.context.engine_client.signer.address

            logger.info(f"✅ [Nado] Connected! Owner: {self.owner[:6]}... | Subaccount: {self.subaccount_name}")

            # Fetch Market Info to populate contract_map
            await self._load_markets()
            self.is_connected = True

        except Exception as e:
            logger.error(f"❌ [Nado] Init Failed: {e}")
            raise e

    async def _load_markets(self):
        """Sync market IDs and Tick Sizes"""
        try:
            # Get all symbols
            symbols_map = self.client.market.get_all_product_symbols()
            all_markets = self.client.market.get_all_engine_markets().perp_products

            for target in self.target_symbols:
                nado_symbol_str = f"{target.upper()}-PERP"
                product_id = None

                # Find product ID
                for sym_obj in symbols_map:
                    s_str = sym_obj.symbol if hasattr(sym_obj, 'symbol') else str(sym_obj)
                    if s_str == nado_symbol_str:
                        product_id = sym_obj.product_id if hasattr(sym_obj, 'product_id') else sym_obj
                        break

                if product_id is None:
                    logger.warning(f"⚠️ [Nado] Symbol {nado_symbol_str} not found.")
                    continue

                # Find Tick Size
                tick_size = 0.1
                for market in all_markets:
                    if market.product_id == product_id:
                        ts_x18 = market.book_info.price_increment_x18
                        tick_size = float(from_x18(ts_x18))
                        break

                self.contract_map[target] = product_id
                self.tick_sizes[target] = tick_size
                logger.info(f"   - [Nado] Loaded {target} (ID: {product_id}) | Tick: {tick_size}")

        except Exception as e:
            logger.error(f"❌ [Nado] Market Sync Error: {e}")

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """Fetch BBO using REST (since Nado logic is often polling-based)"""
        if symbol not in self.contract_map: return {}

        try:
            pid = self.contract_map[symbol]
            # Use SDK to get orderbook (Depth 1 is enough for BBO)
            ob = self.client.context.engine_client.get_orderbook(ticker_id=pid, depth=1)

            if not ob or not ob.bids or not ob.asks:
                return {}

            best_bid = float(ob.bids[0][0])
            best_ask = float(ob.asks[0][0])

            return {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'ask': best_ask,
                'ts': int(time.time() * 1000)
            }
        except Exception as e:
            logger.warning(f"⚠️ [Nado] Fetch OB Error: {e}")
            return {}

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT", params: Dict = None) -> Optional[str]:
        if symbol not in self.contract_map:
            logger.error(f"❌ [Nado] Unknown symbol: {symbol}")
            return None

        pid = self.contract_map[symbol]
        tick_size = self.tick_sizes.get(symbol, 0.1)

        # Rounding logic
        try:
            d_price = Decimal(str(price)) if price else Decimal("0")
            d_tick = Decimal(str(tick_size))
            if price:
                # Round to nearest tick
                d_price = (d_price / d_tick).quantize(Decimal("1")) * d_tick

            final_price = float(d_price)
        except Exception as e:
            logger.error(f"❌ [Nado] Math Error: {e}")
            return None

        # Determine Amount Sign (Buy is positive, Sell is negative in Nado Logic?)
        # Reference: amount=to_x18(qty) if buy else -to_x18(qty)
        is_buy = (side.lower() == 'buy')

        try:
            # Build Appendix (Post-Only check)
            is_post_only = True
            if params and params.get('post_only') is False:
                is_post_only = False

            # Default to Post-Only for LIMIT orders unless specified otherwise
            appendix = build_appendix(
                order_type=OrderType.POST_ONLY) if is_post_only and order_type == 'LIMIT' else build_appendix(
                order_type=OrderType.GTC)

            # Construct Params
            order_params = OrderParams(
                sender=SubaccountParams(
                    subaccount_owner=self.owner,
                    subaccount_name=self.subaccount_name,
                ),
                priceX18=to_x18(final_price),
                amount=to_x18(amount) if is_buy else -to_x18(amount),
                expiration=get_expiration_timestamp(60 * 60 * 24),  # 1 Day
                nonce=gen_order_nonce(),
                appendix=appendix
            )

            # Execute
            # Note: The reference uses self.client.market.place_order
            # We wrap in a blocking call if the SDK is sync, or await if async.
            # Assuming SDK is sync based on 'create_nado_client', we use run_in_executor

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.market.place_order({"product_id": int(pid), "order": order_params})
            )

            if not result or not result.data:
                logger.error(f"❌ [Nado] Order Failed (No Response)")
                return None

            order_id = result.data.digest
            return str(order_id)

        except Exception as e:
            logger.error(f"❌ [Nado] Order Exception: {e}")
            return None

    async def cancel_order(self, order_id: str, symbol: str = None):
        try:
            pid = self.contract_map.get(symbol) if symbol else None
            if not pid:
                # If symbol not provided, we might fail or need to try all known PIDs?
                # For simplicity, we require symbol or assume single market for now.
                # Just try to iterate known markets if symbol is missing is risky but fallback
                if self.contract_map:
                    pid = list(self.contract_map.values())[0]

            sender_hex = subaccount_to_hex(SubaccountParams(
                subaccount_owner=self.owner,
                subaccount_name=self.subaccount_name,
            ))

            loop = asyncio.get_running_loop()
            cancel_params = CancelOrdersParams(
                productIds=[int(pid)] if pid else [],
                digests=[order_id],
                sender=sender_hex
            )

            await loop.run_in_executor(
                None,
                lambda: self.client.market.cancel_orders(cancel_params)
            )
            # logger.info(f"🗑️ [Nado] Cancel req sent for {order_id}")

        except Exception as e:
            logger.warning(f"⚠️ [Nado] Cancel Failed: {e}")

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        """
        Simulate WebSocket by polling Orderbook every 1 second.
        This adapts the REST-based Nado SDK to the EventEngine's stream requirement.
        """
        logger.info("📡 [Nado] Starting Polling Stream (Simulated WS)...")

        while self.is_connected:
            try:
                for symbol in self.target_symbols:
                    tick = await self.fetch_orderbook(symbol)
                    if tick:
                        tick['type'] = 'tick'
                        # Push to queue
                        tick_queue.put_nowait(tick)

                await asyncio.sleep(1.0)  # Poll interval

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Poll Error: {e}")
                await asyncio.sleep(5.0)

    async def close(self):
        self.is_connected = False
        # Nado SDK might not need explicit close if it's just REST wrappers
        pass