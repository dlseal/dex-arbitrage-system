import asyncio
import time
import os
import logging
import random
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Dict, Optional, Any, List

from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv
from pysdk.grvt_ccxt_logging_selector import logger as sdk_logger

from .base import BaseExchange

logger = logging.getLogger("GRVT_Adapter")


class GrvtAdapter(BaseExchange):
    def __init__(self, api_key: str, private_key: str, trading_account_id: str, symbols: List[str] = None):
        super().__init__("GRVT")
        self.api_key = api_key
        self.private_key = private_key
        self.trading_account_id = trading_account_id
        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]

        env_str = os.getenv('GRVT_ENVIRONMENT', 'prod').lower()
        env_map = {
            'prod': GrvtEnv.PROD,
            'testnet': GrvtEnv.TESTNET,
            'staging': GrvtEnv.STAGING,
            'dev': GrvtEnv.DEV
        }
        self.env = env_map.get(env_str, GrvtEnv.PROD)

        self.rest_client: Optional[GrvtCcxt] = None
        self.ws_client: Optional[GrvtCcxtWS] = None
        self.contract_map = {}
        self.last_ws_msg_time = 0.0

    async def initialize(self):
        retry_count = 5
        for attempt in range(retry_count):
            try:
                logger.info(f"⏳ [GRVT] Connecting (Attempt {attempt + 1})...")
                await self._initialize_logic()
                logger.info("✅ [GRVT] Connection Established!")
                return
            except Exception as e:
                logger.warning(f"⚠️ [GRVT] Connection failed: {e}")
                await asyncio.sleep((attempt + 1) * 3)

        raise ConnectionError("❌ [GRVT] Failed to connect after multiple attempts.")

    async def _initialize_logic(self):
        params = {
            'trading_account_id': self.trading_account_id,
            'private_key': self.private_key,
            'api_key': self.api_key
        }
        self.rest_client = GrvtCcxt(env=self.env, parameters=params)

        logger.info(f"⏳ [GRVT] Syncing markets...")
        markets = await self._fetch_markets_async()

        self.contract_map = {}
        for market in markets:
            base = market.get('base')
            quote = market.get('quote')
            kind = market.get('kind')

            if kind == 'PERPETUAL' and quote == 'USDT':
                if base in self.target_symbols:
                    symbol = f"{base}-{quote}"
                    raw_id = market.get('instrument') or market.get('i')
                    raw_tick = market.get('tick_size') or market.get('ts') or "0.01"
                    min_size = market.get('min_size') or "0.0001"
                    contract_size = market.get('contract_size') or "1.0"

                    self.contract_map[symbol] = {
                        "id": raw_id,
                        "tick_size": Decimal(str(raw_tick)),
                        "min_size": Decimal(str(min_size)),
                        "size_mul": Decimal(str(contract_size))
                    }
                    logger.info(f"   - Loaded {symbol} (ID: {raw_id}) | Tick: {raw_tick} | MinSize: {min_size}")

        ws_params = {
            'api_key': self.api_key,
            'trading_account_id': self.trading_account_id,
            'api_ws_version': 'v1',
            'private_key': self.private_key
        }
        self.ws_client = GrvtCcxtWS(
            env=self.env,
            loop=asyncio.get_running_loop(),
            logger=sdk_logger,
            parameters=ws_params
        )
        await self.ws_client.initialize()
        self.is_connected = True
        self.last_ws_msg_time = time.time()

    async def _fetch_markets_async(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.rest_client.fetch_markets(params={}))

    def _get_contract_info(self, symbol: str):
        if "-" not in symbol: symbol = f"{symbol}-USDT"
        info = self.contract_map.get(symbol)
        if not info:
            logger.error(f"Symbol {symbol} not found in map")
            return None
        return info

    def _get_symbol_from_instrument(self, instrument_id):
        for s, info in self.contract_map.items():
            if info['id'] == instrument_id:
                return s.split('-')[0]
        return "UNKNOWN"

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        info = self._get_contract_info(symbol)
        if not info: return {}
        loop = asyncio.get_running_loop()
        try:
            ob = await loop.run_in_executor(None, lambda: self.rest_client.fetch_order_book(info['id'], limit=10))
            bids = ob.get('bids', [])
            asks = ob.get('asks', [])
            return {
                'exchange': self.name,
                'symbol': symbol.split('-')[0],
                'bid': float(bids[0]['price']) if bids else 0.0,
                'ask': float(asks[0]['price']) if asks else 0.0,
                'ts': int(time.time() * 1000)
            }
        except Exception as e:
            logger.error(f"Fetch OB error: {e}")
            return {}

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT", params: Dict = None) -> Optional[str]:
        info = self._get_contract_info(symbol)
        if not info:
            logger.error(f"❌ [GRVT] Contract info not found for {symbol}")
            return None

        if params is None:
            params = {}

        try:
            tick_size = info['tick_size']
            min_size = info['min_size']

            d_amount = Decimal(str(amount))
            if min_size > 0:
                d_amount = (d_amount / min_size).to_integral_value(rounding=ROUND_DOWN) * min_size

            d_price = Decimal("0")
            if price is not None:
                d_price = Decimal(str(price))
                if tick_size > 0:
                    d_price = (d_price / tick_size).to_integral_value(rounding=ROUND_HALF_UP) * tick_size

            qty_float = float(d_amount)
            px_float = float(d_price) if price is not None else None

            if qty_float <= 0:
                logger.warning(f"⚠️ Order size too small: {qty_float}")
                return None

        except Exception as e:
            logger.error(f"❌ [GRVT] Precision Math Error: {e}")
            return None

        client_order_id = int(time.time() * 1000) & 0xFFFFFFFF
        safe_side = side.lower()

        # 基础参数
        req_params = {
            'client_order_id': client_order_id,
        }

        # 合并传入的 params (如 post_only)
        if params:
            req_params.update(params)

        # 默认 LIMIT 单开启 Post-Only，除非 params 里明确指定了 False
        if order_type == "LIMIT":
            # 如果 strategy 没传 post_only，默认设为 True（安全模式）
            # 如果 strategy 传了 False，这里就用 False（套利模式）
            final_post_only = req_params.get('post_only', True)

            req_params.update({
                'post_only': final_post_only,
                'timeInForce': 'GTT',
                'order_duration_secs': 2591999,
            })

        try:
            target_symbol_id = info['id']

            if order_type == "MARKET":
                await asyncio.wait_for(
                    self.ws_client.create_order(
                        symbol=target_symbol_id,
                        order_type='market',
                        side=safe_side,
                        amount=qty_float,
                        price=None,
                        params=req_params
                    ), timeout=5.0)
            else:
                await asyncio.wait_for(
                    self.ws_client.create_limit_order(
                        symbol=target_symbol_id,
                        side=safe_side,
                        amount=qty_float,
                        price=px_float,
                        params=req_params
                    ), timeout=5.0)

            return str(client_order_id)

        except asyncio.TimeoutError:
            logger.error(f"❌ [GRVT] Order Timeout (5s) - Possible Ghost Order! ID: {client_order_id}")
            return None
        except Exception as e:
            # 记录详细错误，方便调试 Post-Only 拒绝原因
            err_msg = str(e).lower()
            if "post-only" in err_msg or "maker" in err_msg:
                logger.warning(f"⚠️ [GRVT] Order Rejected (Post-Only): {e}")
            else:
                logger.error(f"❌ [GRVT] Create Order Error: {e}")
            return None

    async def cancel_order(self, order_id: str, symbol: str = None):
        try:
            oid = int(order_id) if str(order_id).isdigit() else order_id

            inst_id = None
            if symbol:
                info = self._get_contract_info(symbol)
                if info:
                    inst_id = info['id']

            params = {'client_order_id': oid}
            await self.ws_client.cancel_order(id=None, symbol=inst_id, params=params)

        except Exception as e:
            if "not found" not in str(e).lower():
                logger.warning(f"Cancel failed for {order_id}: {e}")

    async def close(self):
        logger.info("🛑 [GRVT] Closing resources...")
        self.is_connected = False
        self.ws_client = None

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        logger.info(f"📡 [GRVT] Starting Stream Listener...")
        self.last_ws_msg_time = time.time()

        async def message_callback(message: Dict[str, Any]):
            self.last_ws_msg_time = time.time()
            try:
                feed_data = message.get("feed", {})
                if not feed_data and "instrument" in message: feed_data = message

                channel = message.get("params", {}).get("channel") or message.get("stream") or ""

                if "order" in str(channel) and "book" not in str(channel):
                    state = feed_data.get("state", {})
                    status = state.get("status", "").upper()

                    client_oid = message.get('client_order_id') or feed_data.get('client_order_id') or state.get(
                        'client_order_id')
                    system_oid = message.get('order_id') or feed_data.get('order_id')
                    final_order_id = str(client_oid) if client_oid else str(system_oid)

                    if status in ["FILLED", "PARTIALLY_FILLED"]:
                        legs = feed_data.get("legs", [])
                        filled_size = sum(float(l.get("size", 0)) for l in legs)

                        if filled_size > 0 and legs:
                            leg = legs[0]
                            symbol_base = self._get_symbol_from_instrument(leg.get("instrument"))
                            is_buy = leg.get("is_buying_asset", False)
                            side = "BUY" if is_buy else "SELL"
                            price = float(leg.get("limit_price", 0))

                            event = {
                                'type': 'trade',
                                'exchange': self.name,
                                'symbol': symbol_base,
                                'side': side,
                                'price': price,
                                'size': filled_size,
                                'order_id': final_order_id,
                                'status': status,
                                'ts': int(time.time() * 1000)
                            }
                            event_queue.put_nowait(event)
                            logger.info(f"⚡️ [GRVT Fill] {symbol_base} {side} {filled_size} (ID:{final_order_id})")
                    return

                if "book" in str(channel):
                    instrument = feed_data.get("instrument")
                    symbol_base = self._get_symbol_from_instrument(instrument)
                    if symbol_base == "UNKNOWN": return

                    bids = feed_data.get("bids", [])
                    asks = feed_data.get("asks", [])

                    if bids and asks:
                        tick = {
                            'type': 'tick',
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'bid': float(bids[0]['price']),
                            'bid_volume': float(bids[0]['size']),
                            'ask': float(asks[0]['price']),
                            'ask_volume': float(asks[0]['size']),
                            'ts': int(time.time() * 1000)
                        }
                        tick_queue.put_nowait(tick)

            except Exception as e:
                logger.error(f"WS Parse Error: {e}")

        try:
            for symbol, info in self.contract_map.items():
                inst_id = info['id']
                logger.info(f"📤 [GRVT] Subscribing to {symbol} (ID: {inst_id})")

                await self.ws_client.subscribe(stream="book.s", callback=message_callback,
                                               params={"instrument": inst_id})

                await self.ws_client.subscribe(stream="order", callback=message_callback,
                                               params={"instrument": inst_id,
                                                       "sub_account_id": self.trading_account_id})
                await asyncio.sleep(0.1)

            while self.is_connected:
                await asyncio.sleep(5)
                if time.time() - self.last_ws_msg_time > 60.0:
                    logger.error("❌ [GRVT] Watchdog: No data for 60s.")
                    self.is_connected = False
                    raise ConnectionError("GRVT WS Timeout")

        except asyncio.CancelledError:
            raise
        finally:
            await self.close()