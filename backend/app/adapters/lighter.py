import asyncio
import time
import json
import logging
import websockets
from typing import Dict, Optional, List
from .base import BaseExchange

import lighter
from lighter import SignerClient, ApiClient, Configuration


class LighterAdapter(BaseExchange):
    """
    Lighter.xyz Adapter (Manual WebSocket Implementation)
    """

    def __init__(self,
                 api_key: str,
                 private_key: str,
                 account_index: int = 0,
                 api_key_index: int = 0,
                 base_url: str = None,
                 symbols: List[str] = None):

        super().__init__("Lighter")

        # REST URL
        self.base_url = "https://mainnet.zklighter.elliot.ai"
        # WS URL (Derived manually)
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream"

        self.api_key = api_key
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index

        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]

        self.client: Optional[SignerClient] = None
        self.api_client: Optional[ApiClient] = None

        self.market_config = {}
        self.id_to_symbol = {}

    async def initialize(self):
        try:
            config = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=config)

            print("⏳ [Lighter] Fetching market configurations...")
            order_api = lighter.OrderApi(self.api_client)
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                size_mul = int(pow(10, market.supported_size_decimals))
                price_mul = int(pow(10, market.supported_price_decimals))

                self.market_config[market.symbol] = {
                    'id': market.market_id,
                    'size_mul': size_mul,
                    'price_mul': price_mul
                }
                self.id_to_symbol[market.market_id] = market.symbol

                if market.symbol in self.target_symbols:
                    print(f"   - Loaded {market.symbol}: ID={market.market_id}")

            private_keys_dict = {self.api_key_index: self.private_key}
            self.client = SignerClient(
                url=self.base_url,
                account_index=self.account_index,
                api_private_keys=private_keys_dict
            )

            err = self.client.check_client()
            if err:
                raise Exception(f"SignerClient Error: {str(err)}")

            self.is_connected = True
            print(f"✅ [Lighter] Initialized.")

        except Exception as e:
            print(f"❌ [Lighter] Init Failed: {e}")
            raise e

    def _get_market_info(self, symbol: str):
        target = symbol.split('-')[0]
        info = self.market_config.get(target)
        if not info:
            available = list(self.market_config.keys())[:5]
            raise ValueError(f"Market '{target}' (from '{symbol}') not found. Available: {available}...")
        return info

    async def listen_websocket(self, queue: asyncio.Queue):
        """
        Manual WebSocket Listener
        """
        loop = asyncio.get_running_loop()

        while True:
            try:
                print(f"📡 [Lighter] Connecting to {self.ws_url}...")
                async with websockets.connect(self.ws_url) as ws:
                    print(f"✅ [Lighter] WS Connected!")

                    # 1. Subscribe
                    target_ids = []
                    for symbol, info in self.market_config.items():
                        if symbol in self.target_symbols:
                            target_ids.append(info['id'])

                    # Send subscription messages
                    for market_id in target_ids:
                        msg = json.dumps({
                            "type": "subscribe",
                            "channel": f"order_book/{market_id}"
                        })
                        await ws.send(msg)
                        # print(f"   -> Subscribed ID: {market_id}")

                    # 2. Receive Loop
                    async for message in ws:
                        data = json.loads(message)
                        msg_type = data.get("type")

                        # Handle Ping
                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue

                        # Handle Orderbook Update
                        # Format based on SDK source:
                        # {"type": "update/order_book", "channel": "order_book:1", "order_book": {...}}
                        if msg_type in ["update/order_book", "subscribed/order_book"]:
                            channel = data.get("channel", "")
                            if "order_book" in channel:
                                # Extract ID from "order_book:1"
                                try:
                                    market_id = int(channel.split(":")[1])
                                    self._process_ob_data(market_id, data["order_book"], queue, loop)
                                except Exception:
                                    pass

            except Exception as e:
                print(f"❌ [Lighter] WS Disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    def _process_ob_data(self, market_id, ob_data, queue, loop):
        """Process raw OB data from Lighter"""
        if market_id not in self.id_to_symbol:
            return

        symbol = self.id_to_symbol[market_id]
        info = self.market_config[symbol]

        best_bid = 0.0
        best_ask = 0.0

        # Lighter sends lists of {'price': '...', 'size': '...'}
        bids = ob_data.get("bids", [])
        asks = ob_data.get("asks", [])

        if bids:
            # Price is likely a string int, e.g. "4200000"
            raw_p = float(bids[0]["price"])
            best_bid = raw_p / info["price_mul"]

        if asks:
            raw_p = float(asks[0]["price"])
            best_ask = raw_p / info["price_mul"]

        if best_bid > 0 or best_ask > 0:
            tick = {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'ask': best_ask,
                'ts': int(time.time() * 1000)
            }
            loop.call_soon_threadsafe(queue.put_nowait, tick)

    # --- REST (Unchanged) ---
    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        info = self._get_market_info(symbol)
        order_api = lighter.OrderApi(self.api_client)
        ob_data = await order_api.order_book(market_id=info['id'])

        best_ask = float(ob_data.asks[0].price) if ob_data.asks else 0.0
        best_bid = float(ob_data.bids[0].price) if ob_data.bids else 0.0

        return {
            'exchange': self.name,
            'symbol': symbol,
            'bid': best_bid,
            'ask': best_ask,
            'ts': int(time.time() * 1000)
        }

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        info = self._get_market_info(symbol)
        amount_int = int(amount * info['size_mul'])
        price_int = int(price * info['price_mul']) if price else 0
        is_ask = True if side.lower() == 'sell' else False
        client_order_index = int(time.time() * 1000) % 2147483647

        try:
            res, tx_hash, err = None, None, None
            if order_type == "MARKET":
                res, tx_hash, err = await self.client.create_market_order(
                    market_index=info['id'],
                    client_order_index=client_order_index,
                    base_amount=amount_int,
                    avg_execution_price=price_int,
                    is_ask=is_ask
                )
            else:
                res, tx_hash, err = await self.client.create_limit_order(
                    market_index=info['id'],
                    client_order_index=client_order_index,
                    base_amount=amount_int,
                    price=price_int,
                    is_ask=is_ask,
                    time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
                )
            if err:
                print(f"❌ [Lighter] Order Error: {err}")
                return None
            return str(client_order_index)
        except Exception as e:
            print(f"❌ [Lighter] Create Exception: {e}")
            return None