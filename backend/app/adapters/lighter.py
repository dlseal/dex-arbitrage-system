import asyncio
import json
import time
import logging
import websockets
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Optional, List, Any
from .base import BaseExchange

import lighter
from lighter import SignerClient, ApiClient, Configuration

logger = logging.getLogger("LighterAdapter")


class LighterOrderBookManager:
    def __init__(self, symbol: str, market_id: int, ws_url: str, update_callback, client: SignerClient = None):
        self.symbol = symbol
        self.market_id = market_id
        self.ws_url = ws_url
        self.callback = update_callback
        self.client = client
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.snapshot_loaded = False
        self.last_offset = 0
        self.running = False

    async def run(self):
        self.running = True
        while self.running:
            try:
                self.snapshot_loaded = False
                self.bids.clear()
                self.asks.clear()

                expire_at = int(time.time() + 600)
                auth_token = ""

                if self.client:
                    try:
                        auth_token, err = self.client.create_auth_token_with_expiry(expire_at)
                        if err:
                            logger.error(f"❌ [Lighter-{self.symbol}] Auth Token Gen Failed: {err}")
                            await asyncio.sleep(5)
                            continue
                    except Exception as e:
                        logger.error(f"❌ [Lighter-{self.symbol}] Client Auth Error: {e}")
                        await asyncio.sleep(5)
                        continue

                logger.info(f"📡 [Lighter-{self.symbol}] Connecting (Exp: {expire_at})...")

                extra_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Origin": "https://lighter.exchange"
                }

                async with websockets.connect(
                        self.ws_url,
                        extra_headers=extra_headers,
                        ping_interval=None,
                        ping_timeout=None,
                        close_timeout=5,
                        open_timeout=20
                ) as ws:
                    logger.info(f"✅ [Lighter-{self.symbol}] Connected!")

                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_id}"
                    }))

                    if self.client and auth_token:
                        acc_channel = f"account_orders/{self.market_id}/{self.client.account_index}"
                        await ws.send(json.dumps({
                            "type": "subscribe",
                            "channel": acc_channel,
                            "auth": auth_token
                        }))
                        logger.info(f"📤 [Lighter] Subscribed to {acc_channel}")

                    while self.running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=10.0)
                            data = json.loads(message)
                            await self._handle_message(data, ws)

                            if time.time() > expire_at - 60:
                                logger.info(f"🔄 [Lighter] Token expiring, reconnecting...")
                                break

                        except asyncio.TimeoutError:
                            logger.warning(f"⏰ [Lighter-{self.symbol}] No data for 10s, reconnecting...")
                            break
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"🔌 [Lighter-{self.symbol}] Connection Closed")
                            break
                        except Exception as inner_e:
                            logger.error(f"⚠️ [Lighter-{self.symbol}] Loop Error: {inner_e}")
                            break

            except Exception as e:
                logger.error(f"❌ [Lighter-{self.symbol}] Connection Error: {repr(e)}")
                await asyncio.sleep(5)

    async def _handle_message(self, data: Dict, ws):
        msg_type = data.get("type")
        if msg_type == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        if msg_type == "subscribed/order_book":
            ob = data.get("order_book", {})
            self.last_offset = ob.get("offset", 0)
            self._apply_update("bids", ob.get("bids", []), is_snapshot=True)
            self._apply_update("asks", ob.get("asks", []), is_snapshot=True)
            self.snapshot_loaded = True
            await self._push_update()
            logger.info(f"📥 [Lighter-{self.symbol}] Snapshot loaded. Offset: {self.last_offset}")

        elif msg_type == "update/order_book":
            if not self.snapshot_loaded: return
            ob = data.get("order_book", {})
            new_offset = ob.get("offset", 0)
            if new_offset <= self.last_offset: return
            self.last_offset = new_offset
            self._apply_update("bids", ob.get("bids", []))
            self._apply_update("asks", ob.get("asks", []))
            await self._push_update()

        elif msg_type == "update/account_orders":
            orders = data.get("orders", {}).get(str(self.market_id), [])
            for o in orders:
                if o.get("status") == "filled":
                    logger.info(
                        f"⚡️ [Lighter WS] Order Filled: {o.get('side')} {o.get('filled_base_amount')} @ {o.get('price')}")

    def _apply_update(self, side: str, updates: List[Dict], is_snapshot=False):
        target_dict = self.bids if side == "bids" else self.asks
        if is_snapshot: target_dict.clear()
        for item in updates:
            try:
                price = float(item["price"])
                size = float(item["size"])
                if size == 0:
                    target_dict.pop(price, None)
                else:
                    target_dict[price] = size
            except:
                continue

    def _get_depth_snapshot(self, limit=5):
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:limit]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:limit]
        return bids_sorted, asks_sorted

    async def _push_update(self):
        if not self.bids or not self.asks: return
        best_bid = max(self.bids.keys())
        best_ask = min(self.asks.keys())
        bids_depth, asks_depth = self._get_depth_snapshot()
        await self.callback(self.symbol, best_bid, best_ask, bids_depth, asks_depth)


class LighterAdapter(BaseExchange):
    def __init__(self, api_key: str, private_key: str, account_index: int = 0, api_key_index: int = 0,
                 symbols: List[str] = None):
        super().__init__("Lighter")
        self.base_url = "https://mainnet.zklighter.elliot.ai"
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        self.api_key = api_key
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]
        self.client = None
        self.api_client = None
        self.market_config = {}
        self.managers = []

    async def initialize(self):
        try:
            config = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=config)
            order_api = lighter.OrderApi(self.api_client)

            resp = await order_api.order_books()
            for market in resp.order_books:
                size_mul = float(pow(10, market.supported_size_decimals))
                price_mul = float(pow(10, market.supported_price_decimals))
                self.market_config[market.symbol] = {
                    'id': market.market_id,
                    'size_mul': size_mul,
                    'price_mul': price_mul
                }
                if market.symbol in self.target_symbols:
                    logging.info(f"   - [Lighter] Loaded {market.symbol}: ID={market.market_id}")

            private_keys_dict = {self.api_key_index: self.private_key}
            self.client = SignerClient(
                url=self.base_url,
                account_index=self.account_index,
                api_private_keys=private_keys_dict
            )
            err = self.client.check_client()
            if err: raise Exception(str(err))

            self.is_connected = True
            logging.info(f"✅ [Lighter] Initialized. Account Index: {self.account_index}")

        except Exception as e:
            logging.info(f"❌ [Lighter] Init Failed: {e}")
            raise e

    async def close(self):
        logger.info("🛑 [Lighter] Stopping managers...")
        for manager in self.managers:
            manager.running = False
        if self.api_client:
            await self.api_client.close()

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        async def update_callback(symbol, best_bid, best_ask, bids_depth, asks_depth):
            tick = {
                'type': 'tick',
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'ask': best_ask,
                'bids_depth': bids_depth,
                'asks_depth': asks_depth,
                'ts': int(time.time() * 1000)
            }
            try:
                tick_queue.put_nowait(tick)
            except:
                pass

        tasks = []
        for symbol in self.target_symbols:
            info = self.market_config.get(symbol)
            if info:
                manager = LighterOrderBookManager(symbol, info['id'], self.ws_url, update_callback, self.client)
                self.managers.append(manager)
                tasks.append(asyncio.create_task(manager.run()))

        if tasks:
            logging.info(f"🚀 [Lighter] Starting {len(tasks)} WS connections...")
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                await self.close()
                raise

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        empty_ret = {'exchange': self.name, 'symbol': symbol, 'bid': 0.0, 'ask': 0.0, 'ts': int(time.time() * 1000)}
        return empty_ret

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        info = self.market_config.get(symbol)
        if not info:
            logging.error(f"❌ [Lighter] Symbol '{symbol}' not found.")
            return None

        # ✅ 修复精度：使用 Decimal 计算，完全杜绝 float 误差
        try:
            d_amount = Decimal(str(amount))
            d_size_mul = Decimal(str(info['size_mul']))
            amount_int = int((d_amount * d_size_mul).to_integral_value(rounding=ROUND_HALF_UP))

            price_int = 0
            if price:
                d_price = Decimal(str(price))
                d_price_mul = Decimal(str(info['price_mul']))
                price_int = int((d_price * d_price_mul).to_integral_value(rounding=ROUND_HALF_UP))
        except Exception as e:
            logging.error(f"❌ [Lighter] Math Error: {e}")
            return None

        is_ask = True if side.lower() == 'sell' else False
        client_order_index = int(time.time() * 1000) % 2147483647

        try:
            if order_type == "MARKET":
                res, tx_hash, err = await asyncio.wait_for(
                    self.client.create_market_order(
                        market_index=info['id'], client_order_index=client_order_index,
                        base_amount=amount_int, avg_execution_price=price_int, is_ask=is_ask
                    ), timeout=5.0)
            else:
                res, tx_hash, err = await asyncio.wait_for(
                    self.client.create_limit_order(
                        market_index=info['id'], client_order_index=client_order_index,
                        base_amount=amount_int, price=price_int, is_ask=is_ask,
                        time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
                    ), timeout=5.0)

            if err:
                logging.error(f"❌ [Lighter] Order Error: {err}")
                return None
            return str(client_order_index)

        except asyncio.TimeoutError:
            logging.error(f"❌ [Lighter] Order Timeout (5s)")
            return None
        except Exception as e:
            logging.error(f"❌ [Lighter] Create Exception: {e}")
            return None