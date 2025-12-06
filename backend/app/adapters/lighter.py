import asyncio
import json
import time
import logging
import websockets
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
        self.client = client  # 需要 SignerClient 来生成 auth token

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.snapshot_loaded = False
        self.last_offset = 0  # 序列号检查
        self.running = False

    async def run(self):
        self.running = True
        while self.running:
            try:
                # 1. 生成 Auth Token (有效期 10 分钟)
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

                # 2. 必须带上 User-Agent，否则 AWS WAF 可能拦截
                extra_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Origin": "https://lighter.exchange"
                }

                # 3. 建立连接
                async with websockets.connect(
                        self.ws_url,
                        extra_headers=extra_headers,
                        ping_interval=None,  # 禁用底层 Ping (服务器不支持)
                        ping_timeout=None,  # 禁用底层超时 (由看门狗接管)
                        compression=None,
                        close_timeout=5,
                        open_timeout=20
                ) as ws:
                    logger.info(f"✅ [Lighter-{self.symbol}] Connected! Sending subscribe...")

                    # 4. 发送订阅
                    # 4.1 订单簿订阅
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_id}"
                    }))

                    # 4.2 账户订单订阅
                    if self.client and auth_token:
                        acc_channel = f"account_orders/{self.market_id}/{self.client.account_index}"
                        await ws.send(json.dumps({
                            "type": "subscribe",
                            "channel": acc_channel,
                            "auth": auth_token
                        }))
                        logger.info(f"📤 [Lighter] Subscribed to {acc_channel}")

                    # 5. 数据看门狗循环 (Data Watchdog)
                    while self.running:
                        try:
                            # 最多只等 10 秒。如果 10 秒没消息，抛出 TimeoutError 重连
                            message = await asyncio.wait_for(ws.recv(), timeout=10.0)

                            # 处理消息
                            data = json.loads(message)
                            await self._handle_message(data, ws)

                            # Token 续期检查 (快过期前 60 秒重连)
                            if time.time() > expire_at - 60:
                                logger.info(f"🔄 [Lighter] Token expiring, reconnecting...")
                                break

                        except asyncio.TimeoutError:
                            # 10秒没收到消息，说明连接僵死
                            logger.warning(
                                f"⏰ [Lighter-{self.symbol}] No data for 10s (Ghost Connection), reconnecting...")
                            break

                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"🔌 [Lighter-{self.symbol}] Connection Closed")
                            break
                        except Exception as inner_e:
                            logger.error(f"⚠️ [Lighter-{self.symbol}] Loop Error: {inner_e}")
                            break

            except Exception as e:
                # 捕获握手超时或其他错误
                logger.error(f"❌ [Lighter-{self.symbol}] Connection Error: {repr(e)}")
                self.snapshot_loaded = False
                await asyncio.sleep(5)

    async def _handle_message(self, data: Dict, ws):
        msg_type = data.get("type")

        # ✅ 处理应用层 Ping (维持连接活性)
        if msg_type == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        # --- Orderbook 处理 ---
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

            if new_offset <= self.last_offset:
                return

            self.last_offset = new_offset
            self._apply_update("bids", ob.get("bids", []))
            self._apply_update("asks", ob.get("asks", []))
            await self._push_update()

        # --- 账户订单处理 ---
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

    def _get_bbo(self):
        best_bid = max(self.bids.keys()) if self.bids else 0.0
        best_ask = min(self.asks.keys()) if self.asks else 0.0
        return best_bid, best_ask

    async def _push_update(self):
        if not self.bids or not self.asks: return
        best_bid, best_ask = self._get_bbo()
        if best_bid > 0 and best_ask > 0:
            await self.callback(self.symbol, best_bid, best_ask)


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
            # 预检
            err = self.client.check_client()
            if err: raise Exception(str(err))

            self.is_connected = True
            logging.info(f"✅ [Lighter] Initialized. Account Index: {self.account_index}")

        except Exception as e:
            logging.info(f"❌ [Lighter] Init Failed: {e}")
            raise e

    async def listen_websocket(self, queue: asyncio.Queue):
        async def update_callback(symbol, best_bid, best_ask):
            tick = {
                'exchange': self.name,
                'symbol': symbol,
                'bid': best_bid,
                'ask': best_ask,
                'ts': int(time.time() * 1000)
            }
            queue.put_nowait(tick)

        tasks = []
        for symbol in self.target_symbols:
            info = self.market_config.get(symbol)
            if info:
                manager = LighterOrderBookManager(symbol, info['id'], self.ws_url, update_callback, self.client)
                self.managers.append(manager)
                tasks.append(asyncio.create_task(manager.run()))

        if tasks:
            logging.info(f"🚀 [Lighter] Starting {len(tasks)} WS connections...")
            await asyncio.gather(*tasks)

    # --- REST API ---
    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        empty_ret = {'exchange': self.name, 'symbol': symbol, 'bid': 0.0, 'ask': 0.0, 'ts': int(time.time() * 1000)}
        try:
            if not self.market_config: return empty_ret
            target_symbol = symbol.split('-')[0]
            info = self.market_config.get(target_symbol)
            if not info: return empty_ret

            order_api = lighter.OrderApi(self.api_client)
            resp = await order_api.order_books()
            target = next((ob for ob in resp.order_books if ob.market_id == info['id']), None)
            if target:
                best_ask = float(target.asks[0].price) if target.asks else 0.0
                best_bid = float(target.bids[0].price) if target.bids else 0.0
                return {'exchange': self.name, 'symbol': symbol, 'bid': best_bid, 'ask': best_ask,
                        'ts': int(time.time() * 1000)}
        except:
            pass
        return empty_ret

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        info = self.market_config.get(symbol)
        if not info:
            logging.error(
                f"❌ [Lighter] Symbol '{symbol}' not found in market config. Available: {list(self.market_config.keys())}")
            return None
        amount_int = int(amount * info['size_mul'])
        price_int = int(price * info['price_mul']) if price else 0
        is_ask = True if side.lower() == 'sell' else False
        client_order_index = int(time.time() * 1000) % 2147483647

        try:
            if order_type == "MARKET":
                res, tx_hash, err = await self.client.create_market_order(
                    market_index=info['id'], client_order_index=client_order_index,
                    base_amount=amount_int, avg_execution_price=price_int, is_ask=is_ask
                )
            else:
                res, tx_hash, err = await self.client.create_limit_order(
                    market_index=info['id'], client_order_index=client_order_index,
                    base_amount=amount_int, price=price_int, is_ask=is_ask,
                    time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
                )
            if err:
                logging.error(f"❌ [Lighter] Order Error: {err}")
                return None
            return str(client_order_index)
        except Exception as e:
            logging.error(f"❌ [Lighter] Create Exception: {e}")
            return None