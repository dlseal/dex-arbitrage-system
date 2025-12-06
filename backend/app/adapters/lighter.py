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
    """
    单个交易对的订单簿管理器
    负责：维护本地 OrderBook 状态、计算 BBO (Best Bid/Ask)
    """

    def __init__(self, symbol: str, market_id: int, ws_url: str, update_callback):
        self.symbol = symbol
        self.market_id = market_id
        self.ws_url = ws_url
        self.callback = update_callback  # 回调函数，将 BBO 推送给主队列

        self.bids: Dict[float, float] = {}  # Price -> Size
        self.asks: Dict[float, float] = {}  # Price -> Size
        self.order_book_offset = None
        self.snapshot_loaded = False

        self.running = False
        self.ws = None

    async def run(self):
        self.running = True
        while self.running:
            try:
                # 🔴 核心修复 1: 严格模拟浏览器的 Headers
                extra_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Origin": "https://lighter.exchange",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache"
                }

                logger.info(f"📡 [Lighter-{self.symbol}] Connecting to WS...")

                # 🔴 核心修复 2: compression=None (解决 AWS ELB 兼容性)
                async with websockets.connect(
                        self.ws_url,
                        extra_headers=extra_headers,
                        compression=None,
                        ping_interval=20,
                        ping_timeout=20
                ) as ws:
                    self.ws = ws
                    logger.info(f"✅ [Lighter-{self.symbol}] Connected! Sending subscribe...")

                    # 发送订阅请求
                    sub_msg = json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_id}"
                    })
                    await ws.send(sub_msg)

                    async for message in ws:
                        if not self.running: break
                        try:
                            data = json.loads(message)
                            await self._handle_message(data)
                        except json.JSONDecodeError:
                            logger.warning(
                                f"⚠️ [Lighter-{self.symbol}] Non-JSON data received. (Did server ignore encoding=json?)")
                            continue
                        except Exception as e:
                            logger.error(f"⚠️ [Lighter-{self.symbol}] Msg Handle Error: {e}")

            except websockets.exceptions.InvalidStatusCode as e:
                logger.error(f"❌ [Lighter-{self.symbol}] Handshake Rejected: {e}")
                if e.status_code == 400:
                    logger.error("   -> Hint: Check if URL parameters match exactly what AWS ALB expects.")
                self.snapshot_loaded = False
                self.bids.clear()
                self.asks.clear()
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"❌ [Lighter-{self.symbol}] WS Connection Error: {e}")
                self.snapshot_loaded = False
                self.bids.clear()
                self.asks.clear()
                await asyncio.sleep(5)

    async def _handle_message(self, data: Dict):
        msg_type = data.get("type")

        # 1. 心跳
        if msg_type == "ping":
            if self.ws: await self.ws.send(json.dumps({"type": "pong"}))
            return

        # 2. 初始快照 (Snapshot)
        if msg_type == "subscribed/order_book":
            ob = data.get("order_book", {})
            self._apply_update("bids", ob.get("bids", []), is_snapshot=True)
            self._apply_update("asks", ob.get("asks", []), is_snapshot=True)
            self.order_book_offset = ob.get("offset")
            self.snapshot_loaded = True
            await self._push_update()
            logger.info(f"📥 [Lighter-{self.symbol}] Snapshot loaded. Best: {self._get_bbo()}")

        # 3. 增量更新 (Delta Update)
        elif msg_type == "update/order_book":
            if not self.snapshot_loaded: return

            ob = data.get("order_book", {})
            new_offset = ob.get("offset")

            self._apply_update("bids", ob.get("bids", []))
            self._apply_update("asks", ob.get("asks", []))
            self.order_book_offset = new_offset
            await self._push_update()

    def _apply_update(self, side: str, updates: List[Dict], is_snapshot=False):
        """应用更新到本地字典"""
        target_dict = self.bids if side == "bids" else self.asks

        if is_snapshot:
            target_dict.clear()

        for item in updates:
            try:
                # Lighter 的价格和数量通常是字符串
                price = float(item["price"])
                size = float(item["size"])

                if size == 0:
                    target_dict.pop(price, None)
                else:
                    target_dict[price] = size
            except Exception:
                continue

    def _get_bbo(self):
        best_bid = max(self.bids.keys()) if self.bids else 0.0
        best_ask = min(self.asks.keys()) if self.asks else 0.0
        return best_bid, best_ask

    async def _push_update(self):
        """计算最优买卖价并回调"""
        if not self.bids or not self.asks: return

        best_bid, best_ask = self._get_bbo()

        if best_bid > 0 and best_ask > 0:
            await self.callback(self.symbol, best_bid, best_ask)


class LighterAdapter(BaseExchange):
    """
    Lighter.xyz Adapter (WS URL Parameters Fixed)
    """

    def __init__(self,
                 api_key: str,
                 private_key: str,
                 account_index: int = 0,
                 api_key_index: int = 0,
                 symbols: List[str] = None):

        super().__init__("Lighter")

        self.base_url = "https://mainnet.zklighter.elliot.ai"
        # 🔴 核心修复：添加 URL 参数以匹配 AWS 路由规则
        # 使用 encoding=json 替代 msgpack 以避免引入额外依赖
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream?encoding=json&readonly=true"

        self.api_key = api_key
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index

        self.target_symbols = symbols if symbols else ["BTC", "ETH", "SOL"]

        self.client: Optional[SignerClient] = None
        self.api_client: Optional[ApiClient] = None

        self.market_config = {}  # symbol -> {id, price_mul, size_mul}
        self.managers = []  # List of LighterOrderBookManager

    async def initialize(self):
        try:
            config = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=config)

            print("⏳ [Lighter] Fetching market configurations...")
            order_api = lighter.OrderApi(self.api_client)
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                size_mul = float(pow(10, market.supported_size_decimals))
                price_mul = float(pow(10, market.supported_price_decimals))

                self.market_config[market.symbol] = {
                    'id': market.market_id,
                    'size_mul': size_mul,
                    'price_mul': price_mul
                }

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
            raise ValueError(f"Market '{target}' not found.")
        return info

    async def listen_websocket(self, queue: asyncio.Queue):
        """启动多路 WebSocket 管理器"""

        async def update_callback(symbol, raw_best_bid, raw_best_ask):
            info = self.market_config.get(symbol)
            if not info: return

            bid_price = raw_best_bid / info['price_mul']
            ask_price = raw_best_ask / info['price_mul']

            tick = {
                'exchange': self.name,
                'symbol': symbol,
                'bid': bid_price,
                'ask': ask_price,
                'ts': int(time.time() * 1000)
            }
            queue.put_nowait(tick)

        tasks = []
        for symbol in self.target_symbols:
            info = self.market_config.get(symbol)
            if info:
                manager = LighterOrderBookManager(
                    symbol=symbol,
                    market_id=info['id'],
                    ws_url=self.ws_url,
                    update_callback=update_callback
                )
                self.managers.append(manager)
                tasks.append(asyncio.create_task(manager.run()))

        if not tasks:
            logger.warning("⚠️ [Lighter] No symbols to listen!")
            return

        print(f"🚀 [Lighter] Started {len(tasks)} WS connections.")
        await asyncio.gather(*tasks)

    # --- REST API ---
    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        info = self._get_market_info(symbol)
        order_api = lighter.OrderApi(self.api_client)
        ob_data = await order_api.order_book(market_id=info['id'])

        best_ask = float(ob_data.asks[0].price) / info['price_mul'] if ob_data.asks else 0.0
        best_bid = float(ob_data.bids[0].price) / info['price_mul'] if ob_data.bids else 0.0

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