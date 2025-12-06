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
    def __init__(self, symbol: str, market_id: int, ws_url: str, update_callback):
        self.symbol = symbol
        self.market_id = market_id
        self.ws_url = ws_url
        self.callback = update_callback
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.snapshot_loaded = False
        self.running = False

    async def run(self):
        self.running = True
        while self.running:
            try:
                extra_headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Origin": "https://lighter.exchange"
                }
                logger.info(f"📡 [Lighter-{self.symbol}] Connecting to WS...")

                async with websockets.connect(
                        self.ws_url,
                        extra_headers=extra_headers,
                        compression=None,
                        ping_interval=30,
                        ping_timeout=60
                ) as ws:
                    logger.info(f"✅ [Lighter-{self.symbol}] Connected! Sending subscribe...")
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.market_id}"
                    }))

                    async for message in ws:
                        if not self.running: break
                        try:
                            data = json.loads(message)
                            await self._handle_message(data)
                        except Exception as e:
                            logger.error(f"⚠️ [Lighter-{self.symbol}] Msg Handle Error: {e}")

            except Exception as e:
                logger.error(f"❌ [Lighter-{self.symbol}] WS Connection Error: {e}")
                self.snapshot_loaded = False
                await asyncio.sleep(5)

    async def _handle_message(self, data: Dict):
        msg_type = data.get("type")
        if msg_type == "ping": return

        if msg_type == "subscribed/order_book":
            ob = data.get("order_book", {})
            self._apply_update("bids", ob.get("bids", []), is_snapshot=True)
            self._apply_update("asks", ob.get("asks", []), is_snapshot=True)
            self.snapshot_loaded = True
            await self._push_update()
            # 打印一次快照价格，确认修复是否生效
            best_bid, _ = self._get_bbo()
            logger.info(f"📥 [Lighter-{self.symbol}] Snapshot loaded. Best Bid: {best_bid}")

        elif msg_type == "update/order_book":
            if not self.snapshot_loaded: return
            ob = data.get("order_book", {})
            self._apply_update("bids", ob.get("bids", []))
            self._apply_update("asks", ob.get("asks", []))
            await self._push_update()

    def _apply_update(self, side: str, updates: List[Dict], is_snapshot=False):
        target_dict = self.bids if side == "bids" else self.asks
        if is_snapshot: target_dict.clear()

        for item in updates:
            try:
                # 🔴 修复点 1: 直接使用 float，不再除以 multiplier
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
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream?encoding=json&readonly=true"
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

            # 🔴 修复点 2: 使用 fetch_order_books 获取所有市场
            resp = await order_api.order_books()

            for market in resp.order_books:
                # 依然保存 multiplier 以备下单使用 (下单可能需要整数)
                size_mul = float(pow(10, market.supported_size_decimals))
                price_mul = float(pow(10, market.supported_price_decimals))

                self.market_config[market.symbol] = {
                    'id': market.market_id,
                    'size_mul': size_mul,
                    'price_mul': price_mul
                }
                if market.symbol in self.target_symbols:
                    logging.info(f"   - Loaded {market.symbol}: ID={market.market_id}")

            private_keys_dict = {self.api_key_index: self.private_key}
            self.client = SignerClient(
                url=self.base_url,
                account_index=self.account_index,
                api_private_keys=private_keys_dict
            )
            self.is_connected = True
            logging.info(f"✅ [Lighter] Initialized.")

        except Exception as e:
            logging.info(f"❌ [Lighter] Init Failed: {e}")
            raise e

    async def listen_websocket(self, queue: asyncio.Queue):
        async def update_callback(symbol, best_bid, best_ask):
            # 🔴 修复点 3: 这里不再除以 price_mul，因为 Manager 里已经是 float 了
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
                manager = LighterOrderBookManager(symbol, info['id'], self.ws_url, update_callback)
                self.managers.append(manager)
                tasks.append(asyncio.create_task(manager.run()))

        if tasks:
            logging.info(f"🚀 [Lighter] Started {len(tasks)} WS connections.")
            await asyncio.gather(*tasks)

    # --- REST API (修复版) ---
    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        # 默认返回值，防止报错
        empty_ret = {
            'exchange': self.name,
            'symbol': symbol,
            'bid': 0.0,
            'ask': 0.0,
            'ts': int(time.time() * 1000)
        }

        try:
            # 1. 确保配置已加载
            if not self.market_config:
                return empty_ret

            # 2. 获取市场信息
            # 注意：这里做个容错，防止 symbol 格式不匹配 (如 "BTC-USDT" vs "BTC")
            target_symbol = symbol.split('-')[0]
            info = self.market_config.get(target_symbol)

            if not info:
                return empty_ret

            order_api = lighter.OrderApi(self.api_client)
            resp = await order_api.order_books()

            target = next((ob for ob in resp.order_books if ob.market_id == info['id']), None)

            if target:
                best_ask = float(target.asks[0].price) if target.asks else 0.0
                best_bid = float(target.bids[0].price) if target.bids else 0.0

                return {
                    'exchange': self.name,
                    'symbol': symbol,
                    'bid': best_bid,
                    'ask': best_ask,
                    'ts': int(time.time() * 1000)
                }
        except Exception as e:
            # logger.warning(f"Lighter REST Error: {e}")
            pass

        return empty_ret

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        # 🔴 保持不变：下单通常需要转回整数单位
        info = self.market_config.get(symbol)
        amount_int = int(amount * info['size_mul'])
        price_int = int(price * info['price_mul']) if price else 0
        is_ask = True if side.lower() == 'sell' else False
        client_order_index = int(time.time() * 1000) % 2147483647

        try:
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
                logging.info(f"❌ [Lighter] Order Error: {err}")
                return None
            return str(client_order_index)
        except Exception as e:
            logging.info(f"❌ [Lighter] Create Exception: {e}")
            return None
