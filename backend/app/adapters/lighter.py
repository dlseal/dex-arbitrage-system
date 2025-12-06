import asyncio
import time
import json
import logging
from typing import Dict, Optional, List
from .base import BaseExchange

# 引入 Lighter 官方 SDK
import lighter
from lighter import SignerClient, ApiClient, Configuration


class LighterAdapter(BaseExchange):
    """
    Lighter.xyz 适配器 (基于官方 WsClient 重构)
    """

    def __init__(self, base_url:str,api_key: str, private_key: str, account_index: int = 0, api_key_index: int = 0):
        super().__init__("Lighter")

        self.base_url = base_url

        # 身份凭证
        self.api_key = api_key
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index

        self.client: Optional[SignerClient] = None
        self.api_client: Optional[ApiClient] = None
        self.ws_client = None  # 保存 WsClient 实例

        # 缓存：市场配置
        self.market_config = {}
        # 反向映射：ID -> Symbol
        self.id_to_symbol = {}

    async def initialize(self):
        """
        初始化：获取市场配置并建立 REST 客户端
        """
        try:
            # 1. 初始化查询客户端
            config = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=config)

            # 2. 动态获取所有市场配置
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
                print(f"   - Loaded {market.symbol}: ID={market.market_id}")

            # 3. 初始化交易客户端
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
        # "BTC-USDT" -> "BTC"
        target = symbol.split('-')[0]

        info = self.market_config.get(target)
        if not info:
            # 调试辅助：打印前5个可用市场，方便排查
            available = list(self.market_config.keys())[:5]
            raise ValueError(f"Market '{target}' (from '{symbol}') not found. Available: {available}...")
        return info

    async def listen_websocket(self, queue: asyncio.Queue):
        """
        使用 lighter.WsClient 监听数据
        """
        loop = asyncio.get_running_loop()

        # 1. 定义回调函数 (注意：这会在 SDK 的独立线程中运行)
        def on_ob_update(market_id, order_book):
            try:
                # 找到对应的 symbol
                if market_id not in self.id_to_symbol:
                    return

                symbol = self.id_to_symbol[market_id]
                info = self.market_config[symbol]

                # 解析最佳买卖价
                # order_book 结构通常是 {'bids': [['price', 'size'], ...], 'asks': ...}
                # Lighter 返回的可能是原始字符串，需要转换
                best_bid = 0.0
                best_ask = 0.0

                if order_book.get('bids'):
                    # 价格需要除以 price_mul
                    raw_price = float(order_book['bids'][0][0])
                    best_bid = raw_price / info['price_mul']

                if order_book.get('asks'):
                    raw_price = float(order_book['asks'][0][0])
                    best_ask = raw_price / info['price_mul']

                if best_bid > 0 or best_ask > 0:
                    tick = {
                        'exchange': self.name,
                        'symbol': symbol,
                        'bid': best_bid,
                        'ask': best_ask,
                        'ts': int(time.time() * 1000)
                    }

                    # 关键：线程安全地推送到 asyncio 队列
                    loop.call_soon_threadsafe(queue.put_nowait, tick)

            except Exception as e:
                # 生产环境建议用 logging
                print(f"⚠️ [Lighter] Callback Error: {e}")

        # 2. 准备参数
        market_ids = [info['id'] for info in self.market_config.values()]

        print(f"📡 [Lighter] Starting WsClient for IDs: {market_ids}")

        # 3. 初始化 SDK WsClient
        self.ws_client = lighter.WsClient(
            order_book_ids=market_ids,
            on_order_book_update=on_ob_update,
            # 如果需要监听账户变动，可以加 on_account_update
            # account_ids=[self.account_index]
        )

        # 4. 在独立线程中运行阻塞的 run() 方法
        try:
            # run_in_executor(None, ...) 会使用默认的 ThreadPoolExecutor
            await loop.run_in_executor(None, self.ws_client.run)
        except Exception as e:
            print(f"❌ [Lighter] WS Loop Failed: {e}")

    # --- 以下保持 REST 实现不变 ---

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