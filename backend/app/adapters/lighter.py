import asyncio
import time
import os
import json

import websockets
from typing import Dict, Optional
from .base import BaseExchange

import lighter
from lighter import SignerClient, ApiClient, Configuration


class LighterAdapter(BaseExchange):
    """
    Lighter.xyz 适配器 (基于官方 SDK 深度定制)
    """

    def __init__(self, api_key: str, private_key: str):
        # 注意：Lighter 的 api_key 在这里对应 "Public Address"
        # private_key 对应 "Private Key"
        super().__init__("Lighter", api_key, private_key)

        # Lighter 特有配置 (从环境变量读取，如果未传则默认 0)
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX', '0'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX', '0'))
        self.base_url = "https://mainnet.zklighter.elliot.ai"  # 参考代码中的主网地址

        self.client: Optional[SignerClient] = None
        self.api_client: Optional[ApiClient] = None

        # 缓存：Market Config
        self.market_config = {}  # {symbol: {'id': int, 'size_mul': int, 'price_mul': int}}

    async def initialize(self):
        """
        初始化：连接 Client 并自动获取市场精度配置
        """
        try:
            # 1. 初始化查询客户端
            config = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=config)

            # 2. 动态获取所有市场配置 (参考源码 _get_market_config)
            print("⏳ [Lighter] Fetching market configurations...")
            order_api = lighter.OrderApi(self.api_client)
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                # 提取精度乘数
                size_mul = int(pow(10, market.supported_size_decimals))
                price_mul = int(pow(10, market.supported_price_decimals))

                # 存入缓存，方便后续快速转换
                # 注意：Lighter symbol 可能是 "WBTC-USDC"，需确保与系统统一
                self.market_config[market.symbol] = {
                    'id': market.market_id,
                    'size_mul': size_mul,
                    'price_mul': price_mul
                }
                print(f"   - Loaded {market.symbol}: ID={market.market_id}, PriceMul={price_mul}")

            # 3. 初始化交易客户端 (SignerClient)
            self.client = SignerClient(
                url=self.base_url,
                private_key=self.private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index
            )

            # 检查连接
            err = self.client.check_client()
            if err:
                raise Exception(f"SignerClient Error: {err}")

            self.is_connected = True
            print(f"✅ [Lighter] Initialized. Account Index: {self.account_index}")

        except Exception as e:
            print(f"❌ [Lighter] Init Failed: {e}")
            raise e

    def _get_market_info(self, symbol: str):
        # 处理 symbol 别名 (如果需要)
        # 例如系统传 WBTC-USDT，但 Lighter 是 WBTC-USDC
        target = symbol
        if symbol == "WBTC-USDT": target = "WBTC-USDC"

        info = self.market_config.get(target)
        if not info:
            raise ValueError(f"Market {symbol} not found in Lighter configs")
        return info

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """
        获取订单簿 (REST API 方式，作为 WS 的备用)
        """
        info = self._get_market_info(symbol)
        order_api = lighter.OrderApi(self.api_client)

        # Lighter SDK 获取 OrderBook
        ob_data = await order_api.order_book(market_id=info['id'])

        # 解析最佳买卖价
        # 注意：Lighter 返回的可能是原始 Int，需要除以乘数
        best_ask = 0.0
        best_bid = 0.0

        if ob_data.asks and len(ob_data.asks) > 0:
            # 假设 SDK 返回的已经是处理好的对象，或者需要转换
            # 根据 SDK 源码，通常返回的是 decimal 字符串或 float
            # 这里做安全转换
            best_ask = float(ob_data.asks[0].price)

        if ob_data.bids and len(ob_data.bids) > 0:
            best_bid = float(ob_data.bids[0].price)

        return {
            'exchange': self.name,
            'symbol': symbol,
            'bid': best_bid,
            'ask': best_ask,
            'ts': int(time.time() * 1000)
        }

    async def create_order(self,
                           symbol: str,
                           side: str,
                           amount: float,
                           price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        """
        下单实现 (应用乘数逻辑)
        """
        info = self._get_market_info(symbol)

        # 1. 转换数值为 Lighter 的整数格式
        amount_int = int(amount * info['size_mul'])
        price_int = int(price * info['price_mul']) if price else 0

        is_ask = True if side.lower() == 'sell' else False

        # 生成唯一 ID
        client_order_index = int(time.time() * 1000) % 1000000

        try:
            res, tx_hash, err = None, None, None

            if order_type == "MARKET":
                # 市价单逻辑
                res, tx_hash, err = await self.client.create_market_order(
                    market_index=info['id'],
                    client_order_index=client_order_index,
                    base_amount=amount_int,
                    avg_execution_price=price_int,  # 市价单的保护价格
                    is_ask=is_ask
                )
            else:
                # 限价单逻辑 (SignerClient 内置方法)
                # 参考提供的代码：create_limit_order
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

            print(f"✅ [Lighter] Order Sent: {tx_hash}")
            return str(client_order_index)  # 返回 client_id 方便追踪

        except Exception as e:
            print(f"❌ [Lighter] Create Exception: {e}")
            return None

    async def get_funding_rate(self, symbol: str) -> float:
        # Lighter 是 ZK-Orderbook，费率机制特殊，暂时返回 0
        # 实际可能需要查询 perpetual details
        return 0.0

    async def listen_websocket(self, queue: asyncio.Queue):
        """
        WebSocket 监听 (重写版)
        由于没有 LighterCustomWebSocketManager，我们用 websockets 库直接实现
        """
        ws_url = self.base_url.replace("https", "wss") + "/stream"
        print(f"📡 [Lighter] Connecting WS: {ws_url}")

        while True:
            try:
                async with websockets.connect(ws_url) as ws:
                    # 1. 订阅
                    # 假设我们需要订阅 WBTC-USDC (ID=1) 的 orderbook
                    # 这里需遍历我们缓存的所有 market_id 进行订阅
                    for symbol, info in self.market_config.items():
                        sub_msg = {
                            "type": "subscribe",
                            "channel": "orderbook",
                            "marketId": info['id']
                        }
                        await ws.send(json.dumps(sub_msg))

                    # 2. 循环接收
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)

                        # 处理心跳 (参考源码 logic)
                        if data.get("type") == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue

                        # 处理数据更新
                        # Lighter WS 数据结构需参考具体文档，这里做通用解析假设
                        if "type" in data and data["type"] == "orderbook":
                            # 提取 best bid/ask 并放入队列
                            # 伪代码：需要根据实际 WS 报文调整字段
                            pass

            except Exception as e:
                print(f"⚠️ [Lighter] WS Disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
