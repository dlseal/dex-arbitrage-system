import asyncio
import time
import os
from decimal import Decimal
from typing import Dict, Optional, Any

# 引入 GRVT SDK 核心组件
from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv, GrvtWSEndpointType
from pysdk.grvt_ccxt_logging_selector import logger as sdk_logger

from .base import BaseExchange


class GrvtAdapter(BaseExchange):
    """
    GRVT 交易所适配器 (基于参考实现重构)
    特点：REST与WS分离，动态获取合约配置
    """

    def __init__(self, api_key: str, private_key: str, trading_account_id: str):
        super().__init__("GRVT", api_key, private_key)
        self.trading_account_id = trading_account_id

        # 1. 环境配置 (参考代码逻辑)
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

        # 缓存：合约配置 { "BTC-USDT": {"id": "BTC_USDT_Perp", "tick_size": Decimal("0.1")} }
        self.contract_map = {}

    async def initialize(self):
        """
        初始化：建立 REST 连接，获取市场配置，建立 WS 连接
        """
        try:
            # 1. 初始化 REST Client
            params = {
                'trading_account_id': self.trading_account_id,
                'private_key': self.private_key,
                'api_key': self.api_key
            }
            self.rest_client = GrvtCcxt(env=self.env, parameters=params)

            # 2. 动态加载市场配置 (参考源码 get_contract_attributes)
            print(f"⏳ [GRVT] Fetching markets from {self.env.name}...")
            markets = await self._fetch_markets_async()  # 包装同步方法为异步

            for market in markets:
                # 提取关键字段
                base = market.get('base')  # e.g., BTC
                quote = market.get('quote')  # e.g., USDT
                kind = market.get('kind')  # e.g., PERPETUAL
                instrument = market.get('instrument')  # e.g., BTC_USDT_Perp
                tick_size = Decimal(str(market.get('tick_size', 0)))

                if kind == 'PERPETUAL' and quote == 'USDT':
                    # 构建统一 symbol: "BTC-USDT"
                    symbol = f"{base}-{quote}"
                    self.contract_map[symbol] = {
                        "id": instrument,
                        "tick_size": tick_size,
                        "min_size": Decimal(str(market.get('min_size', 0)))
                    }
                    print(f"   - Loaded {symbol} -> {instrument}")

            # 3. 初始化 WS Client
            loop = asyncio.get_running_loop()
            ws_params = {
                'api_key': self.api_key,
                'trading_account_id': self.trading_account_id,
                'api_ws_version': 'v1',
                'private_key': self.private_key
            }

            self.ws_client = GrvtCcxtWS(
                env=self.env,
                loop=loop,
                logger=sdk_logger,
                parameters=ws_params
            )

            await self.ws_client.initialize()
            # 等待连接建立 (参考代码做法)
            await asyncio.sleep(2)

            self.is_connected = True
            print(f"✅ [GRVT] Initialized. Account: {self.trading_account_id}")

        except Exception as e:
            print(f"❌ [GRVT] Init Failed: {e}")
            raise e

    async def _fetch_markets_async(self):
        """Helper: 将同步的 fetch_markets 包装为异步"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.rest_client.fetch_markets)

    def _get_contract_info(self, symbol: str):
        info = self.contract_map.get(symbol)
        if not info:
            # 尝试模糊匹配，比如 symbol 是 "BTC_USDT_Perp"
            for k, v in self.contract_map.items():
                if v['id'] == symbol:
                    return v
            raise ValueError(f"Market {symbol} not found in GRVT configs")
        return info

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """
        获取订单簿 (REST)
        """
        info = self._get_contract_info(symbol)

        # 调用 SDK (同步方法需包装)
        loop = asyncio.get_running_loop()
        ob = await loop.run_in_executor(None,
                                        lambda: self.rest_client.fetch_order_book(info['id'], limit=10)
                                        )

        bids = ob.get('bids', [])
        asks = ob.get('asks', [])

        best_bid = float(bids[0]['price']) if bids else 0.0
        best_ask = float(asks[0]['price']) if asks else 0.0

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
        下单 (包含 Post Only 逻辑)
        """
        info = self._get_contract_info(symbol)

        # 数量/价格 转换为 Decimal 并按 tick_size 取整 (可复用 helper)
        qty_decimal = Decimal(str(amount))
        price_decimal = Decimal(str(price)) if price else Decimal("0")

        params = {}
        if order_type == "LIMIT":
            # 参考代码：默认开启 Post Only 以确保 Maker 费率
            params['post_only'] = True
            # 设置签名有效期 (参考代码：30天)
            params['order_duration_secs'] = 30 * 86400 - 1

        loop = asyncio.get_running_loop()

        try:
            if order_type == "MARKET":
                # 市价单
                # GRVT SDK 的 create_market_order 用法需确认，通常 CCXT 风格如下:
                result = await loop.run_in_executor(None, lambda: self.rest_client.create_order(
                    symbol=info['id'],
                    type='market',
                    side=side,
                    amount=float(qty_decimal),  # SDK 可能需要 float
                    params=params
                ))
            else:
                # 限价单 (使用 create_limit_order 更明确)
                result = await loop.run_in_executor(None, lambda: self.rest_client.create_limit_order(
                    symbol=info['id'],
                    side=side,
                    amount=float(qty_decimal),
                    price=float(price_decimal),
                    params=params
                ))

            # 提取 Order ID
            # 参考代码：result.get('metadata').get('client_order_id') 或 result['id']
            # 这里返回 result['id'] (GRVT Order ID)
            return result['id']

        except Exception as e:
            print(f"❌ [GRVT] Create Order Error: {e}")
            return None

    async def get_funding_rate(self, symbol: str) -> float:
        # 需要包装 fetch_funding_rate
        return 0.0

    async def listen_websocket(self, queue: asyncio.Queue):
        """
        WS 监听 (同时处理 Orderbook 和 订单更新)
        """
        print(f"📡 [GRVT] Starting WS subscriptions...")

        # 1. 定义 Orderbook 回调 (用于策略行情)
        async def ob_callback(msg: Dict[str, Any]):
            # 解析 GRVT book 推送 (需确认具体结构，通常含有 bids/asks)
            # 这里简化处理，假设 msg 包含 feed 数据
            try:
                # 注意：这里需要根据实际 book 推送结构解析
                # 参考代码只处理了 order 更新，我们需要查阅文档补充 book 解析
                pass
            except Exception as e:
                print(f"⚠️ [GRVT] OB Parse Error: {e}")

        # 2. 定义 订单更新 回调 (参考代码的核心逻辑)
        async def order_callback(msg: Dict[str, Any]):
            try:
                if 'feed' in msg:
                    data = msg.get('feed', {})
                    # 深度解析逻辑 (完全复刻参考代码)
                    leg = data.get('legs', [])[0] if data.get('legs') else None
                    if leg:
                        order_state = data.get('state', {})
                        status = order_state.get('status', '')
                        # 可以在这里打印日志，或者推送到另外一个 UserDataQueue
                        print(f"🔔 [GRVT] Order Update: {status} | Filled: {order_state.get('traded_size')}")
            except Exception as e:
                print(f"⚠️ [GRVT] Order Parse Error: {e}")

        # 3. 执行订阅
        # 订阅行情 (Orderbook) - 假设 stream='book'
        for symbol, info in self.contract_map.items():
            # 注意：GRVT WS 订阅公有频道可能不需要 RPC_FULL，需确认 EndpointType
            # 这里先演示订阅私有订单流，因为参考代码只有这个
            await self.ws_client.subscribe(
                stream="order",
                callback=order_callback,
                ws_end_point_type=GrvtWSEndpointType.TRADE_DATA_RPC_FULL,
                params={"instrument": info['id']}
            )

            # TODO: 订阅公有 Orderbook
            # await self.ws_client.subscribe(stream="book", ..., params={"instrument": info['id'], "depth": 10})

        # 保持连接活跃
        while True:
            await asyncio.sleep(1)

    async def close(self):
        if self.ws_client:
            await self.ws_client.__aexit__()
