import asyncio
import time
import os
import logging
import traceback
from decimal import Decimal
from typing import Dict, Optional, Any

# 引入 GRVT SDK 核心组件
from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv
from pysdk.grvt_ccxt_logging_selector import logger as sdk_logger

from .base import BaseExchange


class GrvtAdapter(BaseExchange):
    """
    GRVT 交易所适配器 (基于官方 test_grvt_ccxt_ws.py 重构)
    """

    def __init__(self, api_key: str, private_key: str, trading_account_id: str):
        super().__init__("GRVT")

        self.api_key = api_key
        self.private_key = private_key
        self.trading_account_id = trading_account_id

        # 环境配置
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

    async def initialize(self):
        """
        初始化：REST 和 WS
        """
        try:
            # 1. 初始化 REST Client (用于获取市场信息)
            params = {
                'trading_account_id': self.trading_account_id,
                'private_key': self.private_key,
                'api_key': self.api_key
            }
            self.rest_client = GrvtCcxt(env=self.env, parameters=params)

            # 2. 动态加载市场配置
            print(f"⏳ [GRVT] Fetching markets from {self.env.name}...")
            markets = await self._fetch_markets_async()

            loaded_count = 0
            for market in markets:
                if market.get('kind') == 'PERPETUAL' and market.get('quote') == 'USDT':
                    symbol = f"{market.get('base')}-{market.get('quote')}"
                    self.contract_map[symbol] = {
                        "id": market.get('instrument'),
                        "tick_size": Decimal(str(market.get('tick_size', 0))),
                        "min_size": Decimal(str(market.get('min_size', 0)))
                    }
                    loaded_count += 1

            print(f"   - Loaded {loaded_count} markets.")

            # 3. 初始化 WS Client (完全参考官方示例)
            loop = asyncio.get_running_loop()

            # 官方示例要求的 WS 参数
            ws_params = {
                'api_key': self.api_key,
                'trading_account_id': self.trading_account_id,
                'api_ws_version': 'v1',  # 关键：指定版本
                'private_key': self.private_key
            }

            # 传入 loop 和 logger
            self.ws_client = GrvtCcxtWS(
                env=self.env,
                loop=loop,
                logger=sdk_logger,
                parameters=ws_params
            )

            await self.ws_client.initialize()

            # 给一点时间建立连接
            await asyncio.sleep(1)

            self.is_connected = True
            print(f"✅ [GRVT] Initialized.")

        except Exception as e:
            print(f"❌ [GRVT] Init Failed: {e}")
            traceback.print_exc()
            await self.close()  # 清理资源
            raise e

    async def _fetch_markets_async(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.rest_client.fetch_markets)

    def _get_contract_info(self, symbol: str):
        info = self.contract_map.get(symbol)
        if not info:
            raise ValueError(f"Market {symbol} not found in GRVT configs")
        return info

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """REST 获取订单簿 (作为备用)"""
        info = self._get_contract_info(symbol)
        loop = asyncio.get_running_loop()
        ob = await loop.run_in_executor(None, lambda: self.rest_client.fetch_order_book(info['id'], limit=10))

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

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        info = self._get_contract_info(symbol)
        qty = float(Decimal(str(amount)))
        px = float(Decimal(str(price))) if price else 0.0

        params = {
            'post_only': True,
            'order_duration_secs': 2591999  # 30天
        }

        loop = asyncio.get_running_loop()
        try:
            if order_type == "MARKET":
                res = await loop.run_in_executor(None, lambda: self.rest_client.create_order(
                    symbol=info['id'], type='market', side=side, amount=qty, params=params
                ))
            else:
                res = await loop.run_in_executor(None, lambda: self.rest_client.create_limit_order(
                    symbol=info['id'], side=side, amount=qty, price=px, params=params
                ))
            return res['id']
        except Exception as e:
            print(f"❌ [GRVT] Order Error: {e}")
            return None

    async def listen_websocket(self, queue: asyncio.Queue):
        """
        WS 监听 (基于官方示例的回调模式)
        """
        print(f"📡 [GRVT] Starting WS subscriptions...")
        loop = asyncio.get_running_loop()

        # --- 回调处理 ---
        async def message_callback(message: Dict[str, Any]):
            """通用回调处理"""
            try:
                # 提取 instrument
                # 官方示例：message.get("feed", {}).get("instrument")
                feed_data = message.get("feed", {})
                instrument = feed_data.get("instrument")
                channel = message.get("params", {}).get("channel")  # e.g. book.s, order

                # 1. 处理订单簿快照 (book.s)
                if channel == "book.s":
                    # 解析 snapshot
                    # 结构通常是: feed: { bids: [], asks: [], ... }
                    bids = feed_data.get("bids", [])
                    asks = feed_data.get("asks", [])

                    if bids and asks:
                        best_bid = float(bids[0]['price'])
                        best_ask = float(asks[0]['price'])

                        # 找到对应的通用 Symbol (BTC-USDT)
                        symbol = None
                        for s, info in self.contract_map.items():
                            if info['id'] == instrument:
                                symbol = s
                                break

                        if symbol:
                            tick = {
                                'exchange': self.name,
                                'symbol': symbol,
                                'bid': best_bid,
                                'ask': best_ask,
                                'ts': int(time.time() * 1000)
                            }
                            # 放入队列
                            loop.call_soon_threadsafe(queue.put_nowait, tick)

                # 2. 处理订单更新 (order)
                elif channel == "order":
                    # 打印订单状态
                    state = feed_data.get("state", {})
                    print(f"🔔 [GRVT] Order Update [{instrument}]: {state.get('status')}")

            except Exception as e:
                # 生产环境请使用 logging
                pass

        # --- 执行订阅 ---
        # 参考官方示例：await api.subscribe(stream=stream, callback=callback, params=params)

        for symbol, info in self.contract_map.items():
            instrument_id = info['id']

            # 1. 订阅公共行情 (book.s = Snapshot)
            # 官方示例 pub_args_dict
            await self.ws_client.subscribe(
                stream="book.s",
                callback=message_callback,
                params={"instrument": instrument_id}
            )

            # 2. 订阅私有订单 (order)
            # 官方示例 prv_args_dict: 必须传 sub_account_id
            await self.ws_client.subscribe(
                stream="order",
                callback=message_callback,
                params={
                    "instrument": instrument_id,
                    "sub_account_id": self.trading_account_id  # 关键！
                }
            )

            # 为了防止并发订阅过快，加一点点延迟 (参考官方示例里的 sleep(0))
            await asyncio.sleep(0)

        print(f"✅ [GRVT] Subscribed to {len(self.contract_map)} markets.")

        # 保持连接
        while True:
            await asyncio.sleep(1)

    async def close(self):
        """清理资源"""
        if self.rest_client and hasattr(self.rest_client, '_session') and self.rest_client._session:
            await self.rest_client._session.close()

        # WS 清理逻辑，参考 shutdown
        # 这里简单处理，实际可能需要 cancel task
        pass