import asyncio
import time
import os
import logging
import traceback
from decimal import Decimal
from typing import Dict, Optional, Any, List

# 引入 GRVT SDK
from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv, GrvtWSEndpointType
from pysdk.grvt_ccxt_logging_selector import logger as sdk_logger

from .base import BaseExchange


class GrvtAdapter(BaseExchange):
    """
    GRVT 交易所适配器 (增强版：修复资源清理与超时重试)
    """

    def __init__(self,
                 api_key: str,
                 private_key: str,
                 trading_account_id: str,
                 symbols: List[str] = None):

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

        # rest_client 是同步的，ws_client 是异步的
        self.rest_client: Optional[GrvtCcxt] = None
        self.ws_client: Optional[GrvtCcxtWS] = None
        self.contract_map = {}

    async def initialize(self):
        """
        初始化：带重试机制 (增强版)
        """
        retry_count = 5  # 增加重试次数
        for attempt in range(retry_count):
            try:
                print(f"⏳ [GRVT] 正在连接 WS (第 {attempt + 1} 次尝试)...")
                await self._initialize_logic()
                print("✅ [GRVT] 连接成功！")
                return
            except Exception as e:
                logging.warning(f"⚠️ [GRVT] 连接失败: {e}")
                # 每次失败等待时间加长 (3s, 6s, 9s...)
                wait_time = (attempt + 1) * 3
                print(f"   -> 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

        # 如果全部失败
        logging.error("❌ [GRVT] 无法建立连接，请检查网络/VPN！")
        # raise e

    async def _initialize_logic(self):
        # 1. 初始化 REST (同步)
        params = {
            'trading_account_id': self.trading_account_id,
            'private_key': self.private_key,
            'api_key': self.api_key
        }
        self.rest_client = GrvtCcxt(env=self.env, parameters=params)

        # 2. 动态加载市场
        print(f"⏳ [GRVT] Fetching markets from {self.env.name}...")
        markets = await self._fetch_markets_async()

        loaded_count = 0
        for market in markets:
            base = market.get('base')
            quote = market.get('quote')
            kind = market.get('kind')

            if kind == 'PERPETUAL' and quote == 'USDT':
                if base in self.target_symbols:
                    symbol = f"{base}-{quote}"
                    self.contract_map[symbol] = {
                        "id": market.get('instrument'),
                        "tick_size": Decimal(str(market.get('tick_size', 0))),
                        "min_size": Decimal(str(market.get('min_size', 0)))
                    }
                    loaded_count += 1

        if loaded_count == 0:
            print(f"⚠️ [GRVT] Warning: No target markets found for {self.target_symbols}")

        # 3. 初始化 WS
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

        await self.ws_client.initialize()  # 这里最容易超时
        await asyncio.sleep(1)

        self.is_connected = True
        print(f"✅ [GRVT] Initialized. Monitoring: {self.target_symbols}")

    async def _fetch_markets_async(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.rest_client.fetch_markets)

    def _get_contract_info(self, symbol: str):
        if "-" not in symbol: symbol = f"{symbol}-USDT"
        info = self.contract_map.get(symbol)
        if not info:
            raise ValueError(f"Market {symbol} not found (Targets: {self.target_symbols})")
        return info

    # --- 修复后的 close 方法 ---
    async def close(self):
        """安全清理资源"""
        # 1. 清理 WS 客户端 (异步)
        if self.ws_client:
            try:
                # 尝试访问内部 session 并关闭
                if hasattr(self.ws_client, '_session') and self.ws_client._session:
                    if not self.ws_client._session.closed:
                        await self.ws_client._session.close()
            except Exception as e:
                print(f"⚠️ [GRVT] WS Close Error: {e}")

        # 2. 清理 REST 客户端 (同步)
        # 注意：GrvtCcxt 是同步的，通常不需要 await 关闭，或者它没有显式的 close 方法
        # 这里什么都不做，或者如果它有 close() 就同步调用
        if self.rest_client:
            pass

            # --- 其他方法保持不变 ---

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        info = self._get_contract_info(symbol)
        loop = asyncio.get_running_loop()
        ob = await loop.run_in_executor(None, lambda: self.rest_client.fetch_order_book(info['id'], limit=10))
        bids = ob.get('bids', [])
        asks = ob.get('asks', [])
        best_bid = float(bids[0]['price']) if bids else 0.0
        best_ask = float(asks[0]['price']) if asks else 0.0
        return {
            'exchange': self.name,
            'symbol': symbol.split('-')[0],
            'bid': best_bid,
            'ask': best_ask,
            'ts': int(time.time() * 1000)
        }

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        info = self._get_contract_info(symbol)
        qty = float(Decimal(str(amount)))
        px = float(Decimal(str(price))) if price else 0.0
        params = {'post_only': True, 'order_duration_secs': 2591999}
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
        print(f"📡 [GRVT] Starting WS subscriptions...")
        loop = asyncio.get_running_loop()

        async def message_callback(message: Dict[str, Any]):
            try:
                # 👇 调试关键：打印接收到的消息结构，确认 feed 在哪
                # logger.debug(f"[GRVT RAW] {str(message)[:100]}...")

                feed_data = message.get("feed", {})

                # 如果 message 本身就是 feed 数据 (有些 SDK 版本不同)
                if "instrument" not in feed_data and "instrument" in message:
                    feed_data = message

                instrument = feed_data.get("instrument")
                channel = message.get("params", {}).get("channel")

                # 如果 SDK 返回的结构不同，尝试从 payload 里找
                if not channel:
                    channel = message.get("stream")  # 可能是 "v1.book.s"

                symbol_base = None
                for s, info in self.contract_map.items():
                    if info['id'] == instrument:
                        symbol_base = s.split('-')[0]
                        break

                # 如果没找到 instrument，可能是心跳或控制消息，忽略
                if not symbol_base:
                    return

                # 处理 Orderbook 数据
                if "book" in str(channel):  # 兼容 "book.s" 和 "v1.book.s"
                    bids = feed_data.get("bids", [])
                    asks = feed_data.get("asks", [])

                    if bids and asks:
                        # GRVT 价格通常也是字符串，转 float
                        best_bid = float(bids[0]['price'])
                        best_ask = float(asks[0]['price'])

                        tick = {
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'bid': best_bid,
                            'ask': best_ask,
                            'ts': int(time.time() * 1000)
                        }
                        # 👇 这里的 put_nowait 是将数据推进引擎的关键
                        loop.call_soon_threadsafe(queue.put_nowait, tick)

            except Exception as e:
                # 🔴 关键修复：打印错误堆栈！不要 pass！
                print(f"❌ [GRVT Callback Error] {e} | Msg: {str(message)[:50]}")
                # traceback.print_exc()

        for symbol, info in self.contract_map.items():
            instrument_id = info['id']
            # 订阅公共行情
            await self.ws_client.subscribe(
                stream="book.s",
                callback=message_callback,
                params={"instrument": instrument_id}
            )
            # 订阅私有订单
            await self.ws_client.subscribe(
                stream="order",
                callback=message_callback,
                params={"instrument": instrument_id, "sub_account_id": self.trading_account_id}
            )
            await asyncio.sleep(0)

        while True:
            await asyncio.sleep(1)