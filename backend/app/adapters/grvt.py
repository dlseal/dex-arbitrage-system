import asyncio
import time
import os
import logging
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
    GRVT 交易所适配器 (增强版：支持成交推送)
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
        """初始化：带重试机制"""
        retry_count = 5
        for attempt in range(retry_count):
            try:
                logging.info(f"⏳ [GRVT] 正在连接 WS (第 {attempt + 1} 次尝试)...")
                await self._initialize_logic()
                logging.info("✅ [GRVT] 连接成功！")
                return
            except Exception as e:
                logging.warning(f"⚠️ [GRVT] 连接失败: {e}")
                wait_time = (attempt + 1) * 3
                logging.info(f"   -> 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

        logging.error("❌ [GRVT] 无法建立连接，请检查网络/VPN！")

    async def _initialize_logic(self):
        # 1. 初始化 REST (同步)
        params = {
            'trading_account_id': self.trading_account_id,
            'private_key': self.private_key,
            'api_key': self.api_key
        }
        self.rest_client = GrvtCcxt(env=self.env, parameters=params)

        # 2. 动态加载市场
        logging.info(f"⏳ [GRVT] Fetching markets from {self.env.name}...")
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
            logging.info(f"⚠️ [GRVT] Warning: No target markets found for {self.target_symbols}")

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

        await self.ws_client.initialize()
        await asyncio.sleep(1)

        self.is_connected = True
        logging.info(f"✅ [GRVT] Initialized. Monitoring: {self.target_symbols}")

    async def _fetch_markets_async(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.rest_client.fetch_markets)

    def _get_contract_info(self, symbol: str):
        if "-" not in symbol: symbol = f"{symbol}-USDT"
        info = self.contract_map.get(symbol)
        if not info:
            raise ValueError(f"Market {symbol} not found (Targets: {self.target_symbols})")
        return info

    def _get_symbol_from_instrument(self, instrument_id):
        """辅助方法：通过 ID 反查 Symbol"""
        for s, info in self.contract_map.items():
            if info['id'] == instrument_id:
                return s.split('-')[0]
        return "UNKNOWN"

    async def close(self):
        """安全清理资源"""
        if self.ws_client:
            try:
                if hasattr(self.ws_client, '_session') and self.ws_client._session:
                    if not self.ws_client._session.closed:
                        await self.ws_client._session.close()
            except Exception as e:
                logging.info(f"⚠️ [GRVT] WS Close Error: {e}")

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

        # 1. 获取市场精度配置 (Decimal类型)
        tick_size = info.get('tick_size')
        min_size = info.get('min_size')  # 通常也是步长

        # 2. 数量精度修正
        # 将数量转换为 Decimal
        d_amount = Decimal(str(amount))
        if min_size and min_size > 0:
            # 逻辑：(数量 / 步长) 取整 * 步长
            # 例如: amount=0.1234, min_size=0.01 -> 12.34 -> 12 -> 0.12
            d_amount = (d_amount / min_size).to_integral_value(rounding='ROUND_DOWN') * min_size

        qty = float(d_amount)

        # 3. 价格精度修正
        px = 0.0
        if price:
            d_price = Decimal(str(price))
            if tick_size and tick_size > 0:
                # 逻辑：(价格 / Tick) 取整 * Tick
                # 例如: price=89444.56, tick=0.1 -> 894445.6 -> 894446 (四舍五入) -> 89444.6
                d_price = (d_price / tick_size).to_integral_value(rounding='ROUND_HALF_UP') * tick_size
            px = float(d_price)

        # 4. 强制小写 (修复之前的 'side' 报错)
        side = side.lower()

        # 默认 Post Only
        params = {'post_only': True, 'order_duration_secs': 2591999}
        if order_type == "MARKET":
            params = {}

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
            # 打印修正后的参数，方便调试
            logging.error(f"❌ [GRVT] Order Error: {e} | Side:{side} Qty:{qty} Price:{px}")
            return None

    async def listen_websocket(self, queue: asyncio.Queue):
        logging.info(f"📡 [GRVT] Starting WS subscriptions...")
        loop = asyncio.get_running_loop()

        async def message_callback(message: Dict[str, Any]):
            try:
                feed_data = message.get("feed", {})

                # 兼容不同 SDK 版本的结构
                if "instrument" not in feed_data and "instrument" in message:
                    feed_data = message

                channel = message.get("params", {}).get("channel")
                # 如果 SDK 返回的结构不同，尝试从 payload 里找
                if not channel:
                    channel = message.get("stream")

                # --- 1. 处理订单/成交事件 (Order Update) ---
                # 注意：GRVT WS 频道名称可能为 'order' 或 'v1.order'
                if channel and "order" in str(channel) and "book" not in str(channel):
                    order_state = feed_data.get("state")
                    # 只有当状态为已成交或部分成交时，才触发对冲
                    if order_state in ["FILLED", "PARTIALLY_FILLED"]:
                        instrument = feed_data.get("instrument")
                        symbol_base = self._get_symbol_from_instrument(instrument)

                        event = {
                            'type': 'trade',  # 标记为交易事件
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'side': feed_data.get("side"),  # BUY/SELL
                            'price': float(feed_data.get("price", 0)),
                            'size': float(feed_data.get("size", 0)),
                            'ts': int(time.time() * 1000)
                        }
                        # 🚨 必须使用 call_soon_threadsafe 放入队列
                        loop.call_soon_threadsafe(queue.put_nowait, event)
                    return

                # --- 2. 处理 Orderbook 数据 ---
                # 找到对应的 Instrument
                instrument = feed_data.get("instrument")
                symbol_base = self._get_symbol_from_instrument(instrument)

                if symbol_base == "UNKNOWN":
                    return

                # 处理 Orderbook 数据
                if channel and "book" in str(channel):  # 兼容 "book.s" 和 "v1.book.s"
                    bids = feed_data.get("bids", [])
                    asks = feed_data.get("asks", [])

                    if bids and asks:
                        best_bid = float(bids[0]['price'])
                        best_ask = float(asks[0]['price'])

                        tick = {
                            'type': 'tick',  # 标记为行情事件
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'bid': best_bid,
                            'ask': best_ask,
                            'ts': int(time.time() * 1000)
                        }
                        loop.call_soon_threadsafe(queue.put_nowait, tick)

            except Exception as e:
                logging.warning(f"❌ [GRVT Callback Error] {e}")

        for symbol, info in self.contract_map.items():
            instrument_id = info['id']
            # 订阅公共行情
            await self.ws_client.subscribe(
                stream="book.s",
                callback=message_callback,
                params={"instrument": instrument_id}
            )
            # 订阅私有订单 (关键)
            await self.ws_client.subscribe(
                stream="order",
                callback=message_callback,
                params={"instrument": instrument_id, "sub_account_id": self.trading_account_id}
            )
            await asyncio.sleep(0.1)

        while True:
            await asyncio.sleep(1)