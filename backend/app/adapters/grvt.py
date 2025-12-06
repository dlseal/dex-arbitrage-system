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

                    # 🔴 修复：兼容 Full模式(tick_size) 和 Lite模式(ts)
                    raw_id = market.get('instrument') or market.get('i')
                    raw_tick = market.get('tick_size') or market.get('ts') or 0
                    raw_min = market.get('min_size') or market.get('ms') or 0

                    self.contract_map[symbol] = {
                        "id": raw_id,
                        "tick_size": Decimal(str(raw_tick)),
                        "min_size": Decimal(str(raw_min))
                    }
                    loaded_count += 1
                    logging.info(f"   - Loaded {symbol}: Tick={raw_tick}, Min={raw_min}")

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
        # 传入 params 确保尽可能获取完整信息
        return await loop.run_in_executor(None, lambda: self.rest_client.fetch_markets(params={}))

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
        min_size = info.get('min_size')

        # 2. 数量精度修正
        d_amount = Decimal(str(amount))
        if min_size and min_size > 0:
            d_amount = (d_amount / min_size).to_integral_value(rounding='ROUND_DOWN') * min_size
        qty = float(d_amount)

        # 3. 价格精度修正
        px = 0.0
        if price:
            d_price = Decimal(str(price))
            if tick_size and tick_size > 0:
                # 🔴 核心修复：确保价格符合 Tick 精度 (例如 89444.56 -> 89444.6 如果tick=0.1)
                d_price = (d_price / tick_size).to_integral_value(rounding='ROUND_HALF_UP') * tick_size
            px = float(d_price)

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
            # 兼容不同格式的返回值
            if isinstance(res, dict):
                # 优先获取系统 ID
                oid = res.get('id') or res.get('order_id')

                # 获取客户端 ID (尝试从根目录或 metadata 中获取)
                cid = str(res.get('client_order_id', ''))
                if not cid and 'metadata' in res:
                    cid = str(res.get('metadata', {}).get('client_order_id', ''))

                # 修复：如果系统 ID 为 0x00 或空，则必须使用 client_order_id
                if not oid or oid == "0x00":
                    if cid:
                        return cid
                    else:
                        logging.error(f"❌ [GRVT] 下单返回无效 ID: {res}")
                        return None
                return oid
            return str(res)

        except Exception as e:
            logging.error(f"❌ [GRVT] Order Error: {e} | Side:{side} Qty:{qty} Price:{px}")
            return None

    async def cancel_order(self, order_id: str):
        """智能撤单：自动识别 order_id 或 client_order_id"""
        loop = asyncio.get_running_loop()
        try:
            # 如果 ID 是纯数字，视为 client_order_id
            if str(order_id).isdigit():
                return await loop.run_in_executor(None, lambda: self.rest_client.cancel_order(
                    id=None,
                    symbol=None,
                    params={'client_order_id': int(order_id)}
                ))
            else:
                # 否则视为系统 order_id
                return await loop.run_in_executor(None, lambda: self.rest_client.cancel_order(id=order_id))
        except Exception as e:
            logging.error(f"❌ [GRVT] Cancel Error: {e}")
            raise e

    async def fetch_order(self, order_id: str):
        """智能查单：自动识别 order_id 或 client_order_id"""
        loop = asyncio.get_running_loop()
        try:
            if str(order_id).isdigit():
                return await loop.run_in_executor(None, lambda: self.rest_client.fetch_order(
                    id=None,
                    symbol=None,
                    params={'client_order_id': int(order_id)}
                ))
            else:
                return await loop.run_in_executor(None, lambda: self.rest_client.fetch_order(id=order_id))
        except Exception as e:
            # logging.warning(f"⚠️ [GRVT] Fetch Error: {e}")
            raise e

    async def listen_websocket(self, queue: asyncio.Queue):
        # 保持原有的 WebSocket 逻辑不变，这里省略以节省篇幅，请保留您原文件中的 listen_websocket 代码
        logging.info(f"📡 [GRVT] Starting WS subscriptions...")
        loop = asyncio.get_running_loop()

        async def message_callback(message: Dict[str, Any]):
            try:
                feed_data = message.get("feed", {})
                if "instrument" not in feed_data and "instrument" in message:
                    feed_data = message

                channel = message.get("params", {}).get("channel")
                if not channel:
                    channel = message.get("stream")

                # 处理订单更新
                if channel and "order" in str(channel) and "book" not in str(channel):
                    order_state = feed_data.get("state")
                    if order_state in ["FILLED", "PARTIALLY_FILLED"]:
                        instrument = feed_data.get("instrument")
                        symbol_base = self._get_symbol_from_instrument(instrument)

                        event = {
                            'type': 'trade',
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'side': feed_data.get("side"),
                            'price': float(feed_data.get("price", 0)),
                            'size': float(feed_data.get("size", 0)),
                            'ts': int(time.time() * 1000)
                        }
                        loop.call_soon_threadsafe(queue.put_nowait, event)
                    return

                # 处理 Orderbook
                instrument = feed_data.get("instrument")
                symbol_base = self._get_symbol_from_instrument(instrument)

                if symbol_base == "UNKNOWN":
                    return

                if channel and "book" in str(channel):
                    bids = feed_data.get("bids", [])
                    asks = feed_data.get("asks", [])

                    if bids and asks:
                        best_bid = float(bids[0]['price'])
                        best_ask = float(asks[0]['price'])

                        tick = {
                            'type': 'tick',
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
            await self.ws_client.subscribe(
                stream="book.s",
                callback=message_callback,
                params={"instrument": instrument_id}
            )
            await self.ws_client.subscribe(
                stream="order",
                callback=message_callback,
                params={"instrument": instrument_id, "sub_account_id": self.trading_account_id}
            )
            await asyncio.sleep(0.1)

        while True:
            await asyncio.sleep(1)