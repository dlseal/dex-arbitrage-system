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
            # 🔴 核心修复：增加 5 秒超时控制，防止网络请求卡死主线程
            if order_type == "MARKET":
                res, tx_hash, err = await asyncio.wait_for(
                    self.client.create_market_order(
                        market_index=info['id'], client_order_index=client_order_index,
                        base_amount=amount_int, avg_execution_price=price_int, is_ask=is_ask
                    ),
                    timeout=5.0
                )
            else:
                res, tx_hash, err = await asyncio.wait_for(
                    self.client.create_limit_order(
                        market_index=info['id'], client_order_index=client_order_index,
                        base_amount=amount_int, price=price_int, is_ask=is_ask,
                        time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
                    ),
                    timeout=5.0
                )

            if err:
                logging.error(f"❌ [Lighter] Order Error: {err}")
                return None
            return str(client_order_index)

        except asyncio.TimeoutError:
            logging.error(f"❌ [Lighter] Order Timeout (5s) - API 未响应，跳过等待")
            return None
        except Exception as e:
            logging.error(f"❌ [Lighter] Create Exception: {e}")
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

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        """
        监听 WebSocket 数据流，并将数据分流到不同的队列
        :param tick_queue: 行情队列 (允许丢包)
        :param event_queue: 事件队列 (严禁丢包，用于成交回报)
        """
        logging.info(f"📡 [GRVT] Starting WS subscriptions (Fixed Logic)...")
        loop = asyncio.get_running_loop()

        async def message_callback(message: Dict[str, Any]):
            try:
                # 1. 提取 Feed 数据
                feed_data = message.get("feed", {})
                if not feed_data and "instrument" in message:
                    feed_data = message

                channel = message.get("params", {}).get("channel")
                if not channel:
                    channel = message.get("stream")

                # ------------------- 核心分流逻辑 (已修复) -------------------

                # 2. 处理订单更新 (Order Update) -> 推送至 event_queue
                if channel and "order" in str(channel) and "book" not in str(channel):
                    state = feed_data.get("state", {})
                    status = state.get("status", "").upper()

                    # 只要有成交发生 (无论完全成交还是部分成交)
                    if status in ["FILLED", "PARTIALLY_FILLED"]:
                        legs = feed_data.get("legs", [])

                        # 🔴 核心修复：严禁使用 state['traded_size'] (这是累计值)
                        # 必须遍历 legs 计算当次事件的真实增量成交量 (Delta)
                        filled_size = sum(float(l.get("size", 0)) for l in legs)

                        if filled_size > 0 and legs:
                            leg = legs[0]  # 取第一个 leg 获取元数据
                            instrument = leg.get("instrument")
                            symbol_base = self._get_symbol_from_instrument(instrument)

                            # 确定方向
                            is_buy = leg.get("is_buying_asset", False)
                            side = "BUY" if is_buy else "SELL"
                            price = float(leg.get("limit_price", 0))

                            # 尝试获取 order_id，用于后续策略清理残余订单
                            # 优先顺序：message根层级 -> feed数据 -> state数据
                            order_id = message.get('order_id') or feed_data.get('order_id') or state.get('order_id')

                            event = {
                                'type': 'trade',
                                'exchange': self.name,
                                'symbol': symbol_base,
                                'side': side,
                                'price': price,
                                'size': filled_size,  # 这里的 size 已经是正确的增量了
                                'order_id': order_id,  # 传递 ID 给策略
                                'status': status,  # 传递状态
                                'ts': int(time.time() * 1000)
                            }
                            # ⚠️ 关键：推送到事件队列 (Event Queue)
                            loop.call_soon_threadsafe(event_queue.put_nowait, event)
                            logging.info(f"⚡️ [WS推送] GRVT 成交(Delta): {symbol_base} {side} {filled_size} @ {price}")
                    return

                # 3. 处理 Orderbook -> 推送至 tick_queue
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
                        # 推送到行情队列
                        loop.call_soon_threadsafe(tick_queue.put_nowait, tick)

            except Exception as e:
                logging.warning(f"❌ [GRVT Callback Error] {e}")

        # 4. 执行订阅 (这部分逻辑保持原样，但需要放在新的 message_callback 下方)
        for symbol, info in self.contract_map.items():
            instrument_id = info['id']
            # 订阅行情 (L1 Orderbook)
            await self.ws_client.subscribe(
                stream="book.s",
                callback=message_callback,
                params={"instrument": instrument_id}
            )
            # 订阅私有订单流
            await self.ws_client.subscribe(
                stream="order",
                callback=message_callback,
                params={"instrument": instrument_id, "sub_account_id": self.trading_account_id}
            )
            await asyncio.sleep(0.1)  # 避免瞬间请求过多

        # 5. 保持连接活跃
        while True:
            await asyncio.sleep(1)