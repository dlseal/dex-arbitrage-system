import asyncio
import time
import os
import logging
import random
from decimal import Decimal
from typing import Dict, Optional, Any, List

from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv
from pysdk.grvt_ccxt_logging_selector import logger as sdk_logger

from .base import BaseExchange

logger = logging.getLogger("GRVT_Adapter")


class GrvtAdapter(BaseExchange):
    def __init__(self, api_key: str, private_key: str, trading_account_id: str, symbols: List[str] = None):
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

        self.rest_client: Optional[GrvtCcxt] = None
        self.ws_client: Optional[GrvtCcxtWS] = None
        self.contract_map = {}
        self.last_ws_msg_time = 0.0

    async def initialize(self):
        retry_count = 5
        for attempt in range(retry_count):
            try:
                logger.info(f"⏳ [GRVT] Connecting (Attempt {attempt + 1})...")
                await self._initialize_logic()
                logger.info("✅ [GRVT] Connection Established!")
                return
            except Exception as e:
                logger.warning(f"⚠️ [GRVT] Connection failed: {e}")
                await asyncio.sleep((attempt + 1) * 3)

        raise ConnectionError("❌ [GRVT] Failed to connect after multiple attempts.")

    async def _initialize_logic(self):
        params = {
            'trading_account_id': self.trading_account_id,
            'private_key': self.private_key,
            'api_key': self.api_key
        }
        self.rest_client = GrvtCcxt(env=self.env, parameters=params)

        logger.info(f"⏳ [GRVT] Syncing markets...")
        markets = await self._fetch_markets_async()

        for market in markets:
            base = market.get('base')
            quote = market.get('quote')
            kind = market.get('kind')

            if kind == 'PERPETUAL' and quote == 'USDT':
                if base in self.target_symbols:
                    symbol = f"{base}-{quote}"
                    raw_id = market.get('instrument') or market.get('i')
                    # 兼容不同字段名
                    raw_tick = market.get('tick_size') or market.get('ts') or 0

                    self.contract_map[symbol] = {
                        "id": raw_id,
                        "tick_size": float(raw_tick),
                        "size_mul": float(market.get('contract_size', 1.0))
                    }
                    logger.info(f"   - Loaded {symbol} (ID: {raw_id})")

        ws_params = {
            'api_key': self.api_key,
            'trading_account_id': self.trading_account_id,
            'api_ws_version': 'v1',
            'private_key': self.private_key
        }
        self.ws_client = GrvtCcxtWS(
            env=self.env,
            loop=asyncio.get_running_loop(),
            logger=sdk_logger,
            parameters=ws_params
        )
        await self.ws_client.initialize()
        self.is_connected = True
        self.last_ws_msg_time = time.time()

    async def _fetch_markets_async(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.rest_client.fetch_markets(params={}))

    def _get_contract_info(self, symbol: str):
        if "-" not in symbol: symbol = f"{symbol}-USDT"
        info = self.contract_map.get(symbol)
        if not info:
            logger.error(f"Symbol {symbol} not found in map")
            return None
        return info

    def _get_symbol_from_instrument(self, instrument_id):
        for s, info in self.contract_map.items():
            if info['id'] == instrument_id:
                return s.split('-')[0]
        return "UNKNOWN"

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        info = self._get_contract_info(symbol)
        if not info: return {}
        loop = asyncio.get_running_loop()
        try:
            # ✅ 官方示例确认 limit=10 是正确的 (Supported Depths: 10, 20, 50)
            ob = await loop.run_in_executor(None, lambda: self.rest_client.fetch_order_book(info['id'], limit=10))
            bids = ob.get('bids', [])
            asks = ob.get('asks', [])
            return {
                'exchange': self.name,
                'symbol': symbol.split('-')[0],
                'bid': float(bids[0]['price']) if bids else 0.0,
                'ask': float(asks[0]['price']) if asks else 0.0,
                'ts': int(time.time() * 1000)
            }
        except Exception as e:
            logger.error(f"Fetch OB error: {e}")
            return {}

    async def create_order(self, symbol: str, side: str, amount: float, price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        info = self._get_contract_info(symbol)
        if not info:
            logger.error(f"❌ [GRVT] Contract info not found for {symbol}")
            return None

        # --- 1. 精度与数据类型处理 ---
        try:
            tick_size = Decimal(str(info.get('tick_size', 0)))
            min_size = Decimal(str(info.get('min_size', 0)))

            # 数量精度修正
            d_amount = Decimal(str(amount))
            if min_size > 0:
                d_amount = (d_amount / min_size).to_integral_value(rounding='ROUND_DOWN') * min_size
            else:
                d_amount = d_amount.quantize(Decimal("0.000001"))
            qty = float(d_amount)

            # 价格精度修正
            px = None
            if price is not None:
                d_price = Decimal(str(price))
                if tick_size > 0:
                    d_price = (d_price / tick_size).to_integral_value(rounding='ROUND_HALF_UP') * tick_size
                px = float(d_price)

        except Exception as e:
            logger.error(f"❌ [GRVT] Precision Math Error: {e}")
            return None

        # --- 2. ID 生成与参数准备 ---
        ts_part = int(time.time()) % 1000000
        rand_part = random.randint(0, 999)
        client_order_id = int(f"{ts_part}{rand_part:03d}")

        safe_side = side.lower()

        # 构造 params
        params = {
            'client_order_id': client_order_id,
        }

        if order_type == "LIMIT":
            params.update({
                'post_only': False,
                'order_duration_secs': 2591999,
                'timeInForce': 'GTT'
            })

        try:
            # 目标 Symbol ID
            target_symbol_id = info['id']

            # --- 3. 调用 SDK (修正参数名) ---
            if order_type == "MARKET":
                # ✅ 修正：将 type='market' 改为 order_type='market'
                await asyncio.wait_for(
                    self.ws_client.create_order(
                        symbol=target_symbol_id,
                        order_type='market',  # <--- 修正点
                        side=safe_side,
                        amount=qty,
                        price=None,
                        params=params
                    ), timeout=5.0)
            else:
                # create_limit_order 不需要 order_type 参数
                await asyncio.wait_for(
                    self.ws_client.create_limit_order(
                        symbol=target_symbol_id,
                        side=safe_side,
                        amount=qty,
                        price=px,
                        params=params
                    ), timeout=5.0)

            return str(client_order_id)

        except asyncio.TimeoutError:
            logger.error(f"❌ [GRVT] Order Timeout (5s)")
            return None
        except Exception as e:
            logger.error(f"❌ [GRVT] Create Order Error: {e}")
            return None

    async def cancel_order(self, order_id: str):
        try:
            # 只有纯数字才被视为 client_order_id
            if str(order_id).isdigit():
                await self.ws_client.cancel_order(client_order_id=int(order_id))
            else:
                await self.ws_client.cancel_order(order_id=order_id)
        except Exception:
            pass

    async def close(self):
        logger.info("🛑 [GRVT] Closing resources...")
        self.is_connected = False
        self.ws_client = None

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        logger.info(f"📡 [GRVT] Starting Stream Listener...")
        self.last_ws_msg_time = time.time()

        async def message_callback(message: Dict[str, Any]):
            self.last_ws_msg_time = time.time()
            try:
                # 兼容 feed 包裹格式
                feed_data = message.get("feed", {})
                if not feed_data and "instrument" in message: feed_data = message

                channel = message.get("params", {}).get("channel") or message.get("stream")

                # --- A. 订单更新 ---
                if channel and "order" in str(channel) and "book" not in str(channel):
                    state = feed_data.get("state", {})
                    status = state.get("status", "").upper()

                    if status in ["FILLED", "PARTIALLY_FILLED"]:
                        legs = feed_data.get("legs", [])
                        filled_size = sum(float(l.get("size", 0)) for l in legs)

                        if filled_size > 0 and legs:
                            leg = legs[0]
                            symbol_base = self._get_symbol_from_instrument(leg.get("instrument"))
                            is_buy = leg.get("is_buying_asset", False)
                            side = "BUY" if is_buy else "SELL"
                            price = float(leg.get("limit_price", 0))

                            # 提取 ID
                            client_oid = message.get('client_order_id') or feed_data.get(
                                'client_order_id') or state.get('client_order_id')
                            system_oid = message.get('order_id') or feed_data.get('order_id')
                            final_order_id = str(client_oid) if client_oid else str(system_oid)

                            event = {
                                'type': 'trade',
                                'exchange': self.name,
                                'symbol': symbol_base,
                                'side': side,
                                'price': price,
                                'size': filled_size,
                                'order_id': final_order_id,
                                'ts': int(time.time() * 1000)
                            }
                            event_queue.put_nowait(event)
                            logger.info(f"⚡️ [GRVT Fill] {symbol_base} {side} {filled_size} (ID:{final_order_id})")
                    return

                # --- B. 订单簿更新 ---
                if channel and "book" in str(channel):
                    instrument = feed_data.get("instrument")
                    symbol_base = self._get_symbol_from_instrument(instrument)
                    if symbol_base == "UNKNOWN": return

                    bids = feed_data.get("bids", [])
                    asks = feed_data.get("asks", [])

                    if bids and asks:
                        tick = {
                            'type': 'tick',
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'bid': float(bids[0]['price']),
                            'ask': float(asks[0]['price']),
                            'ts': int(time.time() * 1000)
                        }
                        tick_queue.put_nowait(tick)

            except Exception as e:
                pass  # 忽略解析错误，避免刷屏

        try:
            for symbol, info in self.contract_map.items():
                inst_id = info['id']
                logger.info(f"📤 [GRVT] Subscribing to {symbol} (ID: {inst_id})")

                # ✅ 修正：移除 depth 参数！官方 WS 示例中不带 params 订阅 book.s
                await self.ws_client.subscribe(stream="book.s", callback=message_callback,
                                               params={"instrument": inst_id})

                await self.ws_client.subscribe(stream="order", callback=message_callback,
                                               params={"instrument": inst_id,
                                                       "sub_account_id": self.trading_account_id})
                await asyncio.sleep(0.1)

            # Watchdog
            while self.is_connected:
                await asyncio.sleep(10)
                if time.time() - self.last_ws_msg_time > 60.0:
                    logger.error("❌ [GRVT] Watchdog: No data for 60s.")
                    raise ConnectionError("GRVT WS Timeout")

        except asyncio.CancelledError:
            raise
        finally:
            await self.close()