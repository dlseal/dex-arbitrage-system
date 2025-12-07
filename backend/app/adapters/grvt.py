import asyncio
import time
import os
import logging
from decimal import Decimal
from typing import Dict, Optional, Any, List

# 引入 GRVT SDK
from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv
from pysdk.grvt_ccxt_logging_selector import logger as sdk_logger

from .base import BaseExchange

logger = logging.getLogger("GRVT_Adapter")


class GrvtAdapter(BaseExchange):
    """
    GRVT 交易所适配器 (实盘修复版)
    核心修复：精度处理、ClientOrderID匹配、心跳看门狗
    """

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
        self.last_ws_msg_time = 0.0  # 看门狗计时器

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
        # 1. Init REST
        params = {
            'trading_account_id': self.trading_account_id,
            'private_key': self.private_key,
            'api_key': self.api_key
        }
        self.rest_client = GrvtCcxt(env=self.env, parameters=params)

        # 2. Fetch Markets
        logger.info(f"⏳ [GRVT] Syncing markets...")
        markets = await self._fetch_markets_async()

        loaded_count = 0
        for market in markets:
            base = market.get('base')
            quote = market.get('quote')
            kind = market.get('kind')

            if kind == 'PERPETUAL' and quote == 'USDT':
                if base in self.target_symbols:
                    symbol = f"{base}-{quote}"
                    # 兼容不同字段名
                    raw_id = market.get('instrument') or market.get('i')
                    raw_tick = market.get('tick_size') or market.get('ts') or 0
                    raw_min = market.get('min_size') or market.get('ms') or 0

                    # 补充：获取价格和数量的精度倍数
                    # GRVT通常直接传真实价格，但为了保险，获取 multiplier
                    size_mul = 1.0  # GRVT SDK 内部处理了精度，这里主要用于 create_order 预处理
                    price_mul = 1.0

                    self.contract_map[symbol] = {
                        "id": raw_id,
                        "tick_size": float(raw_tick),
                        "min_size": float(raw_min),
                        "size_mul": float(market.get('contract_size', 1.0))  # 假设
                    }
                    loaded_count += 1
                    logger.info(f"   - Loaded {symbol} (ID: {raw_id})")

        if loaded_count == 0:
            logger.warning(f"⚠️ [GRVT] No target markets found!")

        # 3. Init WS
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
            logger.error(f"Symbol {symbol} not found in map {list(self.contract_map.keys())}")
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
            ob = await loop.run_in_executor(None, lambda: self.rest_client.fetch_order_book(info['id'], limit=5))
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
        if not info: return None

        # ✅ 修复点1：精度处理
        # GRVT SDK 接收 float，但为了避免 0.0099999 < 0.01 导致后端拒绝，
        # 我们在这里做一次标准的 round 处理，保留合适的小数位 (例如 6位)
        amount_safe = float(Decimal(str(amount)).quantize(Decimal("0.000001")))
        price_safe = float(Decimal(str(price)).quantize(Decimal("0.000001"))) if price else None

        # 生成 Client Order ID (作为整数传递)
        client_order_id = int(time.time() * 1000000) % 2147483647

        is_ask = True if side.lower() == 'sell' else False

        try:
            # 使用 wait_for 防止死锁
            if order_type == "MARKET":
                res = await asyncio.wait_for(
                    self.ws_client.create_market_order(
                        instrument=info['id'],
                        size=amount_safe,
                        side='sell' if is_ask else 'buy',
                        client_order_id=client_order_id
                    ),
                    timeout=5.0
                )
            else:
                res = await asyncio.wait_for(
                    self.ws_client.create_limit_order(
                        instrument=info['id'],
                        size=amount_safe,
                        price=price_safe,
                        side='sell' if is_ask else 'buy',
                        time_in_force="GTT",
                        client_order_id=client_order_id,
                        post_only=False
                    ),
                    timeout=5.0
                )

            # 返回 client_order_id 以便策略追踪
            return str(client_order_id)

        except asyncio.TimeoutError:
            logger.error(f"❌ [GRVT] Order Timeout (5s)")
            return None
        except Exception as e:
            logger.error(f"❌ [GRVT] Create Order Error: {e}")
            return None

    async def cancel_order(self, order_id: str):
        # 简化版：尝试撤单
        try:
            if str(order_id).isdigit():
                # 假设是 client_order_id
                await self.ws_client.cancel_order(client_order_id=int(order_id))
            else:
                await self.ws_client.cancel_order(order_id=order_id)
        except Exception:
            pass  # 忽略撤单失败（可能是已成交）

    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        logger.info(f"📡 [GRVT] Starting Stream Listener...")
        loop = asyncio.get_running_loop()

        async def message_callback(message: Dict[str, Any]):
            self.last_ws_msg_time = time.time()

            try:
                feed_data = message.get("feed", {})
                if not feed_data and "instrument" in message: feed_data = message

                channel = message.get("params", {}).get("channel") or message.get("stream")

                # --- A. 处理订单回报 ---
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

                            # ✅ 修复点2：优先匹配 client_order_id
                            # 策略层使用的是 client_order_id，必须提取并传回
                            client_oid = message.get('client_order_id') or \
                                         feed_data.get('client_order_id') or \
                                         state.get('client_order_id')

                            system_oid = message.get('order_id') or feed_data.get('order_id')

                            # 优先使用 Client ID，如果没有则回退到 System ID
                            final_order_id = str(client_oid) if client_oid else str(system_oid)

                            event = {
                                'type': 'trade',
                                'exchange': self.name,
                                'symbol': symbol_base,
                                'side': side,
                                'price': price,
                                'size': filled_size,
                                'order_id': final_order_id,  # 关键修复
                                'ts': int(time.time() * 1000)
                            }
                            event_queue.put_nowait(event)
                            logger.info(f"⚡️ [GRVT Fill] {symbol_base} {side} {filled_size} (ID:{final_order_id})")
                    return

                # --- B. 处理行情 ---
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
                        # RingQueue会自动丢弃旧数据，直接put
                        try:
                            tick_queue.put_nowait(tick)
                        except:
                            pass

            except Exception as e:
                logger.error(f"GRVT Callback Parse Error: {e}")

        # 订阅
        for symbol, info in self.contract_map.items():
            inst_id = info['id']
            await self.ws_client.subscribe(stream="book.s", callback=message_callback, params={"instrument": inst_id})
            await self.ws_client.subscribe(stream="order", callback=message_callback,
                                           params={"instrument": inst_id, "sub_account_id": self.trading_account_id})
            await asyncio.sleep(0.1)

        # ✅ 修复点3：连接看门狗 (Watchdog)
        while True:
            await asyncio.sleep(5)
            if time.time() - self.last_ws_msg_time > 30.0:
                logger.error("❌ [GRVT] Watchdog Triggered: No data for 30s. Restarting...")
                # 这里抛出异常，让上层 Engine 捕获并重启 adapter 或整个程序
                raise ConnectionError("GRVT WebSocket Timeout")