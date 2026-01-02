import asyncio
import time
import os
import logging
import random
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Dict, Optional, Any, List
from app.config import settings

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

        env_str = settings.grvt_environment.lower()
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
        self._order_seq = 0

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
        # 初始化同步 REST 客户端
        self.rest_client = GrvtCcxt(env=self.env, parameters=params)

        logger.info(f"⏳ [GRVT] Syncing markets...")
        markets = await self._fetch_markets_async()

        self.contract_map = {}
        for market in markets:
            base = market.get('base')
            quote = market.get('quote')
            kind = market.get('kind')

            # 筛选我们关注的永续合约
            if kind == 'PERPETUAL' and quote == 'USDT':
                if base in self.target_symbols:
                    symbol = f"{base}-{quote}"
                    raw_id = market.get('instrument') or market.get('i')
                    raw_tick = market.get('tick_size') or market.get('ts') or "0.01"
                    min_size = market.get('min_size') or "0.0001"
                    contract_size = market.get('contract_size') or "1.0"

                    self.contract_map[symbol] = {
                        "id": raw_id,
                        "tick_size": Decimal(str(raw_tick)),
                        "min_size": Decimal(str(min_size)),
                        "size_mul": Decimal(str(contract_size))
                    }
                    logger.info(f"   - Loaded {symbol} (ID: {raw_id}) | Tick: {raw_tick} | MinSize: {min_size}")

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
        # 统一格式化为 BTC-USDT 查找
        if "-" not in symbol and "_" not in symbol:
            symbol = f"{symbol}-USDT"

        # 尝试直接匹配
        if symbol in self.contract_map:
            return self.contract_map[symbol]

        # 尝试通过 raw_id 反向查找 (如果不常用可以忽略，但为了稳健)
        for k, v in self.contract_map.items():
            if v['id'] == symbol:
                return v

        # 再次尝试转换格式
        norm_symbol = symbol.replace('_', '-')
        return self.contract_map.get(norm_symbol)

    def _get_symbol_from_instrument(self, instrument_id):
        for s, info in self.contract_map.items():
            if info['id'] == instrument_id:
                return s.split('-')[0]
        return "UNKNOWN"

    # --- 核心交易接口 ---

    async def fetch_positions(self, symbols: List[str] = None) -> List[Dict]:
        """
        [PROD FIX] 实现 fetch_positions 以支持仓位同步
        """
        try:
            target_instruments = []
            if symbols:
                for s in symbols:
                    info = self._get_contract_info(s)
                    if info:
                        target_instruments.append(info['id'])

            # 如果没找到映射，可能就是原始 instrument_id，直接传递
            if symbols and not target_instruments:
                target_instruments = symbols

            loop = asyncio.get_running_loop()

            # 调用底层同步 fetch_positions
            # 根据官方用例: fetch_positions(symbols=['BTC_USDT_Perp'])
            kwargs = {}
            if target_instruments:
                kwargs['symbols'] = target_instruments

            positions = await loop.run_in_executor(
                None,
                lambda: self.rest_client.fetch_positions(**kwargs)
            )
            return positions
        except Exception as e:
            logger.error(f"❌ [GRVT] Fetch Positions Failed: {e}")
            # 这里抛出异常，让上层策略知道同步失败，而不是返回空列表误导策略
            raise e

    async def cancel_all_orders(self, symbol: str) -> bool:
        """
        [PROD FIX] 实现 cancel_all_orders 以支持 Bailout
        """
        try:
            info = self._get_contract_info(symbol)
            if not info:
                logger.warning(f"⚠️ Cancel All: Unknown symbol {symbol}")
                return False

            inst_id = info['id']
            loop = asyncio.get_running_loop()

            logger.info(f"🚨 [GRVT] Cancelling ALL orders for {inst_id}...")
            await loop.run_in_executor(
                None,
                lambda: self.rest_client.cancel_all_orders(symbol=inst_id)
            )
            return True
        except Exception as e:
            logger.error(f"❌ [GRVT] Cancel All Failed: {e}")
            return False

    async def _fetch_orderbook_impl(self, symbol: str) -> Dict[str, float]:
        info = self._get_contract_info(symbol)
        if not info: return {}
        loop = asyncio.get_running_loop()
        try:
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
            raise e

    async def _create_order_impl(self, symbol: str, side: str, amount: float, price: Optional[float],
                                 order_type: str, **kwargs) -> str:
        info = self._get_contract_info(symbol)
        if not info:
            raise ValueError(f"Contract info not found for {symbol}")

        try:
            tick_size = info['tick_size']
            min_size = info['min_size']

            d_amount = Decimal(str(amount))
            # 向下取整到最小交易单位
            if min_size > 0:
                d_amount = (d_amount / min_size).to_integral_value(rounding=ROUND_DOWN) * min_size

            d_price = Decimal("0")
            if price is not None:
                d_price = Decimal(str(price))
                if tick_size > 0:
                    d_price = (d_price / tick_size).to_integral_value(rounding=ROUND_HALF_UP) * tick_size

            qty_float = float(d_amount)
            px_float = float(d_price) if price is not None else None

            if qty_float <= 0:
                raise ValueError(f"Order size too small after quantization: {qty_float}")

        except Exception as e:
            logger.error(f"❌ [GRVT] Math Error: {e}")
            raise e

        self._order_seq = (self._order_seq + 1) % 10000
        ts_part = int(time.time() * 1000) & 0xFFFFFF
        client_order_id = (ts_part << 14) | self._order_seq  # 增加位宽防止碰撞
        safe_side = side.lower()

        req_params = {
            'client_order_id': client_order_id,
        }
        req_params.update(kwargs)

        if order_type == "LIMIT":
            # 默认 Post Only
            final_post_only = req_params.get('post_only', True)
            req_params.update({
                'post_only': final_post_only,
                'timeInForce': 'GTT',
                'order_duration_secs': 3600,  # 1小时自动过期，防止僵尸单
            })

        try:
            target_symbol_id = info['id']

            if order_type == "MARKET":
                # Market 单不能有 post_only
                if 'post_only' in req_params: del req_params['post_only']

                await asyncio.wait_for(
                    self.ws_client.create_order(
                        symbol=target_symbol_id,
                        order_type='market',
                        side=safe_side,
                        amount=qty_float,
                        price=None,
                        params=req_params
                    ), timeout=5.0)  # 稍微放宽市价单超时
            else:
                await asyncio.wait_for(
                    self.ws_client.create_limit_order(
                        symbol=target_symbol_id,
                        side=safe_side,
                        amount=qty_float,
                        price=px_float,
                        params=req_params
                    ), timeout=3.0)

            return str(client_order_id)

        except asyncio.TimeoutError:
            logger.error(f"❌ [GRVT] Order Timeout (WS) - ID: {client_order_id}")
            return None
        except Exception as e:
            err_msg = str(e).lower()
            if "post-only" in err_msg or "maker" in err_msg:
                # 正常的业务逻辑错误，不用 error 级别
                logger.warning(f"⚠️ [GRVT] Post-Only Reject: {e}")
            else:
                logger.error(f"❌ [GRVT] Create Order Error: {e}")
            raise e

    async def _cancel_order_impl(self, order_id: str, symbol: str) -> bool:
        try:
            oid = int(order_id) if str(order_id).isdigit() else order_id
            inst_id = None
            if symbol:
                info = self._get_contract_info(symbol)
                if info:
                    inst_id = info['id']

            # GRVT WS cancel usually needs client_order_id inside params
            params = {'client_order_id': oid}
            await self.ws_client.cancel_order(id=None, symbol=inst_id, params=params)
            return True

        except Exception as e:
            if "not found" not in str(e).lower():
                logger.warning(f"Cancel failed for {order_id}: {e}")
            return False

    async def get_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        try:
            oid = int(order_id) if str(order_id).isdigit() else order_id
            info = self._get_contract_info(symbol)
            if not info: return {}

            loop = asyncio.get_running_loop()
            order = await loop.run_in_executor(
                None,
                lambda: self.rest_client.fetch_order(id=oid, symbol=info['id'])
            )

            return {
                'id': str(order.get('id', '')),
                'client_order_id': str(order.get('clientOrderId', '')),
                'filled': float(order.get('filled', 0.0)),
                'status': order.get('status', 'unknown'),
                'price': float(order.get('price', 0.0) or 0.0),
                'avg_price': float(order.get('average', 0.0) or 0.0),
            }
        except Exception as e:
            return {}

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
                feed_data = message.get("feed", {})
                if not feed_data and "instrument" in message: feed_data = message

                channel = message.get("params", {}).get("channel") or message.get("stream") or ""

                if "order" in str(channel) and "book" not in str(channel):
                    state = feed_data.get("state", {})
                    status = state.get("status", "").upper()

                    # 获取ID
                    client_oid = message.get('client_order_id') or feed_data.get('client_order_id') or state.get(
                        'client_order_id')
                    system_oid = message.get('order_id') or feed_data.get('order_id')
                    final_order_id = str(client_oid) if client_oid else str(system_oid)

                    instrument = feed_data.get("instrument") or state.get("instrument")
                    symbol_base = self._get_symbol_from_instrument(instrument)

                    # [MODIFIED] 推送所有关键状态，包括 REJECTED/CANCELED
                    if status in ["FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED"]:
                        event = {
                            'type': 'order',  # 使用通用的 order 事件类型
                            'exchange': self.name,
                            'symbol': symbol_base,
                            'order_id': final_order_id,
                            'status': status,
                            'ts': int(time.time() * 1000)
                        }
                        # 额外补充成交信息
                        if status in ["FILLED", "PARTIALLY_FILLED"]:
                            legs = feed_data.get("legs", [])
                            filled_size = sum(float(l.get("size", 0)) for l in legs)
                            if filled_size > 0 and legs:
                                leg = legs[0]
                                is_buy = leg.get("is_buying_asset", False)
                                event['side'] = "BUY" if is_buy else "SELL"
                                event['price'] = float(leg.get("limit_price", 0))
                                event['size'] = filled_size
                                event['type'] = 'trade'  # 保持对 trade 处理的兼容

                        event_queue.put_nowait(event)
                    return

                if "book" in str(channel):
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
                            'bid_volume': float(bids[0]['size']),
                            'ask': float(asks[0]['price']),
                            'ask_volume': float(asks[0]['size']),
                            'ts': int(time.time() * 1000)
                        }
                        tick_queue.put_nowait(tick)

            except Exception as e:
                logger.error(f"WS Parse Error: {e}")

        try:
            for symbol, info in self.contract_map.items():
                inst_id = info['id']
                logger.info(f"📤 [GRVT] Subscribing to {symbol} (ID: {inst_id})")

                await self.ws_client.subscribe(stream="book.s", callback=message_callback,
                                               params={"instrument": inst_id})

                await self.ws_client.subscribe(stream="order", callback=message_callback,
                                               params={"instrument": inst_id,
                                                       "sub_account_id": self.trading_account_id})
                await asyncio.sleep(0.1)

            while self.is_connected:
                await asyncio.sleep(5)
                # Watchdog
                if time.time() - self.last_ws_msg_time > 60.0:
                    logger.error("❌ [GRVT] Watchdog: No data for 60s.")
                    self.is_connected = False
                    raise ConnectionError("GRVT WS Timeout")

        except asyncio.CancelledError:
            raise
        finally:
            await self.close()