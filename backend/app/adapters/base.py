from abc import ABC, abstractmethod
from typing import Dict, Optional, Any, TYPE_CHECKING, List
import asyncio
import logging
from dataclasses import dataclass

if TYPE_CHECKING:
    from app.core.backoff import ErrorBackoffController

logger = logging.getLogger("BaseExchange")

@dataclass
class Balance:
    currency: str
    total: float
    free: float

class BaseExchange(ABC):
    """
    交易所抽象基类 (Production Ready)
    """

    def __init__(self, name: str):
        self.name = name
        self.is_connected = False
        self.backoff_controller: Optional['ErrorBackoffController'] = None

    @abstractmethod
    async def initialize(self):
        pass

    # --- 核心交易接口 (Template Method) ---

    async def create_order(self, symbol: str, side: str, amount: float,
                           price: Optional[float] = None, order_type: str = "LIMIT",
                           **kwargs) -> Optional[str]:
        if self.backoff_controller:
            await self.backoff_controller.wait_permission(self.name)
        try:
            return await self._create_order_impl(symbol, side, amount, price, order_type, **kwargs)
        except Exception as e:
            if self.backoff_controller:
                self.backoff_controller.record_error(self.name)
            logger.error(f"❌ [{self.name}] Order Failed: {e}")
            raise e

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        if self.backoff_controller:
            await self.backoff_controller.wait_permission(self.name)
        try:
            return await self._cancel_order_impl(order_id, symbol)
        except Exception as e:
            logger.error(f"❌ [{self.name}] Cancel Failed: {e}")
            return False

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        if self.backoff_controller:
            await self.backoff_controller.wait_permission(self.name)
        try:
            return await self._fetch_orderbook_impl(symbol)
        except Exception as e:
            if self.backoff_controller:
                self.backoff_controller.record_error(self.name)
            raise e

    # --- 数据查询接口 (提供默认空实现以防崩溃) ---

    async def get_balances(self) -> List[Balance]:
        """获取账户余额列表"""
        return []

    async def fetch_positions(self) -> List[Dict]:
        """
        获取持仓列表
        返回格式: [{'symbol': str, 'size': float, 'entry_price': float, ...}]
        """
        return []

    # --- 抽象实现 ---

    @abstractmethod
    async def _create_order_impl(self, symbol: str, side: str, amount: float, price: Optional[float], order_type: str, **kwargs) -> str:
        pass

    @abstractmethod
    async def _cancel_order_impl(self, order_id: str, symbol: str) -> bool:
        pass

    @abstractmethod
    async def _fetch_orderbook_impl(self, symbol: str) -> Dict[str, float]:
        pass

    @abstractmethod
    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        pass

    async def close(self):
        self.is_connected = False