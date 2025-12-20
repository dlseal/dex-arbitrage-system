from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import asyncio


class BaseExchange(ABC):
    """
    交易所抽象基类 (增强版)
    """

    def __init__(self, name: str):
        self.name = name
        self.is_connected = False

    @abstractmethod
    async def initialize(self):
        """异步初始化：建立连接、加载账户索引等"""
        pass

    @abstractmethod
    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """获取标准化订单簿 {'bid': ..., 'ask': ...}"""
        pass

    @abstractmethod
    async def create_order(self,
                           symbol: str,
                           side: str,
                           amount: float,
                           price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        """创建订单，返回 order_id (str)"""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """撤销订单"""
        pass

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        查询订单详情
        必须返回格式: {'id': str, 'filled': float, 'status': str, ...}
        """
        pass

    @abstractmethod
    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        """维持 WebSocket 长连接"""
        pass

    async def cancel_all_orders(self, symbol: str):
        pass

    async def get_balance(self, currency: str) -> Dict[str, float]:
        return {'total': 0.0, 'free': 0.0}

    async def get_position(self, symbol: str) -> Dict[str, float]:
        return {'amount': 0.0, 'entry_price': 0.0}