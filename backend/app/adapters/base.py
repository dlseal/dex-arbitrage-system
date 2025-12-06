from abc import ABC, abstractmethod
from typing import Dict, Optional
import asyncio

class BaseExchange(ABC):
    """
    交易所抽象基类
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
        """获取标准化订单簿"""
        pass

    @abstractmethod
    async def create_order(self, 
                           symbol: str, 
                           side: str, 
                           amount: float, 
                           price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        """创建订单"""
        pass

    @abstractmethod
    async def listen_websocket(self, queue: asyncio.Queue):
        """维持 WebSocket 长连接"""
        pass