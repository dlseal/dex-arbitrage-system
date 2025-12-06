# backend/app/adapters/base.py
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import asyncio


class BaseExchange(ABC):
    """
    交易所抽象基类 (Adapter Pattern)
    确保 Lighter, EdgeX, GRVT 实现统一的接口规范 [cite: 57]
    """

    def __init__(self, name: str, api_key: str, private_key: str):
        self.name = name
        self.api_key = api_key
        self.private_key = private_key
        self.is_connected = False

    @abstractmethod
    async def initialize(self):
        """异步初始化：建立连接、加载账户索引、获取市场元数据"""
        pass

    @abstractmethod
    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """
        获取标准化订单簿快照
        返回格式需统一: {'bid': 100.0, 'ask': 100.1, 'ts': 1700000000}
        """
        pass

    @abstractmethod
    async def create_order(self,
                           symbol: str,
                           side: str,
                           amount: float,
                           price: Optional[float] = None,
                           order_type: str = "LIMIT") -> str:
        """
        创建订单
        :param side: 'buy' or 'sell'
        :param order_type: 'LIMIT' (Maker) or 'MARKET' (Taker)
        :return: order_id
        """
        pass

    @abstractmethod
    async def get_funding_rate(self, symbol: str) -> float:
        """获取当前资金费率，需标准化为每小时费率以进行比较 [cite: 104]"""
        pass

    @abstractmethod
    async def listen_websocket(self, queue: asyncio.Queue):
        """
        维持 WebSocket 长连接，并将原始数据转换为标准 Tick 放入队列 [cite: 50]
        """
        pass
