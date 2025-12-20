from abc import ABC, abstractmethod
from typing import Dict, Optional, Any, TYPE_CHECKING, List
import asyncio
import logging

# 使用 TYPE_CHECKING 避免循环导入
if TYPE_CHECKING:
    from app.core.backoff import ErrorBackoffController

logger = logging.getLogger("BaseExchange")


class BaseExchange(ABC):
    """
    交易所抽象基类 (生产级增强版)

    核心特性：
    1. 集成 ErrorBackoffController：实现API请求的自动熔断与冷却。
    2. Template Method 模式：强制统一的风控检查流程，子类仅需实现 _impl 方法。
    """

    def __init__(self, name: str):
        self.name = name
        self.is_connected = False
        # 依赖注入：错误避让控制器 (由 Main 或 Orchestrator 注入)
        self.backoff_controller: Optional['ErrorBackoffController'] = None

    @abstractmethod
    async def initialize(self):
        """异步初始化：建立连接、加载账户索引等"""
        pass

    # ------------------------------------------------------------------------
    # 核心交易接口 (公共入口 - 包含熔断保护)
    # 子类请勿覆盖这些方法，而是实现对应的 _impl 方法
    # ------------------------------------------------------------------------

    async def create_order(self,
                           symbol: str,
                           side: str,
                           amount: float,
                           price: Optional[float] = None,
                           order_type: str = "LIMIT",
                           **kwargs) -> Optional[str]:
        """
        [Template Method] 创建订单
        执行流程: 熔断检查 -> 执行下单(_create_order_impl) -> 异常处理/记录
        """
        # 1. 熔断/冷却检查
        if self.backoff_controller:
            await self.backoff_controller.wait_permission(self.name)

        try:
            # 2. 调用子类具体实现
            order_id = await self._create_order_impl(symbol, side, amount, price, order_type, **kwargs)
            return order_id
        except Exception as e:
            # 3. 记录错误并触发熔断计数
            if self.backoff_controller:
                self.backoff_controller.record_error(self.name)
            logger.error(f"❌ [{self.name}] 下单失败: {e} (Symbol: {symbol}, Side: {side})")
            raise e

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """
        [Template Method] 撤销订单
        """
        if self.backoff_controller:
            await self.backoff_controller.wait_permission(self.name)

        try:
            return await self._cancel_order_impl(order_id, symbol)
        except Exception as e:
            if self.backoff_controller:
                self.backoff_controller.record_error(self.name)
            logger.error(f"❌ [{self.name}] 撤单失败: {e} (ID: {order_id})")
            raise e

    async def fetch_orderbook(self, symbol: str) -> Dict[str, float]:
        """
        [Template Method] 获取订单簿
        通常 REST API 获取订单簿也受速率限制，因此同样受控。
        """
        if self.backoff_controller:
            await self.backoff_controller.wait_permission(self.name)

        try:
            return await self._fetch_orderbook_impl(symbol)
        except Exception as e:
            # 读操作失败通常不增加严重的熔断计数，或者增加权重较低
            # 这里为了简化，统一处理
            if self.backoff_controller:
                self.backoff_controller.record_error(self.name)
            raise e

    # ------------------------------------------------------------------------
    # 抽象实现接口 (子类必须实现这些方法)
    # ------------------------------------------------------------------------

    @abstractmethod
    async def _create_order_impl(self,
                                 symbol: str,
                                 side: str,
                                 amount: float,
                                 price: Optional[float],
                                 order_type: str,
                                 **kwargs) -> str:
        """【子类实现】具体的下单 API 调用逻辑"""
        pass

    @abstractmethod
    async def _cancel_order_impl(self, order_id: str, symbol: str) -> bool:
        """【子类实现】具体的撤单 API 调用逻辑"""
        pass

    @abstractmethod
    async def _fetch_orderbook_impl(self, symbol: str) -> Dict[str, float]:
        """【子类实现】具体的获取订单簿 API 调用逻辑"""
        pass

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        查询订单详情 (读操作通常频率较低，暂不强制 Template Method，可视需求增加)
        必须返回格式: {'id': str, 'filled': float, 'status': str, ...}
        """
        pass

    @abstractmethod
    async def listen_websocket(self, tick_queue: asyncio.Queue, event_queue: asyncio.Queue):
        """维持 WebSocket 长连接"""
        pass

    # ------------------------------------------------------------------------
    # 通用/默认方法
    # ------------------------------------------------------------------------

    async def cancel_all_orders(self, symbol: str):
        """默认实现：需要子类具体支持或循环调用 cancel_order"""
        logger.warning(f"[{self.name}] cancel_all_orders 未实现")
        pass

    async def get_balance(self, currency: str) -> Dict[str, float]:
        return {'total': 0.0, 'free': 0.0}

    async def get_position(self, symbol: str) -> Dict[str, float]:
        return {'amount': 0.0, 'entry_price': 0.0}

    async def close(self):
        """资源释放/关闭连接"""
        self.is_connected = False