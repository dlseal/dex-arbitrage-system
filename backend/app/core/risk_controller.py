import asyncio
import logging
from decimal import Decimal
from typing import Dict, Set, Optional, Any

logger = logging.getLogger("RiskController")


class RiskConfig:
    """风险控制配置"""

    def __init__(self):
        # 仓位限制
        self.max_single_position_value = 5000.0  # 单币种最大持仓价值 (USDT)
        self.max_total_position_value = 20000.0  # 全账户最大持仓价值

        # 余额管理
        self.min_balance_threshold = 100.0  # 余额报警阈值
        self.critical_balance_threshold = 20.0  # 余额熔断阈值

        # 熔断机制
        self.max_daily_loss = 1000.0  # 每日最大亏损
        self.max_consecutive_failures = 5  # 最大连续失败次数

        # 监控间隔
        self.check_interval = 60  # 秒


class GlobalRiskController:
    """
    全局风险控制器
    职责：
    1. 资金与持仓水位的实时监控
    2. 交易前的风控检查 (Pre-trade check)
    3. 异常状态下的系统熔断
    """

    def __init__(self, adapters: Dict[str, Any], config: RiskConfig = None):
        self.adapters = adapters
        self.config = config or RiskConfig()

        self._running = False
        self._paused = False
        self._pause_reason = ""
        self._consecutive_failures = 0
        self._monitor_task: Optional[asyncio.Task] = None

        # 状态缓存
        self._total_position_value = Decimal("0")
        self._balances: Dict[str, float] = {}

    async def start(self):
        """启动后台监控"""
        if self._running:
            return
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("🛡️ 全局风险控制器已启动")

    async def stop(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def check_trade_risk(self, symbol: str, quantity: float, price: float) -> bool:
        """
        交易前风控检查 (Pre-trade Check)
        """
        if self._paused:
            logger.warning(f"⛔ 交易被拒绝: 系统已暂停 ({self._pause_reason})")
            return False

        trade_value = Decimal(str(quantity)) * Decimal(str(price))

        # 1. 检查总持仓限制
        if self._total_position_value + trade_value > self.config.max_total_position_value:
            logger.warning(f"⛔ 交易被拒绝: 超过总持仓限制 ({self._total_position_value} + {trade_value})")
            return False

        return True

    def record_failure(self):
        """记录执行失败，触发熔断检查"""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.config.max_consecutive_failures:
            self.trigger_circuit_breaker(f"连续失败 {self._consecutive_failures} 次")

    def record_success(self):
        """重置失败计数"""
        if self._consecutive_failures > 0:
            self._consecutive_failures = 0

    def trigger_circuit_breaker(self, reason: str):
        """触发熔断"""
        if not self._paused:
            self._paused = True
            self._pause_reason = reason
            logger.critical(f"🚨 触发系统熔断! 原因: {reason}")
            # 这里可以添加发送报警通知的逻辑 (Telegram/Slack)

    def resume_system(self):
        """恢复系统"""
        self._paused = False
        self._pause_reason = ""
        self._consecutive_failures = 0
        logger.info("✅ 系统已解除熔断，恢复运行")

    async def _monitor_loop(self):
        """周期性检查账户健康状态"""
        while self._running:
            try:
                await self._check_balances_and_positions()
            except Exception as e:
                logger.error(f"风险监控循环异常: {e}")
            await asyncio.sleep(self.config.check_interval)

    async def _check_balances_and_positions(self):
        """查询所有适配器的余额和持仓"""
        total_value = Decimal("0")

        for name, adapter in self.adapters.items():
            if not hasattr(adapter, 'get_balances') or not hasattr(adapter, 'get_positions'):
                continue

            try:
                # 1. 检查余额
                balances = await adapter.get_balances()
                # 假设 adapter 返回标准化的余额结构，这里简化处理查找 USDT/USDC
                usdc_balance = next((b.total for b in balances if b.currency in ['USDC', 'USDT']), 0)

                if usdc_balance < self.config.critical_balance_threshold:
                    self.trigger_circuit_breaker(f"{name} 余额 ({usdc_balance}) 低于严重阈值")

                # 2. 检查持仓 (简单估值)
                positions = await adapter.get_positions()
                for pos in positions:
                    # 假设 PositionData 有 size 和 mark_price
                    p_value = abs(Decimal(str(pos.size)) * Decimal(str(pos.mark_price or 0)))
                    total_value += p_value

                    if p_value > self.config.max_single_position_value:
                        logger.warning(f"⚠️ {name} {pos.symbol} 持仓过重: {p_value}")

            except Exception as e:
                logger.error(f"检查 {name} 状态失败: {e}")

        self._total_position_value = total_value