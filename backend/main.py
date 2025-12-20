import asyncio
import logging
import signal
import sys
import os
from typing import List, Dict, Any
import getpass

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from app.config import settings
from app.adapters.base import BaseExchange
from app.core.engine import EventEngine
from app.core.risk_controller import GlobalRiskController  # [新增]
from app.core.backoff import ErrorBackoffController  # [新增]

logging.basicConfig(
    level=settings.common.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 压制噪音
for lib in ["pysdk", "asyncio", "urllib3", "websockets"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

logger = logging.getLogger("Main")


class SystemOrchestrator:
    """系统编排器：管理生命周期和依赖注入"""

    def __init__(self):
        self.adapters: List[BaseExchange] = []
        self.risk_controller: GlobalRiskController = None
        self.backoff_controller: ErrorBackoffController = None
        self.engine: EventEngine = None
        self.running = False

    async def setup(self):
        logger.info(f"🚀 正在启动 DEX 系统 (Nado 独立模式)...")

        # 1. 安全检查
        self._setup_security()

        # 2. 初始化核心组件
        self.backoff_controller = ErrorBackoffController()
        logger.info("✅ 错误避让控制器已就绪")

        # 3. 加载适配器
        self._load_adapters()
        if not self.adapters:
            raise RuntimeError("未加载任何适配器")

        # 4. 初始化风控 (依赖适配器)
        self.risk_controller = GlobalRiskController(
            adapters={ex.name: ex for ex in self.adapters}
        )
        # 这里需要等待风控模块启动
        await self.risk_controller.start()

        # 5. 加载策略 & 引擎
        strategy = self._load_strategy()
        self.engine = EventEngine(exchanges=self.adapters, strategy=strategy)

    def _setup_security(self):
        if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
            try:
                master_input = getpass.getpass("🔐 [Security] Enter Master Key > ")
                if not master_input:
                    raise ValueError("Key cannot be empty")
                os.environ["MASTER_KEY"] = master_input.strip()
            except Exception:
                sys.exit(1)

    def _load_adapters(self):
        # 确定所需适配器
        strategy_type = settings.strategies.active
        required_exchanges = set()

        if strategy_type == "HFT_MM":
            required_exchanges.add(settings.strategies.hft_mm.exchange)
        elif strategy_type == "AI_GRID":
            required_exchanges.add(settings.strategies.ai_grid.exchange)
        else:
            logger.error(f"不支持的策略: {strategy_type}")
            sys.exit(1)

        # 实例化并注入 Backoff
        if "Nado" in required_exchanges:
            try:
                from app.adapters.nado import NadoAdapter
                nado_key = settings.nado_private_key.get_secret_value() if settings.nado_private_key else None
                adapter = NadoAdapter(
                    private_key=nado_key,
                    mode=settings.nado_mode,
                    subaccount_name=settings.nado_subaccount_name,
                    symbols=settings.common.target_symbols
                )
                # [注入] 错误控制器
                adapter.backoff_controller = self.backoff_controller
                self.adapters.append(adapter)
                logger.info("✅ Nado Adapter Loaded (With Backoff)")
            except Exception as e:
                logger.critical(f"Nado Load Failed: {e}")
                sys.exit(1)

        # 这里可以继续添加 GRVT/Lighter 等其他适配器的加载逻辑

    def _load_strategy(self):
        strategy_type = settings.strategies.active
        adapters_map = {ex.name: ex for ex in self.adapters}

        logger.info(f"🧠 初始化策略: {strategy_type}")

        # [关键] 将 RiskController 传递给策略
        # 注意：你需要修改具体策略类 (如 HFTMarketMakingStrategy) 的 __init__ 方法，
        # 让其接收 risk_controller 参数，并传递给 Executor

        if strategy_type == "HFT_MM":
            from app.strategies.hft_market_making import HFTMarketMakingStrategy
            return HFTMarketMakingStrategy(
                adapters=adapters_map,
                risk_controller=self.risk_controller  # 依赖注入
            )
        elif strategy_type == "AI_GRID":
            from app.strategies.ai_grid import AiAdaptiveGridStrategy
            return AiAdaptiveGridStrategy(
                adapters=adapters_map,
                risk_controller=self.risk_controller  # 依赖注入
            )
        else:
            raise ValueError(f"Unknown strategy: {strategy_type}")

    async def run(self):
        self.running = True

        # 初始化连接
        logger.info("🔌 连接交易所...")
        await asyncio.gather(*(ex.initialize() for ex in self.adapters))

        # 启动引擎
        logger.info("📡 系统全速运行中...")
        try:
            await self.engine.start()
        except asyncio.CancelledError:
            logger.info("Main loop cancelled")

    async def shutdown(self):
        logger.info("🛑 正在优雅关闭系统...")
        self.running = False

        # 1. 停止风控监控
        if self.risk_controller:
            await self.risk_controller.stop()

        # 2. 停止引擎 (会断开 WS)
        # engine.stop() 逻辑需要在 engine.py 中完善，或者直接依赖 adapter.close()

        # 3. 关闭适配器连接
        for ex in self.adapters:
            if hasattr(ex, 'close'):
                await ex.close()

        logger.info("👋 Bye!")


async def main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    orchestrator = SystemOrchestrator()

    # 注册信号
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await orchestrator.setup()

        # 在后台运行主逻辑
        run_task = asyncio.create_task(orchestrator.run())

        # 等待停止信号
        await stop_event.wait()

        # 取消任务并执行清理
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass

    except Exception as e:
        logger.critical(f"🔥 系统崩溃: {e}", exc_info=True)
    finally:
        await orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())