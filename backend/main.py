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
from app.core.risk_controller import GlobalRiskController
from app.core.backoff import ErrorBackoffController

logging.basicConfig(
    level=settings.common.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 压制第三方库噪音
for lib in ["pysdk", "asyncio", "urllib3", "websockets", "web3"]:
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
        logger.info(f"🚀 正在启动 DEX 系统 (Active Strategy: {settings.strategies.active})...")

        # 1. 安全检查
        self._setup_security()

        # 2. 初始化核心组件
        self.backoff_controller = ErrorBackoffController()
        logger.info("✅ 错误避让控制器已就绪")

        # 3. 加载适配器
        self._load_adapters()
        if not self.adapters:
            logger.error("❌ 未加载任何适配器，请检查配置文件的 target_symbols 或 exchange 设置")
            raise RuntimeError("No adapters loaded")

        # 4. 初始化风控 (依赖适配器)
        self.risk_controller = GlobalRiskController(
            adapters={ex.name: ex for ex in self.adapters}
        )
        await self.risk_controller.start()

        # 5. 加载策略 & 引擎
        strategy = self._load_strategy()
        if not strategy:
            raise RuntimeError("Strategy init failed")

        self.engine = EventEngine(exchanges=self.adapters, strategy=strategy)

    def _setup_security(self):
        # 仅当使用 Nado 且 key 加密时才询问
        if "Nado" in self._get_required_exchanges():
            if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
                try:
                    master_input = getpass.getpass("🔐 [Security] Enter Master Key > ")
                    if not master_input:
                        raise ValueError("Key cannot be empty")
                    os.environ["MASTER_KEY"] = master_input.strip()
                except Exception:
                    sys.exit(1)

    def _get_required_exchanges(self):
        strategy_type = settings.strategies.active
        required_exchanges = set()
        if strategy_type == "HFT_MM":
            required_exchanges.add(settings.strategies.hft_mm.exchange)
        elif strategy_type == "GL_FARM":  # 新增
            required_exchanges.add("GRVT")
            required_exchanges.add("Lighter")
        elif strategy_type == "AI_GRID":
            required_exchanges.add(settings.strategies.ai_grid.exchange)
        elif strategy_type == "SPREAD_ARB":
            required_exchanges.add(settings.strategies.spread_arb.exchange_a)
            required_exchanges.add(settings.strategies.spread_arb.exchange_b)
        return required_exchanges

    def _load_adapters(self):
        required_exchanges = self._get_required_exchanges()
        logger.info(f"🔌 需要加载的交易所: {required_exchanges}")

        # --- 1. Load Nado ---
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
                adapter.backoff_controller = self.backoff_controller
                self.adapters.append(adapter)
                logger.info("✅ Nado Adapter Loaded")
            except Exception as e:
                logger.critical(f"Nado Load Failed: {e}")
                sys.exit(1)

        # --- 2. Load GRVT [关键修复: 补全代码] ---
        if "GRVT" in required_exchanges:
            try:
                from app.adapters.grvt import GrvtAdapter

                # 安全获取配置
                api_key = settings.grvt_api_key.get_secret_value() if settings.grvt_api_key else ""
                p_key = settings.grvt_private_key.get_secret_value() if settings.grvt_private_key else ""

                if not api_key or not p_key:
                    raise ValueError("GRVT API Key or Private Key missing in .env")

                adapter = GrvtAdapter(
                    api_key=api_key,
                    private_key=p_key,
                    trading_account_id=settings.grvt_trading_account_id,
                    symbols=settings.common.target_symbols
                )
                adapter.backoff_controller = self.backoff_controller
                self.adapters.append(adapter)
                logger.info("✅ GRVT Adapter Loaded")
            except Exception as e:
                logger.critical(f"GRVT Load Failed: {e}", exc_info=True)
                sys.exit(1)

        # --- 3. Load Lighter ---
        if "Lighter" in required_exchanges:
            try:
                from app.adapters.lighter import LighterAdapter
                l_api = settings.lighter_api_key.get_secret_value() if settings.lighter_api_key else ""
                l_pk = settings.lighter_private_key.get_secret_value() if settings.lighter_private_key else ""

                adapter = LighterAdapter(
                    api_key=l_api,
                    private_key=l_pk,
                    account_index=settings.lighter_account_index,
                    api_key_index=settings.lighter_api_key_index,
                    symbols=settings.common.target_symbols
                )
                adapter.backoff_controller = self.backoff_controller
                self.adapters.append(adapter)
                logger.info("✅ Lighter Adapter Loaded")
            except Exception as e:
                logger.critical(f"Lighter Load Failed: {e}")
                sys.exit(1)

    def _load_strategy(self):
        strategy_type = settings.strategies.active
        adapters_map = {ex.name: ex for ex in self.adapters}

        logger.info(f"🧠 初始化策略: {strategy_type}")

        if strategy_type == "HFT_MM":
            from app.strategies.hft_market_making import HFTMarketMakingStrategy
            return HFTMarketMakingStrategy(
                adapters=adapters_map,
                risk_controller=self.risk_controller
            )
        elif strategy_type == "GL_FARM":
            from app.strategies.grvt_lighter_farm import GrvtLighterFarmStrategy
            # 确保 Lighter 和 GRVT 适配器都已加载
            if "GRVT" not in adapters_map or "Lighter" not in adapters_map:
                raise RuntimeError("GL_FARM strategy requires both GRVT and Lighter adapters")
            return GrvtLighterFarmStrategy(adapters=adapters_map)
        elif strategy_type == "AI_GRID":
            from app.strategies.ai_grid import AiAdaptiveGridStrategy
            return AiAdaptiveGridStrategy(
                adapters=adapters_map,
                risk_controller=self.risk_controller
            )
        elif strategy_type == "SPREAD_ARB":
            from app.strategies.spread_arb import SpreadArbitrageStrategy
            # SpreadArb 构造函数需要 exchange_a 和 exchange_b 参数
            return SpreadArbitrageStrategy(
                adapters=adapters_map,
                exchange_a=settings.strategies.spread_arb.exchange_a,
                exchange_b=settings.strategies.spread_arb.exchange_b
            )
        else:
            raise ValueError(f"Unknown strategy: {strategy_type}")

    async def run(self):
        self.running = True

        # 初始化连接
        logger.info("🔌 连接交易所...")
        # 并发初始化所有适配器
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

        if self.risk_controller:
            await self.risk_controller.stop()

        # 停止所有适配器
        tasks = []
        for ex in self.adapters:
            if hasattr(ex, 'close'):
                tasks.append(ex.close())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("👋 Bye!")


async def main():
    # Windows 下 EventLoop 策略设置
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    orchestrator = SystemOrchestrator()
    stop_event = asyncio.Event()

    # [关键修复] 跨平台信号处理
    loop = asyncio.get_running_loop()

    def handle_stop_signal():
        if not stop_event.is_set():
            logger.info("🛑 Stop signal received. Initiating shutdown...")
            stop_event.set()

    if sys.platform != 'win32':
        # Linux/Mac 使用标准信号处理
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_stop_signal)
    else:
        # Windows 无法使用 add_signal_handler 处理 SIGINT/SIGTERM
        logger.info("ℹ️ Windows Mode: Press Ctrl+C to stop.")
        # 在 Windows 上，我们依赖下面的 try-except KeyboardInterrupt 块

    try:
        await orchestrator.setup()

        # 启动主任务
        run_task = asyncio.create_task(orchestrator.run())

        # 主阻塞逻辑
        while not stop_event.is_set():
            # 监控 run_task 状态，如果它异常退出，我们也退出
            if run_task.done():
                if run_task.exception():
                    logger.error(f"Main task failed: {run_task.exception()}")
                    raise run_task.exception()
                break

            # Windows 下这里可以响应 Ctrl+C
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    except KeyboardInterrupt:
        logger.warning("⚠️ KeyboardInterrupt (Ctrl+C) detected.")
        handle_stop_signal()
    except Exception as e:
        logger.critical(f"🔥 系统崩溃: {e}", exc_info=True)
    finally:
        # 确保任务被取消
        if 'run_task' in locals() and not run_task.done():
            run_task.cancel()
            try:
                await run_task
            except asyncio.CancelledError:
                pass

        await orchestrator.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # 最终的静默退出