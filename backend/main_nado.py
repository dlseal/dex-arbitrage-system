# backend/main_nado.py
import asyncio
import logging
import sys
import os
import signal
import getpass
import traceback

# 1. 路径设置
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# 2. [关键配置劫持] - 必须最先执行
try:
    import app.config_nado as config_nado

    sys.modules['app.config'] = config_nado
    if not hasattr(config_nado, 'settings'):
        raise ImportError("config_nado missing settings")
    print("✅ [Boot] Config hijacked: app.config -> app.config_nado")
except Exception as e:
    print(f"❌ Config hijack failure: {e}")
    sys.exit(1)

# 3. 业务导入
try:
    from app.adapters.nado import NadoAdapter
    from app.core.engine import EventEngine
    from app.config import settings
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

# 日志配置
logging.basicConfig(
    level=settings.common.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NadoMain")


async def main():
    logger.info(f"🚀 Nado Trading System Starting (Mode: {settings.nado_mode})")

    # --- 密钥输入 ---
    if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
        try:
            key = getpass.getpass("\n🔐 Enter Master Key > ")
            if not key: return
            os.environ["MASTER_KEY"] = key.strip()
        except KeyboardInterrupt:
            return

    # --- 初始化组件 ---
    try:
        nado_key = settings.nado_private_key.get_secret_value() if settings.nado_private_key else None
        adapter = NadoAdapter(
            private_key=nado_key,
            mode=settings.nado_mode,
            subaccount_name=settings.nado_subaccount_name,
            symbols=settings.common.target_symbols
        )
        adapters_map = {adapter.name: adapter}

        # 策略工厂
        strategy_type = settings.strategies.active
        logger.info(f"📋 Strategy Type: {strategy_type}")

        if strategy_type == "AI_GRID":
            from app.strategies.ai_grid import AiAdaptiveGridStrategy
            strategy = AiAdaptiveGridStrategy(adapters_map)
            strategy.name = "AI_GRID"
        elif strategy_type == "HFT_MM":
            from app.strategies.hft_market_making import HFTMarketMakingStrategy
            strategy = HFTMarketMakingStrategy(adapters_map)
            strategy.name = "HFT_MM"
        else:
            logger.error(f"❌ Unknown Strategy: {strategy_type}")
            return

        if hasattr(strategy, 'start'):
            logger.info("🧠 Bootstrapping Strategy Logic...")
            await strategy.start()

        engine = EventEngine(exchanges=[adapter], strategy=strategy)

    except Exception as e:
        logger.critical(f"❌ Init Failed: {e}", exc_info=True)
        return

    # --- 信号处理 (修改后) ---
    stop_event = asyncio.Event()

    def signal_shutdown_handler():
        """处理 SIGTERM (kill 命令) -> 执行退出"""
        logger.info("🛑 Stop signal (SIGTERM) received. Shutting down...")
        stop_event.set()

    def signal_ignore_handler():
        """处理 SIGINT (Ctrl+C) -> 忽略并警告"""
        logger.warning("⚠️ Ctrl+C (SIGINT) received. IGNORED to prevent accidental stop.")
        logger.warning("ℹ️  To stop the process, use 'kill <pid>' (SIGTERM).")

    # 非 Windows 系统下 (Linux/Mac) 分离信号处理
    if sys.platform != 'win32':
        loop = asyncio.get_running_loop()

        # 1. 注册 SIGTERM 用于真正的停止
        try:
            loop.add_signal_handler(signal.SIGTERM, signal_shutdown_handler)
        except NotImplementedError:
            pass

        # 2. 注册 SIGINT 用于屏蔽 Ctrl+C
        try:
            loop.add_signal_handler(signal.SIGINT, signal_ignore_handler)
        except NotImplementedError:
            pass
    else:
        # Windows 下通常很难屏蔽 Ctrl+C 且不影响控制台，保持原样
        logger.info("ℹ️ Windows Mode: Use Ctrl+C to stop.")

        def simple_handler():
            logger.info("🛑 Stop signal received.")
            stop_event.set()

        try:
            # Windows 只能简单捕获
            loop = asyncio.get_running_loop()
            # 注意: Windows asyncio 对信号支持有限，通常依赖 KeyboardInterrupt
        except Exception:
            pass

    # --- 连接与启动 ---
    logger.info("🔌 Connecting...")
    try:
        await adapter.initialize()
    except Exception as e:
        logger.error(f"❌ Connect Error: {e}")
        return

    logger.info("📡 Engine Starting...")
    engine_task = asyncio.create_task(engine.start())

    # --- [主循环] ---
    try:
        while not stop_event.is_set():
            if engine_task.done():
                exc = engine_task.exception()
                if exc:
                    logger.critical(f"💥 Engine Task CRASHED: {exc}")
                    traceback.print_exception(type(exc), exc, exc.__traceback__)
                else:
                    logger.warning("⚠️ Engine task finished unexpectedly.")
                break

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    except (KeyboardInterrupt, asyncio.CancelledError):
        # 这里的 KeyboardInterrupt 主要针对 Windows，或者信号注册失败的情况
        logger.info("🛑 Keyboard Interrupt caught in main loop.")
        stop_event.set()

    finally:
        logger.info("🛑 Shutting down...")
        engine.running = False
        stop_event.set()

        if not engine_task.done():
            engine_task.cancel()

        if hasattr(adapter, 'close'):
            try:
                await asyncio.wait_for(adapter.close(), timeout=2.0)
            except Exception:
                pass

        try:
            await engine_task
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

        logger.info("👋 System Exit")


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # 如果信号被屏蔽，这里不会触发；如果未屏蔽(Windows)，这里处理退出
        pass