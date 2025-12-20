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

# 2. [关键修复] 配置劫持 - 必须最先执行
try:
    # 强制加载 nado 专用配置
    import app.config_nado as config_nado

    # 注入到 sys.modules，欺骗后续的 import app.config
    sys.modules['app.config'] = config_nado

    # 确保 settings 对象存在
    if not hasattr(config_nado, 'settings'):
        print("❌ Error: config_nado missing 'settings' object")
        sys.exit(1)

    print("✅ [Boot] Config hijacked: app.config -> app.config_nado")

except Exception as e:
    print(f"❌ Config hijack critical failure: {e}")
    traceback.print_exc()
    sys.exit(1)

# 3. 业务模块导入 (必须在劫持之后)
try:
    from app.adapters.nado import NadoAdapter
    from app.core.engine import EventEngine
    # 这里的 settings 已经是 config_nado.settings
    from app.config import settings
except ImportError as e:
    print(f"❌ Import Error: {e}")
    traceback.print_exc()
    sys.exit(1)

logging.basicConfig(
    level=settings.common.log_level,
    format="%(asctime)s [%(levelname)s] NadoMain: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NadoMain")


async def main():
    logger.info(f"🚀 Nado Trading System Starting (Mode: {settings.nado_mode})")

    # --- 密钥处理 ---
    if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
        try:
            print("\n🔐 Security Check")
            key = getpass.getpass("Enter Master Key > ")
            if not key: return
            os.environ["MASTER_KEY"] = key.strip()
        except KeyboardInterrupt:
            return

    # --- 初始化 Adapter ---
    try:
        nado_key = settings.nado_private_key.get_secret_value() if settings.nado_private_key else None

        adapter = NadoAdapter(
            private_key=nado_key,
            mode=settings.nado_mode,
            subaccount_name=settings.nado_subaccount_name,
            symbols=settings.common.target_symbols
        )
        adapters_map = {adapter.name: adapter}
        adapters = [adapter]
    except Exception as e:
        logger.critical(f"❌ Adapter Init Failed: {e}", exc_info=True)
        return

    # --- 初始化策略 ---
    strategy_type = settings.strategies.active
    logger.info(f"📋 Strategy: {strategy_type}")

    strategy = None
    try:
        if strategy_type == "AI_GRID":
            from app.strategies.ai_grid import AiAdaptiveGridStrategy
            # 注入 RiskController 如果有的话，这里简化为 None 或自行初始化
            strategy = AiAdaptiveGridStrategy(adapters_map)
        elif strategy_type == "HFT_MM":
            from app.strategies.hft_market_making import HFTMarketMakingStrategy
            strategy = HFTMarketMakingStrategy(adapters_map)
        else:
            logger.error(f"❌ Unsupported Strategy: {strategy_type}")
            return
    except Exception as e:
        logger.critical(f"❌ Strategy Init Failed: {e}", exc_info=True)
        return

    # --- 启动引擎 ---
    engine = EventEngine(exchanges=adapters, strategy=strategy)

    # 信号处理
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("🛑 Stop signal received.")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    logger.info("🔌 Connecting to Exchange...")
    try:
        await adapter.initialize()
    except Exception as e:
        logger.error(f"❌ Connection Failed: {e}")
        return

    logger.info("📡 Engine Start")
    engine_task = asyncio.create_task(engine.start())

    await stop_event.wait()

    # 优雅关闭
    logger.info("🛑 Shutting down...")
    engine.running = False
    engine_task.cancel()
    if hasattr(adapter, 'close'):
        await adapter.close()

    try:
        await engine_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())