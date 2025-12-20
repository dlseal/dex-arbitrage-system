# backend/main_nado.py
import asyncio
import logging
import sys
import os
import signal
import getpass

# 1. 确保路径正确
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# 2. 导入专用配置 (Nado Config)
from app.config_nado import settings
from app.core.engine import EventEngine
# 仅导入 Nado Adapter
from app.adapters.nado import NadoAdapter

logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] NadoMain: %(message)s")
logger = logging.getLogger("NadoMain")


async def main():
    logger.info("🚀 启动 Nado 专用交易系统 (Pydantic v1 / Web3 v6)")

    # --- 密钥解密逻辑 (简化版) ---
    if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
        try:
            key = getpass.getpass("🔑 输入 Master Key 解密 Nado 私钥 > ")
            os.environ["MASTER_KEY"] = key
        except:
            return

    # --- 策略选择 ---
    strategy_type = settings.strategies.active
    logger.info(f"📋 当前策略: {strategy_type}")

    # --- 加载 Adapter ---
    try:
        nado_key = settings.nado_private_key.get_secret_value() if settings.nado_private_key else None
        adapter = NadoAdapter(
            private_key=nado_key,
            mode=settings.nado_mode,
            subaccount_name=settings.nado_subaccount_name,
            symbols=settings.common.target_symbols
        )
        adapters = [adapter]
        adapters_map = {adapter.name: adapter}
    except Exception as e:
        logger.critical(f"❌ Nado Adapter 初始化失败: {e}")
        return

    # --- 加载策略 ---
    strategy = None
    if strategy_type == "AI_GRID":
        from app.strategies.ai_grid import AiAdaptiveGridStrategy
        # 这里的 Strategy 可能会引用 app.config，你需要去 strategies/ai_grid.py 修改导入
        # 或者为了简单，我们在运行时 patch 一下 settings
        import app.strategies.ai_grid
        app.strategies.ai_grid.settings = settings  # 替换为 Nado settings
        strategy = AiAdaptiveGridStrategy(adapters_map)

    elif strategy_type == "HFT_MM":
        from app.strategies.hft_market_making import HFTMarketMakingStrategy
        import app.strategies.hft_market_making
        app.strategies.hft_market_making.settings = settings
        strategy = HFTMarketMakingStrategy(adapters_map)
    else:
        logger.error("❌ 仅支持 AI_GRID 或 HFT_MM")
        return

    # --- 启动 ---
    engine = EventEngine(exchanges=adapters, strategy=strategy)

    # 信号处理
    def handle_exit(*args):
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    logger.info("🔌 连接交易所...")
    await adapter.initialize()

    logger.info("📡 启动引擎...")
    await engine.start()


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())