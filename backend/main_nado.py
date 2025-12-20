# backend/main_nado.py
import asyncio
import logging
import sys
import os
import signal
import getpass

# -------------------------------------------------------------------------
# 1. 路径与环境准备
# -------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# -------------------------------------------------------------------------
# 2. [关键修复] 劫持 app.config
# 必须在导入任何其他 app 模块之前执行！
# -------------------------------------------------------------------------
try:
    # 先导入我们的专用配置
    from app import config_nado

    # 强行将 'app.config' 指向 'app.config_nado'
    # 这样当 NadoAdapter 执行 "from app.config import settings" 时，
    # 实际上拿到的是 config_nado 里的 settings
    sys.modules['app.config'] = config_nado

    # 为了保险，把 settings 也直接挂载过去（防止 from app.config import settings 写法）
    if not hasattr(sys.modules['app.config'], 'settings'):
        sys.modules['app.config'].settings = config_nado.settings

    print("✅ 成功劫持配置模块: app.config -> app.config_nado")

except Exception as e:
    print(f"❌ 配置劫持失败: {e}")
    sys.exit(1)

# -------------------------------------------------------------------------
# 3. 现在可以安全导入业务模块了
# -------------------------------------------------------------------------
# 此时导入 NadoAdapter，它内部 import app.config 不会再报错
from app.adapters.nado import NadoAdapter
from app.core.engine import EventEngine

# 引入 Nado 专用配置对象
settings = config_nado.settings

logging.basicConfig(
    level=settings.common.log_level,
    format="%(asctime)s [%(levelname)s] NadoMain: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NadoMain")


async def main():
    logger.info("🚀 启动 Nado 专用交易系统 (Pydantic v1 / Web3 v6 兼容模式)")

    # --- 密钥解密逻辑 ---
    if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
        print("\n" + "=" * 50)
        print("🔐 安全模式: 检测到加密密钥")
        print("=" * 50)
        try:
            key = getpass.getpass("🔑 请输入 Master Key 解密 Nado 私钥 > ")
            if not key: return
            os.environ["MASTER_KEY"] = key.strip()
        except KeyboardInterrupt:
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
        adapters_map = {adapter.name: adapter}
        adapters = [adapter]
        logger.info("📦 Nado Adapter 已加载")
    except Exception as e:
        logger.critical(f"❌ Nado Adapter 初始化失败: {e}", exc_info=True)
        return

    # --- 加载策略 ---
    # 注意：这里也需要处理策略内部对 app.config 的引用
    strategy = None

    try:
        if strategy_type == "AI_GRID":
            from app.strategies.ai_grid import AiAdaptiveGridStrategy
            strategy = AiAdaptiveGridStrategy(adapters_map)

        elif strategy_type == "HFT_MM":
            from app.strategies.hft_market_making import HFTMarketMakingStrategy
            strategy = HFTMarketMakingStrategy(adapters_map)
        else:
            logger.error(f"❌ Nado 模式不支持策略: {strategy_type} (仅支持 AI_GRID 或 HFT_MM)")
            return
    except ImportError as e:
        logger.error(f"❌ 策略加载失败 (依赖缺失): {e}")
        return
    except Exception as e:
        logger.error(f"❌ 策略初始化错误: {e}", exc_info=True)
        return

    # --- 启动引擎 ---
    engine = EventEngine(exchanges=adapters, strategy=strategy)

    def handle_exit(sig, frame):
        logger.info("\n🛑 正在停止系统...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    logger.info("🔌 连接交易所...")
    try:
        await adapter.initialize()
    except Exception as e:
        logger.error(f"❌ 连接失败: {e}")
        return

    logger.info("📡 启动事件循环...")
    await engine.start()


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass