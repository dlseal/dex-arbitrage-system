import asyncio
import logging
import signal
import sys
import os
from typing import List

current_dir = os.path.dirname(os.path.abspath(__file__))

if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# ==========================================
# 正常导入模块
# ==========================================
from app.config import Config
from app.adapters.base import BaseExchange
from app.adapters.grvt import GrvtAdapter
from app.adapters.lighter import LighterAdapter
from app.adapters.nado import NadoAdapter

from app.core.engine import EventEngine

# 导入所有策略
from app.strategies.spread_arb import SpreadArbitrageStrategy
from app.strategies.grvt_lighter_farm import GrvtLighterFarmStrategy
from app.strategies.grvt_inventory_farm import GrvtInventoryFarmStrategy
from app.strategies.hft_market_making import HFTMarketMakingStrategy

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger("GrvtCcxtWS").setLevel(logging.WARNING)
logging.getLogger("pysdk").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logger = logging.getLogger("Main")


async def main():
    logger.info(f"🚀 正在启动 DEX 对冲套利系统... (运行目录: {current_dir})")

    # 1. 验证配置
    try:
        Config.validate()
    except ValueError as e:
        logger.error(str(e))
        return

    # 2. 确定需要的交易所 (按需加载逻辑)
    # ==========================================
    required_exchanges = set()

    if Config.STRATEGY_TYPE == "HFT_MM":
        required_exchanges.add(Config.HFT_EXCHANGE)

    elif Config.STRATEGY_TYPE == "GL_FARM":
        required_exchanges.add("GRVT")
        required_exchanges.add("Lighter")

    elif Config.STRATEGY_TYPE == "GL_INVENTORY":
        required_exchanges.add("GRVT")

    else:
        # Spread Arb 模式
        required_exchanges.add(Config.SPREAD_EXCHANGE_A)
        required_exchanges.add(Config.SPREAD_EXCHANGE_B)

    logger.info(f"📋 当前策略 ({Config.STRATEGY_TYPE}) 需要加载的交易所: {required_exchanges}")

    # 3. 实例化交易所适配器
    # ==========================================
    adapters: List[BaseExchange] = []

    # --- 初始化 GRVT ---
    if "GRVT" in required_exchanges and Config.GRVT_API_KEY:
        try:
            grvt = GrvtAdapter(
                api_key=Config.GRVT_API_KEY,
                private_key=Config.GRVT_PRIVATE_KEY,
                trading_account_id=Config.GRVT_TRADING_ACCOUNT_ID,
                symbols=Config.TARGET_SYMBOLS
            )
            adapters.append(grvt)
            logger.info("📦 GRVT Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 GRVT Adapter: {e}")

    # --- 初始化 Lighter ---
    if "Lighter" in required_exchanges and Config.LIGHTER_API_KEY:
        try:
            lighter = LighterAdapter(
                api_key=Config.LIGHTER_API_KEY,
                private_key=Config.LIGHTER_PRIVATE_KEY,
                account_index=Config.LIGHTER_ACCOUNT_INDEX,
                api_key_index=Config.LIGHTER_API_KEY_INDEX,
                symbols=Config.TARGET_SYMBOLS
            )
            adapters.append(lighter)
            logger.info("📦 Lighter Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 Lighter Adapter: {e}")

    # --- 初始化 Nado ---
    if "Nado" in required_exchanges and Config.NADO_PRIVATE_KEY:
        try:
            nado = NadoAdapter(
                private_key=Config.NADO_PRIVATE_KEY,
                mode=Config.NADO_MODE,
                subaccount_name=Config.NADO_SUBACCOUNT_NAME,
                symbols=Config.TARGET_SYMBOLS
            )
            adapters.append(nado)
            logger.info("📦 Nado Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 Nado Adapter: {e}")


    if not adapters:
        logger.error(f"❌ 没有加载任何适配器！请检查 .env 配置或 STRATEGY_TYPE。")
        return

    # 4. 初始化策略 & 启动引擎
    adapters_map = {ex.name: ex for ex in adapters}
    strategy = None

    if Config.STRATEGY_TYPE == "HFT_MM":
        logger.info("⚡️ 启动模式: HFT Market Making (AS + OFI)")
        strategy = HFTMarketMakingStrategy(adapters_map)

    elif Config.STRATEGY_TYPE == "GL_FARM":
        logger.info("🚜 启动模式: GRVT(Maker) + Lighter(Taker) 刷量策略")
        strategy = GrvtLighterFarmStrategy(adapters_map)

    elif Config.STRATEGY_TYPE == "GL_INVENTORY":
        logger.info("🏭 启动模式: GRVT 库存累积刷量")
        strategy = GrvtInventoryFarmStrategy(adapters_map)

    else:
        logger.info(f"⚖️ 启动模式: 通用价差套利 (Spread Arb)")
        logger.info(f"   👉 交易所 A: {Config.SPREAD_EXCHANGE_A}")
        logger.info(f"   👉 交易所 B: {Config.SPREAD_EXCHANGE_B}")

        strategy = SpreadArbitrageStrategy(
            adapters=adapters_map,
            exchange_a=Config.SPREAD_EXCHANGE_A,
            exchange_b=Config.SPREAD_EXCHANGE_B
        )

    if hasattr(strategy, 'is_active') and not strategy.is_active:
        logger.error("❌ 策略初始化失败，正在退出...")
        return

    engine = EventEngine(exchanges=adapters, strategy=strategy)

    def handle_exit(sig, frame):
        logger.info("\n🛑 接收到退出信号，正在关闭系统...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # 5. 执行初始化测试
    logger.info("🔌 正在连接交易所并同步状态...")
    try:
        await asyncio.gather(*(ex.initialize() for ex in adapters))
        logger.info("✅ 所有交易所连接成功！")

        logging.info("\n" + "=" * 50)
        logging.info(f"{'Exchange':<15} | {'Symbol':<15} | {'Bid':<15} | {'Ask':<15}")
        logging.info("-" * 50)

        for ex in adapters:
            try:
                target_sym = Config.TARGET_SYMBOLS[0] if Config.TARGET_SYMBOLS else "BTC"
                ticker = await ex.fetch_orderbook(target_sym)

                if not ticker:
                    logging.info(f"{ex.name:<15} | {target_sym + '(N/A)':<15} | {'-':<15} | {'-':<15}")
                else:
                    logging.info(
                        f"{ex.name:<15} | {ticker.get('symbol', '?'):<15} | {ticker.get('bid', 0):<15} | {ticker.get('ask', 0):<15}")
            except Exception as e:
                logging.info(f"{ex.name:<15} | {'ERROR':<15} | {str(e):<30}")
        logging.info("=" * 50 + "\n")

    except Exception as e:
        logger.error(f"❌ 初始化过程中发生严重错误: {e}")
        return

    # 6. 进入主事件循环
    logger.info("📡 启动数据流监听...")
    await engine.start()


if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        asyncio.run(main())
    except KeyboardInterrupt:
        pass