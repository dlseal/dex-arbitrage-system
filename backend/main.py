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
from app.core.engine import EventEngine

# 导入所有策略
from app.strategies.spread_arb import SpreadArbitrageStrategy
from app.strategies.grvt_lighter_farm import GrvtLighterFarmStrategy
from app.strategies.grvt_inventory_farm import GrvtInventoryFarmStrategy

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# 屏蔽一些嘈杂的日志
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

    # 2. 实例化交易所适配器
    adapters: List[BaseExchange] = []

    # --- 初始化 GRVT ---
    if Config.GRVT_API_KEY:
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
    if Config.LIGHTER_API_KEY:
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

    if not adapters:
        logger.error("❌ 没有可用的交易所适配器，系统退出。请检查 .env 配置。")
        return

    # 3. 初始化策略 & 启动引擎
    adapters_map = {ex.name: ex for ex in adapters}
    strategy = None

    # 根据配置选择策略
    if Config.STRATEGY_TYPE == "GL_FARM":
        logger.info("🚜 启动模式: GRVT(Maker) + Lighter(Taker) 刷量策略")
        strategy = GrvtLighterFarmStrategy(adapters_map)
    elif Config.STRATEGY_TYPE == "GL_INVENTORY":
        logger.info("🏭 启动模式: GRVT 库存累积刷量 (小资金专用)")
        strategy = GrvtInventoryFarmStrategy(adapters_map)
    else:
        # === 优化点：注入配置的交易所名称 ===
        logger.info(f"⚖️ 启动模式: 通用价差套利 (Spread Arb)")
        logger.info(f"   👉 交易所 A: {Config.SPREAD_EXCHANGE_A}")
        logger.info(f"   👉 交易所 B: {Config.SPREAD_EXCHANGE_B}")

        strategy = SpreadArbitrageStrategy(
            adapters=adapters_map,
            exchange_a=Config.SPREAD_EXCHANGE_A,
            exchange_b=Config.SPREAD_EXCHANGE_B
        )

    # 确保策略初始化成功
    if hasattr(strategy, 'is_active') and not strategy.is_active:
        logger.error("❌ 策略初始化失败，正在退出...")
        return

    # 将策略注入引擎
    engine = EventEngine(exchanges=adapters, strategy=strategy)

    # 注册优雅退出信号 (Ctrl+C)
    def handle_exit(sig, frame):
        logger.info("\n🛑 接收到退出信号，正在关闭系统...")
        # 这里可以添加清理逻辑，如 cancel_all_orders
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # 4. 执行初始化测试 (Connectivity Check)
    logger.info("🔌 正在连接交易所并同步状态...")
    try:
        # 并发执行所有交易所的 initialize 方法
        await asyncio.gather(*(ex.initialize() for ex in adapters))
        logger.info("✅ 所有交易所连接成功！")

        # --- 连接性验证 ---
        logging.info("\n" + "=" * 50)
        logging.info(f"{'Exchange':<15} | {'Symbol':<15} | {'Bid':<15} | {'Ask':<15}")
        logging.info("-" * 50)

        for ex in adapters:
            try:
                # 简单测试获取 BTC 价格
                # 注意：这里保持简单，因为不同 Adapter 可能对 Symbol 要求不同，但通常 BTC 都是支持的
                ticker = await ex.fetch_orderbook("BTC")
                # 若 Adapter 返回空，可能是 Symbol 格式问题，但在初始化连接测试中仅做展示
                if not ticker:
                    logging.info(f"{ex.name:<15} | {'BTC(N/A)':<15} | {'-':<15} | {'-':<15}")
                else:
                    logging.info(
                        f"{ex.name:<15} | {ticker.get('symbol', '?'):<15} | {ticker.get('bid', 0):<15} | {ticker.get('ask', 0):<15}")
            except Exception as e:
                logging.info(f"{ex.name:<15} | {'ERROR':<15} | {str(e):<30}")
        logging.info("=" * 50 + "\n")

    except Exception as e:
        logger.error(f"❌ 初始化过程中发生严重错误: {e}")
        return

    # 5. 进入主事件循环
    logger.info("📡 启动 WebSocket 数据流监听...")
    await engine.start()


if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        asyncio.run(main())
    except KeyboardInterrupt:
        pass