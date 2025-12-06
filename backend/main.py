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

# 交易策略
from app.strategies.spread_arb import SpreadArbitrageStrategy

# 配置日志格式
logging.basicConfig(
    level=logging.DEBUG,  # <--- 打开调试开关
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", # 增加 %(name)s 查看是哪个模块发的
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 为了防止 requests/urllib3/asyncio 产生太多垃圾日志，屏蔽掉它们
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.DEBUG) # 👈 关键：我们要看 websockets 的底层日志

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

    # --- 初始化 GRVT (传入 symbols) ---
    if Config.GRVT_API_KEY:
        try:
            grvt = GrvtAdapter(
                api_key=Config.GRVT_API_KEY,
                private_key=Config.GRVT_PRIVATE_KEY,
                trading_account_id=Config.GRVT_TRADING_ACCOUNT_ID,
                symbols=Config.TARGET_SYMBOLS  # 👈 关键修改：传入配置
            )
            adapters.append(grvt)
            logger.info("📦 GRVT Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 GRVT Adapter: {e}")

    # --- 初始化 Lighter (传入 symbols) ---
    if Config.LIGHTER_API_KEY:
        try:
            lighter = LighterAdapter(
                api_key=Config.LIGHTER_API_KEY,
                private_key=Config.LIGHTER_PRIVATE_KEY,
                account_index=Config.LIGHTER_ACCOUNT_INDEX,
                api_key_index=Config.LIGHTER_API_KEY_INDEX,
                symbols=Config.TARGET_SYMBOLS  # 👈 关键修改：传入配置
            )
            adapters.append(lighter)
            logger.info("📦 Lighter Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 Lighter Adapter: {e}")

    if not adapters:
        logger.error("❌ 没有可用的交易所适配器，系统退出。请检查 .env 配置。")
        return

    # 3. 初始化策略 & 启动引擎
    strategy = SpreadArbitrageStrategy()

    # 将策略注入引擎
    engine = EventEngine(exchanges=adapters, strategy=strategy)

    # 注册优雅退出信号 (Ctrl+C)
    def handle_exit(sig, frame):
        logger.info("\n🛑 接收到退出信号，正在关闭系统...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # 4. 执行初始化测试 (Connectivity Check)
    logger.info("🔌 正在连接交易所并同步状态...")
    try:
        # 并发执行所有交易所的 initialize 方法
        await asyncio.gather(*(ex.initialize() for ex in adapters))
        logger.info("✅ 所有交易所连接成功！")

        # --- 连接性验证：打印当前的 BTC 价格 ---
        print("\n" + "=" * 50)
        print(f"{'Exchange':<15} | {'Symbol':<15} | {'Bid':<15} | {'Ask':<15}")
        print("-" * 50)

        for ex in adapters:
            try:
                # 尝试获取 BTC-USDT 的订单簿
                # 注意: 确保您的 Adapter 内部逻辑能处理 "BTC-USDT" 字符串
                ticker = await ex.fetch_orderbook("BTC-USDT")
                print(f"{ex.name:<15} | {ticker['symbol']:<15} | {ticker['bid']:<15} | {ticker['ask']:<15}")
            except Exception as e:
                print(f"{ex.name:<15} | {'ERROR':<15} | {str(e):<30}")
        print("=" * 50 + "\n")

    except Exception as e:
        logger.error(f"❌ 初始化过程中发生严重错误: {e}")
        # 如果初始化失败，不要继续启动 WS
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