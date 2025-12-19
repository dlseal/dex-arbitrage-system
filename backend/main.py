import asyncio
import logging
import signal
import sys
import os
from typing import List
import getpass

current_dir = os.path.dirname(os.path.abspath(__file__))

if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# ==========================================
# 导入模块
# ==========================================
from app.config import settings  # <--- 使用新的配置单例
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
from app.strategies.ai_grid import AiAdaptiveGridStrategy

# 配置日志格式
logging.basicConfig(
    level=settings.common.log_level,  # 从配置读取日志级别
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 压制第三方库的繁杂日志
logging.getLogger("GrvtCcxtWS").setLevel(logging.WARNING)
logging.getLogger("pysdk").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logger = logging.getLogger("Main")


async def main():
    logger.info(f"🚀 正在启动 DEX 对冲套利系统... (运行目录: {current_dir})")

    # ==========================================
    # 1. 安全启动检查 (Nado Encrypted Key)
    # ==========================================
    # 注意：这里依然使用 os.getenv 检查，因为 settings 加载时可能还没有 MASTER_KEY
    if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
        print("\n" + "=" * 50)
        print("🔐 安全启动模式")
        print("检测到 'ENCRYPTED_NADO_KEY'，请输入解密主密钥。")
        print("（输入内容将隐藏，完成后按回车）")
        print("=" * 50)

        try:
            master_input = getpass.getpass("🔑 Master Key > ")
            if not master_input:
                logger.error("❌ 未输入密钥，系统退出。")
                return

            # 将输入的密钥临时写入环境变量（供 Adapter 内部逻辑读取）
            os.environ["MASTER_KEY"] = master_input.strip()
            logger.info("✅ 主密钥已加载至内存")

        except KeyboardInterrupt:
            print("\n已取消")
            return

    # ==========================================
    # 2. 确定策略与所需交易所
    # ==========================================
    strategy_type = settings.strategies.active
    required_exchanges = set()

    if strategy_type == "HFT_MM":
        required_exchanges.add(settings.strategies.hft_mm.exchange)

    elif strategy_type == "GL_FARM":
        required_exchanges.add("GRVT")
        required_exchanges.add("Lighter")

    elif strategy_type == "GL_INVENTORY":
        required_exchanges.add("GRVT")

    elif strategy_type == "AI_GRID":
        required_exchanges.add(settings.strategies.ai_grid.exchange)

    else:
        # Spread Arb 模式 (默认)
        required_exchanges.add(settings.strategies.spread_arb.exchange_a)
        required_exchanges.add(settings.strategies.spread_arb.exchange_b)

    logger.info(f"📋 当前策略 ({strategy_type}) 需要加载的交易所: {required_exchanges}")

    # ==========================================
    # 3. 实例化交易所适配器
    # ==========================================
    adapters: List[BaseExchange] = []

    # --- 初始化 GRVT ---
    if "GRVT" in required_exchanges and settings.grvt_api_key:
        try:
            # 注意：SecretStr 需要调用 .get_secret_value() 获取明文
            grvt = GrvtAdapter(
                api_key=settings.grvt_api_key.get_secret_value(),
                private_key=settings.grvt_private_key.get_secret_value() if settings.grvt_private_key else None,
                trading_account_id=settings.grvt_trading_account_id,
                symbols=settings.common.target_symbols
            )
            adapters.append(grvt)
            logger.info("📦 GRVT Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 GRVT Adapter: {e}")

    # --- 初始化 Lighter ---
    if "Lighter" in required_exchanges and settings.lighter_api_key:
        try:
            lighter = LighterAdapter(
                api_key=settings.lighter_api_key.get_secret_value(),
                private_key=settings.lighter_private_key.get_secret_value() if settings.lighter_private_key else None,
                account_index=settings.lighter_account_index,
                api_key_index=settings.lighter_api_key_index,
                symbols=settings.common.target_symbols
            )
            adapters.append(lighter)
            logger.info("📦 Lighter Adapter 已加载")
        except Exception as e:
            logger.error(f"无法加载 Lighter Adapter: {e}")

    # --- 初始化 Nado ---
    # Nado 的私钥可能来自 settings (明文) 或环境变量 (解密流程)
    nado_key_plain = settings.nado_private_key.get_secret_value() if settings.nado_private_key else None

    if "Nado" in required_exchanges:
        # 如果有私钥或者有加密的 Key，都尝试初始化
        if nado_key_plain or settings.encrypted_nado_key:
            try:
                nado = NadoAdapter(
                    private_key=nado_key_plain,
                    mode=settings.nado_mode,
                    subaccount_name=settings.nado_subaccount_name,
                    symbols=settings.common.target_symbols
                )
                adapters.append(nado)
                logger.info("📦 Nado Adapter 已加载")
            except Exception as e:
                logger.error(f"无法加载 Nado Adapter: {e}")
        else:
            logger.error("❌ 需要 Nado 但未找到私钥配置")

    if not adapters:
        logger.error(f"❌ 没有加载任何适配器！请检查 .env / config.yaml 或 active 策略配置。")
        return

    # ==========================================
    # 4. 初始化策略 & 启动引擎
    # ==========================================
    adapters_map = {ex.name: ex for ex in adapters}
    strategy = None

    if strategy_type == "HFT_MM":
        logger.info("⚡️ 启动模式: HFT Market Making (AS + OFI)")
        strategy = HFTMarketMakingStrategy(adapters_map)

    elif strategy_type == "GL_FARM":
        logger.info("🚜 启动模式: GRVT(Maker) + Lighter(Taker) 刷量策略")
        strategy = GrvtLighterFarmStrategy(adapters_map)

    elif strategy_type == "GL_INVENTORY":
        logger.info("🏭 启动模式: GRVT 库存累积刷量")
        strategy = GrvtInventoryFarmStrategy(adapters_map)

    elif strategy_type == "AI_GRID":
        logger.info("🤖 启动模式: AI 自适应网格 (AI_GRID)")
        strategy = AiAdaptiveGridStrategy(adapters_map)

    else:
        # Spread Arb (读取 settings 中的 A/B 配置)
        ex_a = settings.strategies.spread_arb.exchange_a
        ex_b = settings.strategies.spread_arb.exchange_b

        logger.info(f"⚖️ 启动模式: 通用价差套利 (Spread Arb)")
        logger.info(f"   👉 交易所 A: {ex_a}")
        logger.info(f"   👉 交易所 B: {ex_b}")

        strategy = SpreadArbitrageStrategy(
            adapters=adapters_map,
            exchange_a=ex_a,
            exchange_b=ex_b
        )

    if hasattr(strategy, 'is_active') and not strategy.is_active:
        logger.error("❌ 策略初始化失败 (is_active=False)，正在退出...")
        return

    engine = EventEngine(exchanges=adapters, strategy=strategy)

    def handle_exit(sig, frame):
        logger.info("\n🛑 接收到退出信号，正在关闭系统...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # ==========================================
    # 5. 执行初始化测试 (连接交易所)
    # ==========================================
    logger.info("🔌 正在连接交易所并同步状态...")
    try:
        await asyncio.gather(*(ex.initialize() for ex in adapters))
        logger.info("✅ 所有交易所连接成功！")

        # 打印初始行情预览
        logging.info("\n" + "=" * 50)
        logging.info(f"{'Exchange':<15} | {'Symbol':<15} | {'Bid':<15} | {'Ask':<15}")
        logging.info("-" * 50)

        for ex in adapters:
            try:
                target_sym = settings.common.target_symbols[0] if settings.common.target_symbols else "BTC"
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
        logger.error(f"❌ 初始化过程中发生严重错误: {e}", exc_info=True)
        return

    # ==========================================
    # 6. 进入主事件循环
    # ==========================================
    logger.info("📡 启动数据流监听...")
    await engine.start()


if __name__ == "__main__":
    try:
        # Windows 平台下的 asyncio 策略修复
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        asyncio.run(main())
    except KeyboardInterrupt:
        pass