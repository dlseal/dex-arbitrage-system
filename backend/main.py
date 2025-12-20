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
from app.config import settings
from app.adapters.base import BaseExchange
from app.core.engine import EventEngine

# [修改点 1] 移除具体的 Adapter 和 Strategy 全局导入
# 避免在 Nado 环境中加载不兼容的库（如 Lighter 需要 web3 v7）
# from app.adapters.grvt import GrvtAdapter
# from app.adapters.lighter import LighterAdapter
# from app.adapters.nado import NadoAdapter

# 配置日志格式
logging.basicConfig(
    level=settings.common.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 压制无关日志
logging.getLogger("pysdk").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logger = logging.getLogger("Main")


async def main():
    logger.info(f"🚀 正在启动 DEX 系统 (Nado 独立模式)... (运行目录: {current_dir})")

    # ==========================================
    # 1. 安全启动检查 (Nado Encrypted Key)
    # ==========================================
    if settings.encrypted_nado_key and not os.getenv("MASTER_KEY"):
        print("\n" + "=" * 50)
        print("🔐 安全启动模式")
        print("检测到 'ENCRYPTED_NADO_KEY'，请输入解密主密钥。")
        print("=" * 50)

        try:
            master_input = getpass.getpass("🔑 Master Key > ")
            if not master_input:
                logger.error("❌ 未输入密钥，系统退出。")
                return
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
    strategy_class = None

    # [修改点 2] 动态确定策略类，避免不必要的导入
    if strategy_type == "HFT_MM":
        required_exchanges.add(settings.strategies.hft_mm.exchange)
        from app.strategies.hft_market_making import HFTMarketMakingStrategy
        strategy_class = HFTMarketMakingStrategy

    elif strategy_type == "AI_GRID":
        required_exchanges.add(settings.strategies.ai_grid.exchange)
        from app.strategies.ai_grid import AiAdaptiveGridStrategy
        strategy_class = AiAdaptiveGridStrategy

    elif strategy_type in ["GL_FARM", "GL_INVENTORY", "SPREAD_ARB"]:
        logger.critical(f"❌ Nado 独立模式不支持双边/跨链策略 ({strategy_type})")
        logger.critical("请在 config.yaml 中将 active 修改为 HFT_MM 或 AI_GRID")
        return

    else:
        # 默认回退 (Spread Arb) - Nado 模式下不支持，直接报错
        logger.error(f"❌ 未知或不支持的策略类型: {strategy_type}")
        return

    logger.info(f"📋 当前策略: {strategy_type} | 交易所: {required_exchanges}")

    # ==========================================
    # 3. 动态实例化适配器 (解决依赖冲突的核心)
    # ==========================================
    adapters: List[BaseExchange] = []

    # --- 仅当需要 Nado 时才导入 NadoAdapter ---
    if "Nado" in required_exchanges:
        try:
            logger.info("📦 正在加载 Nado 模块...")
            from app.adapters.nado import NadoAdapter  # <--- 局部导入

            nado_key_plain = settings.nado_private_key.get_secret_value() if settings.nado_private_key else None

            if nado_key_plain or settings.encrypted_nado_key:
                nado = NadoAdapter(
                    private_key=nado_key_plain,
                    mode=settings.nado_mode,
                    subaccount_name=settings.nado_subaccount_name,
                    symbols=settings.common.target_symbols
                )
                adapters.append(nado)
                logger.info("✅ Nado Adapter 已加载")
            else:
                logger.error("❌ 需要 Nado 但未找到私钥配置")
        except ImportError as e:
            logger.critical(f"❌ Nado 依赖缺失: {e}")
            logger.critical("请确保使用了 requirements-nado.txt 安装依赖 (Web3 v6 环境)")
            return
        except Exception as e:
            logger.error(f"❌ Nado 加载失败: {e}", exc_info=True)
            return

    # --- 其他交易所 (在 Nado 模式下通常不执行) ---
    if "GRVT" in required_exchanges:
        try:
            from app.adapters.grvt import GrvtAdapter
            # ... (初始化逻辑省略，Nado 模式不需要)
        except Exception as e:
            logger.warning(f"跳过 GRVT: {e}")

    if "Lighter" in required_exchanges:
        try:
            from app.adapters.lighter import LighterAdapter
            # ... (初始化逻辑省略，Nado 模式不需要)
        except Exception as e:
            logger.warning(f"跳过 Lighter: {e}")

    if not adapters:
        logger.error("❌ 没有加载任何适配器！系统退出。")
        return

    # ==========================================
    # 4. 初始化策略 & 启动引擎
    # ==========================================
    adapters_map = {ex.name: ex for ex in adapters}

    # 实例化之前确定的策略类
    strategy = strategy_class(adapters_map)

    if hasattr(strategy, 'is_active') and not strategy.is_active:
        logger.error("❌ 策略初始化状态为 inactive，退出...")
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

        # 打印行情预览
        logging.info("\n" + "=" * 50)
        logging.info(f"{'Exchange':<15} | {'Symbol':<15} | {'Price':<15}")
        logging.info("-" * 50)

        for ex in adapters:
            try:
                target_sym = settings.common.target_symbols[0] if settings.common.target_symbols else "BTC"
                ticker = await ex.fetch_orderbook(target_sym)
                if ticker:
                    mid = (ticker.get('bid', 0) + ticker.get('ask', 0)) / 2
                    logging.info(f"{ex.name:<15} | {ticker.get('symbol', '?'):<15} | {mid:<15.2f}")
                else:
                    logging.info(f"{ex.name:<15} | {target_sym} | (No Data)")
            except Exception as e:
                logging.info(f"{ex.name:<15} | ERROR | {str(e)}")
        logging.info("=" * 50 + "\n")

    except Exception as e:
        logger.error(f"❌ 初始化严重错误: {e}", exc_info=True)
        return

    # ==========================================
    # 6. 进入主事件循环
    # ==========================================
    logger.info("📡 启动数据流监听...")
    await engine.start()


if __name__ == "__main__":
    try:
        # Windows 平台策略修复
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        asyncio.run(main())
    except KeyboardInterrupt:
        pass