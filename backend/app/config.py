import json
import os
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()


class Config:
    # --- 全局交易标的配置 ---
    # 默认只监听 BTC, ETH, SOL。可以通过 .env 设置 TARGET_SYMBOLS=BTC,ETH,SOL,DOGE 来覆盖
    TARGET_SYMBOLS = os.getenv("TARGET_SYMBOLS", "BTC,ETH,SOL").split(",")

    # ==========================
    # 策略选择配置
    # ==========================
    # 策略类型: 'SPREAD' (价差套利) 或 'GL_FARM' (GRVT-Lighter刷量)
    STRATEGY_TYPE = os.getenv("STRATEGY_TYPE", "GL_FARM")

    # ==========================
    # 差价对冲策略 (Strategy Config)
    # ==========================
    # 1. 触发套利的最小价差阈值 (默认 0.002 即 0.2%)
    SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.002"))

    # 2. 交易冷却时间 (秒)，防止频繁开单
    TRADE_COOLDOWN = float(os.getenv("TRADE_COOLDOWN", "2.0"))

    # 3. 单次下单数量配置 (JSON格式)
    _default_quantities = {"SOL": 0.01, "DEFAULT": 0.0001}
    try:
        _env_qty = os.getenv("TRADE_QUANTITIES")
        TRADE_QUANTITIES = json.loads(_env_qty) if _env_qty else _default_quantities
    except Exception as e:
        print(f"⚠️ 解析 TRADE_QUANTITIES 失败，使用默认值: {e}")
        TRADE_QUANTITIES = _default_quantities

    # ==========================
    # 刷量策略参数 (Volume Farming Config)
    # ==========================
    # 允许的最大价差亏损 (滑点容忍度)
    # 例如 -0.0005 代表允许亏损 0.05% 去换取成交 (前提是 Rebate > 0.05%)
    # 正数表示必须要盈利才开单，负数表示愿意支付的成本
    MAX_SLIPPAGE_TOLERANCE = float(os.getenv("MAX_SLIPPAGE_TOLERANCE", "-0.0005"))

    # 重挂单阈值: 当目标价格与当前挂单价格偏差超过此比例时，撤单重挂
    REQUOTE_THRESHOLD = float(os.getenv("REQUOTE_THRESHOLD", "0.0005"))

    FARM_SIDE = os.getenv("FARM_SIDE", "BUY").upper()
    #连续失败熔断阈值
    # 如果连续 3 次对冲失败（Lighter下单报错），则触发熔断停止策略
    MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "3"))

    # ==========================
    # 🆕 库存刷量策略专用配置 (Inventory Farm Config)
    # ==========================
    # 最大持仓价值 (USD)，超过此值触发平仓
    MAX_INVENTORY_USD = float(os.getenv("MAX_INVENTORY_USD", "800.0"))

    # 挂单层数 (例如 3 表示同时挂买1, 买2, 买3)
    INVENTORY_LAYERS = int(os.getenv("INVENTORY_LAYERS", "3"))

    # 层间距 (Tick倍数)
    INVENTORY_LAYER_SPREAD = int(os.getenv("INVENTORY_LAYER_SPREAD", "1"))

    # --- GRVT 配置 ---
    GRVT_API_KEY = os.getenv("GRVT_API_KEY")
    GRVT_PRIVATE_KEY = os.getenv("GRVT_PRIVATE_KEY")
    GRVT_TRADING_ACCOUNT_ID = os.getenv("GRVT_TRADING_ACCOUNT_ID")
    GRVT_ENV = os.getenv("GRVT_ENVIRONMENT", "prod")  # prod / testnet

    # --- Lighter 配置 ---
    LIGHTER_API_KEY = os.getenv("LIGHTER_API_KEY")
    LIGHTER_PRIVATE_KEY = os.getenv("LIGHTER_PRIVATE_KEY")
    LIGHTER_ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
    LIGHTER_API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

    @classmethod
    def validate(cls):
        """检查核心配置是否存在"""
        missing = []
        if not cls.GRVT_API_KEY: missing.append("GRVT_API_KEY")
        if not cls.GRVT_PRIVATE_KEY: missing.append("GRVT_PRIVATE_KEY")
        if not cls.GRVT_TRADING_ACCOUNT_ID: missing.append("GRVT_TRADING_ACCOUNT_ID")
        if not cls.LIGHTER_PRIVATE_KEY: missing.append("LIGHTER_PRIVATE_KEY")

        if missing:
            raise ValueError(f"❌ 缺少必要的环境变量: {', '.join(missing)}")