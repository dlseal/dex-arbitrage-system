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
    # 1. 定义参与套利的两个交易所名称 (对应 Adapter 的 name) [NEW]
    # 允许通过环境变量动态切换，例如 A=Binance, B=GRVT
    SPREAD_EXCHANGE_A = os.getenv("SPREAD_EXCHANGE_A", "Lighter")
    SPREAD_EXCHANGE_B = os.getenv("SPREAD_EXCHANGE_B", "GRVT")

    # 2. 触发套利的最小价差阈值 (默认 0.002 即 0.2%)
    SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.002"))

    # 3. 交易冷却时间 (秒)，防止频繁开单
    TRADE_COOLDOWN = float(os.getenv("TRADE_COOLDOWN", "2.0"))

    # 4. 单次下单数量配置 (JSON格式)
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
    MAX_SLIPPAGE_TOLERANCE = float(os.getenv("MAX_SLIPPAGE_TOLERANCE", "-0.0005"))

    # 重挂单阈值
    REQUOTE_THRESHOLD = float(os.getenv("REQUOTE_THRESHOLD", "0.0005"))

    FARM_SIDE = os.getenv("FARM_SIDE", "BUY").upper()
    MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "3"))

    # ==========================
    # 库存刷量策略专用配置 (Inventory Farm Config)
    # ==========================
    MAX_INVENTORY_USD = float(os.getenv("MAX_INVENTORY_USD", "800.0"))
    INVENTORY_LAYERS = int(os.getenv("INVENTORY_LAYERS", "3"))
    INVENTORY_LAYER_SPREAD = int(os.getenv("INVENTORY_LAYER_SPREAD", "1"))

    # ==========================
    # HFT 做市策略配置 (AS + OFI)
    # ==========================
    # 目标交易所 (填写 Adapter 的 name，如 "GRVT" 或 "Lighter")
    HFT_EXCHANGE = os.getenv("HFT_EXCHANGE", "GRVT")

    # AS模型参数: 风险厌恶系数 (Gamma)
    # 值越大，持仓时对价格的偏移越激进（越急于平仓）
    HFT_RISK_AVERSION = float(os.getenv("HFT_RISK_AVERSION", "0.1"))

    # 信号参数: OFI 敏感度 (Alpha)
    # OFI 每增加 1 单位，价格预测偏移多少个 Tick
    HFT_OFI_SENSITIVITY = float(os.getenv("HFT_OFI_SENSITIVITY", "0.5"))

    # 波动率因子 (k的替代方案)
    # Spread = MinSpread + VolFactor * Sigma
    HFT_VOLATILITY_FACTOR = float(os.getenv("HFT_VOLATILITY_FACTOR", "0.8"))

    # 统计窗口大小 (用于计算 OFI 均值和波动率)
    HFT_WINDOW_SIZE = int(os.getenv("HFT_WINDOW_SIZE", "100"))

    # 最小做市价差 (以 Tick 为单位，例如 2 ticks)
    HFT_MIN_SPREAD_TICKS = int(os.getenv("HFT_MIN_SPREAD_TICKS", "2"))

    # 报价更新阈值 (防抖动：只有新价格偏离当前订单 > N 个 Tick 才改单)
    HFT_UPDATE_THRESHOLD_TICKS = int(os.getenv("HFT_UPDATE_THRESHOLD_TICKS", "1"))

    # 最大单边持仓限制 (USD)，超过此值触发强制平仓或停止开仓
    HFT_MAX_POS_USD = float(os.getenv("HFT_MAX_POS_USD", "50000.0"))

    # --- GRVT 配置 ---
    GRVT_API_KEY = os.getenv("GRVT_API_KEY")
    GRVT_PRIVATE_KEY = os.getenv("GRVT_PRIVATE_KEY")
    GRVT_TRADING_ACCOUNT_ID = os.getenv("GRVT_TRADING_ACCOUNT_ID")
    GRVT_ENV = os.getenv("GRVT_ENVIRONMENT", "prod")

    # --- Lighter 配置 ---
    LIGHTER_API_KEY = os.getenv("LIGHTER_API_KEY")
    LIGHTER_PRIVATE_KEY = os.getenv("LIGHTER_PRIVATE_KEY")
    LIGHTER_ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
    LIGHTER_API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

    # --- Nado 配置 ---
    NADO_PRIVATE_KEY = os.getenv("NADO_PRIVATE_KEY")
    NADO_MODE = os.getenv("NADO_MODE", "MAINNET")
    NADO_SUBACCOUNT_NAME = os.getenv("NADO_SUBACCOUNT_NAME", "default")

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