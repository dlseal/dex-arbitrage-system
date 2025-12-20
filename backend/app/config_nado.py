# backend/app/config_nado.py
import os
import yaml
from typing import List, Dict, Optional
# [关键] Nado 环境使用 Pydantic v1，BaseSettings 直接位于 pydantic 中
from pydantic import BaseModel, Field, SecretStr, BaseSettings

# ==========================================
# 1. 定义兼容 Pydantic v1 的模型
# ==========================================

class CommonConfig(BaseModel):
    target_symbols: List[str] = ["BTC"]
    log_level: str = "INFO"
    trade_quantities: Dict[str, float] = {"DEFAULT": 0.0001}

class HftMmConfig(BaseModel):
    exchange: str = "Nado"
    risk_aversion: float = 0.1
    ofi_sensitivity: float = 1.0
    volatility_factor: float = 1.0
    window_size: int = 100
    min_spread_ticks: int = 2
    update_threshold_ticks: int = 2
    max_pos_usd: float = 1000.0

class SpreadArbConfig(BaseModel):
    exchange_a: str = "Lighter"
    exchange_b: str = "GRVT"
    threshold: float = 0.0015
    cooldown: float = 1.5

class FarmingConfig(BaseModel):
    side: str = "BUY"
    max_slippage_tolerance: float = -0.0005
    requote_threshold: float = 0.0002
    max_inventory_usd: float = 800.0
    inventory_layers: int = 3
    inventory_layer_spread: int = 1

class AiGridConfig(BaseModel):
    exchange: str = "Nado"  # Nado 模式下默认指向 Nado
    grid_mode: str = "GEOMETRIC"
    upper_price: float = 100000.0
    lower_price: float = 90000.0
    grid_count: int = 20
    principal: float = 400.0
    leverage: float = 2.0
    min_order_size: float = 100.0
    max_check_interval: int = 86400

class StrategiesConfig(BaseModel):
    active: str = "AI_GRID"
    hft_mm: HftMmConfig = Field(default_factory=HftMmConfig)
    spread_arb: SpreadArbConfig = Field(default_factory=SpreadArbConfig)
    farming: FarmingConfig = Field(default_factory=FarmingConfig)
    ai_grid: AiGridConfig = Field(default_factory=AiGridConfig)

class LlmConfig(BaseModel):
    base_url: str = "https://api.deepseek.com"
    model: str = "deepseek-chat"

class ServicesConfig(BaseModel):
    llm: LlmConfig = Field(default_factory=LlmConfig)

# ==========================================
# 2. 定义总配置 Settings (适配 Pydantic v1 写法)
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENV_PATH = os.path.join(BASE_DIR, ".env")
YAML_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")

class Settings(BaseSettings):
    # --- 环境变量 (Secrets) ---
    grvt_api_key: Optional[SecretStr] = None
    grvt_private_key: Optional[SecretStr] = None
    grvt_trading_account_id: Optional[str] = None
    grvt_environment: str = "prod"

    lighter_api_key: Optional[SecretStr] = None
    lighter_private_key: Optional[SecretStr] = None
    lighter_account_index: int = 0
    lighter_api_key_index: int = 0

    nado_private_key: Optional[SecretStr] = None
    nado_mode: str = "MAINNET"
    nado_subaccount_name: str = "default"
    encrypted_nado_key: Optional[SecretStr] = None
    master_key: Optional[SecretStr] = None

    llm_api_key: Optional[SecretStr] = None

    # --- YAML 配置 ---
    common: CommonConfig = Field(default_factory=CommonConfig)
    strategies: StrategiesConfig = Field(default_factory=StrategiesConfig)
    services: ServicesConfig = Field(default_factory=ServicesConfig)

    # [补全] 完整的 Prompt 模板
    llm_prompt_template: str = """
        你是一个加密货币高频量化交易专家。你正在管理一个 {symbol} 的网格交易策略。

        【市场数据】
        1. 价格: {price}
        2. 波动率 (ATR-14): {atr} (如果 ATR 升高，应增加网格间距/减少网格数)
        3. 趋势指标: RSI-14={rsi}, 布林带位置={bb_pct}% (0=下轨, 100=上轨)
        4. 短期趋势 (1H): {trend_1h}
        5. 盘口压力: 买盘 {bid_vol} vs 卖盘 {ask_vol} (Imbalance: {imbalance}%)
        
        【最近 K 线 (15m)】
        {recent_candles}

        【当前策略状态】
        状态: {current_status}
        当前持仓: {inventory}
        参数: {current_params}

        【任务】
        请根据波动率(ATR)和趋势(RSI/MA)优化网格参数。
        - 如果 RSI > 70 (超买)，考虑向上偏移区间或减少多单网格。
        - 如果 ATR 激增，说明波动剧烈，请扩大 grid_count 或 widening 价格区间以防破网。
        - 如果盘口卖压极大，考虑下调 upper_price。

        请严格返回JSON格式：
        {{
            "action": "UPDATE" | "CONTINUE",
            "upper_price": <float>,
            "lower_price": <float>,
            "grid_count": <int>,
            "duration_hours": <float>, 
            "reason": "<string, max 50 words>"
        }}
    """

    # --- Pydantic v1 配置写法 (区别于 v2 的 SettingsConfigDict) ---
    class Config:
        env_file = ENV_PATH
        env_file_encoding = "utf-8"
        extra = "ignore"  # 忽略多余的环境变量

    @classmethod
    def load(cls) -> "Settings":
        # 1. 尝试从 YAML 加载策略参数
        yaml_data = {}
        if os.path.exists(YAML_PATH):
            try:
                with open(YAML_PATH, "r", encoding="utf-8") as f:
                    yaml_data = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"⚠️ YAML 加载失败，将使用默认值: {e}")

        # 2. 实例化 (环境变量会自动填充根目录的字段)
        return cls(
            common=CommonConfig(**yaml_data.get("common", {})),
            strategies=StrategiesConfig(**yaml_data.get("strategies", {})),
            services=ServicesConfig(**yaml_data.get("services", {}))
        )

    def get_trade_qty(self, symbol: str) -> float:
        """辅助方法：获取交易数量"""
        return self.common.trade_quantities.get(symbol, self.common.trade_quantities.get("DEFAULT", 0.0001))


# 全局单例
try:
    settings = Settings.load()
except Exception as e:
    print(f"❌ Nado Config 加载严重错误: {e}")
    # 允许空启动，避免直接崩溃
    settings = Settings()