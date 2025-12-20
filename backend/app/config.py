# backend/app/config.py
import os
import yaml
from typing import List, Dict, Optional
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


# ==========================================
# 1. 定义 YAML 结构对应的 Pydantic 模型
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
    exchange: str = "GRVT"
    upper_price: float = 100000.0
    lower_price: float = 90000.0
    grid_count: int = 10
    principal: float = 1000.0
    leverage: float = 2.0
    min_order_size: float = 15.0
    max_check_interval: int = 86400


class StrategiesConfig(BaseModel):
    active: str = "HFT_MM"
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
# 2. 定义总配置 Settings (整合 .env 和 .yaml)
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENV_PATH = os.path.join(BASE_DIR, ".env")
YAML_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")

class Settings(BaseSettings):
    # --- 环境变量 (Secrets) ---
    # 对应 .env 文件中的 KEY，Pydantic 会自动读取并转为小写属性
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

    llm_prompt_template: str = """
        你是一个加密货币高频量化交易专家。
        【当前市场状态】交易对: {symbol}, 最新价格: {price}
        【当前策略】状态: {current_status}, 参数: {current_params}
        【任务】分析行情，决定动作。
        请严格返回JSON格式：
        {{
            "action": "UPDATE" | "CONTINUE",
            "upper_price": <float>,
            "lower_price": <float>,
            "grid_count": <int>,
            "duration_hours": <float>,
            "reason": "<string>"
        }}
    """

    model_config = SettingsConfigDict(
        env_file=ENV_PATH,
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @classmethod
    def load(cls) -> "Settings":
        # 1. 尝试从 YAML 加载策略参数
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")
        yaml_data = {}
        if os.path.exists(YAML_PATH):
            with open(YAML_PATH, "r", encoding="utf-8") as f:
                yaml_data = yaml.safe_load(f) or {}

        # 2. 实例化 (环境变量会自动填充根目录的字段)
        # 将 YAML 数据注入到对应的 Pydantic 模型中
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
    print(f"❌ 配置加载严重错误: {e}")
    # 允许空启动
    settings = Settings()