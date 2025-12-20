# backend/app/config_nado.py
import os
import yaml
from typing import List, Dict, Optional
from pydantic import BaseModel, Field, SecretStr, BaseSettings


# 注意：这里使用的是 v1 的 BaseSettings，不依赖 pydantic-settings

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


class AiGridConfig(BaseModel):
    exchange: str = "Nado"
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
    ai_grid: AiGridConfig = Field(default_factory=AiGridConfig)


class ServicesConfig(BaseModel):
    class LlmConfig(BaseModel):
        base_url: str = "https://api.deepseek.com"
        model: str = "deepseek-reasoner"

    llm: LlmConfig = Field(default_factory=LlmConfig)


# ==========================================
# 2. 定义配置加载器
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENV_PATH = os.path.join(BASE_DIR, ".env")
YAML_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")


class Settings(BaseSettings):
    # Secrets
    nado_private_key: Optional[SecretStr] = None
    nado_mode: str = "MAINNET"
    nado_subaccount_name: str = "default"
    encrypted_nado_key: Optional[SecretStr] = None
    master_key: Optional[SecretStr] = None
    llm_api_key: Optional[SecretStr] = None

    # Configs
    common: CommonConfig = Field(default_factory=CommonConfig)
    strategies: StrategiesConfig = Field(default_factory=StrategiesConfig)
    services: ServicesConfig = Field(default_factory=ServicesConfig)

    llm_prompt_template: str = "..."  # 省略长文本

    class Config:
        env_file = ENV_PATH
        env_file_encoding = "utf-8"
        extra = "ignore"  # 忽略其他交易所的配置项，避免报错

    @classmethod
    def load(cls) -> "Settings":
        yaml_data = {}
        if os.path.exists(YAML_PATH):
            with open(YAML_PATH, "r", encoding="utf-8") as f:
                yaml_data = yaml.safe_load(f) or {}

        return cls(
            common=CommonConfig(**yaml_data.get("common", {})),
            strategies=StrategiesConfig(**yaml_data.get("strategies", {})),
            services=ServicesConfig(**yaml_data.get("services", {}))
        )


# 单例
try:
    settings = Settings.load()
except Exception as e:
    print(f"⚠️ Nado Config Load Warning: {e}")
    settings = Settings()