import os
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()


class Config:
    # --- GRVT 配置 ---
    GRVT_API_KEY = os.getenv("GRVT_API_KEY")
    GRVT_PRIVATE_KEY = os.getenv("GRVT_PRIVATE_KEY")
    GRVT_TRADING_ACCOUNT_ID = os.getenv("GRVT_TRADING_ACCOUNT_ID")
    GRVT_ENV = os.getenv("GRVT_ENVIRONMENT", "prod")  # prod / testnet

    # --- Lighter 配置 ---
    # Lighter API Key 通常就是你的钱包地址 (Public Address)
    LIGHTER_API_KEY = os.getenv("LIGHTER_API_KEY")
    LIGHTER_PRIVATE_KEY = os.getenv("LIGHTER_PRIVATE_KEY")
    # 如果 .env 没填，默认设为 0 (通常主账户 index 是 0)
    LIGHTER_ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
    LIGHTER_API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

    @classmethod
    def validate(cls):
        """检查核心配置是否存在"""
        missing = []
        if not cls.GRVT_API_KEY: missing.append("GRVT_API_KEY")
        if not cls.GRVT_PRIVATE_KEY: missing.append("GRVT_PRIVATE_KEY")
        if not cls.GRVT_TRADING_ACCOUNT_ID: missing.append("GRVT_TRADING_ACCOUNT_ID")
        if not cls.LIGHTER_API_KEY: missing.append("LIGHTER_API_KEY")
        if not cls.LIGHTER_PRIVATE_KEY: missing.append("LIGHTER_PRIVATE_KEY")

        if missing:
            raise ValueError(f"❌ 缺少必要的环境变量: {', '.join(missing)}")
