import asyncio
import logging
import os
from dotenv import load_dotenv
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_env import GrvtEnv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugGRVT")


async def main():
    api_key = os.getenv("GRVT_API_KEY")
    # 注意：如果连接不上，很有可能是网络问题
    print(f"🔍 正在尝试连接 GRVT (Prod)...")
    print(f"🔑 API Key: {api_key[:5]}******" if api_key else "❌ 未找到 API Key")

    # 模拟适配器中的参数
    ws_params = {
        'api_key': api_key,
        'trading_account_id': os.getenv("GRVT_TRADING_ACCOUNT_ID"),
        'api_ws_version': 'v1',
        'private_key': os.getenv("GRVT_PRIVATE_KEY")
    }

    try:
        # 初始化客户端
        client = GrvtCcxtWS(
            env=GrvtEnv.PROD,  # 确保这里和你 .env 里的一致
            loop=asyncio.get_running_loop(),
            logger=logger,
            parameters=ws_params
        )

        print("⏳ 正在执行 initialize()... (这步最容易超时)")
        await client.initialize()
        print("✅ GRVT WebSocket 连接成功！")

        # 尝试订阅一个公共频道测试
        print("📡 尝试订阅 BTC-USDT-Perpetual...")
        # 注意：这里需要填写真实的 instrument_id，你可以先填一个不存在的看是否报错，或者填真实的
        # 如果 initialize 过了，说明网络通了

        await asyncio.sleep(5)
        print("🛑 测试结束，关闭连接")
        # 由于 SDK 封装较深，直接退出即可

    except Exception as e:
        print("\n" + "=" * 50)
        print(f"❌ 连接失败: {e}")
        print("=" * 50)
        print("💡 建议：")
        print("1. 检查 VPN 是否开启了【TUN模式】或【全局代理】")
        print("2. 你的 Python 无法访问 GRVT 服务器 (TimeoutError)")
        print("3. 检查防火墙设置")


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())