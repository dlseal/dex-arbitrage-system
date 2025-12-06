import asyncio
import logging
import lighter
from lighter import Configuration, ApiClient


async def main():
    # 🔴 修正点：使用正确的 Lighter Mainnet API 地址
    # 根据您之前提供的代码，这个地址应该是正确的
    host_url = "https://mainnet.zklighter.elliot.ai"

    print(f"🔍 正在连接 Lighter API ({host_url})...")

    conf = Configuration(host=host_url)
    api_client = ApiClient(configuration=conf)

    try:
        orders_api = lighter.OrderApi(api_client)

        # 获取所有 Order Books
        response = await orders_api.order_books()

        print(f"\n✅ 成功获取 {len(response.order_books)} 个交易对:")
        print("=" * 60)
        print(f"{'ID':<5} | {'Symbol':<20} | {'Type'}")
        print("-" * 60)

        for ob in response.order_books:
            print(f"{ob.market_id:<5} | {ob.symbol:<20} | {ob.market_type}")

        print("=" * 60)

    except Exception as e:
        print(f"❌ 获取失败: {e}")
        print("可能是网络问题，或者需要梯子（VPN）。")
    finally:
        await api_client.close()


if __name__ == "__main__":
    asyncio.run(main())