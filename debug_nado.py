import sys
import os

print(f"🐍 Python Executable: {sys.executable}")
print(f"📂 Running in: {os.getcwd()}")

print("-" * 50)
print("尝试导入 nado_protocol...")

try:
    # 1. 先检查依赖包是否正常
    import pydantic

    print(f"✅ Pydantic version: {pydantic.VERSION}")

    import web3

    print(f"✅ Web3 version: {web3.__version__}")

    # 2. 尝试导入核心包（这里是崩溃的高发区）
    import nado_protocol

    print("✅ 成功导入 nado_protocol！")
    print(f"📦 Nado Protocol location: {nado_protocol.__file__}")

except Exception as e:
    print("\n❌ 导入严重失败！真正的错误堆栈如下：")
    print("=" * 50)
    import traceback

    traceback.print_exc()
    print("=" * 50)