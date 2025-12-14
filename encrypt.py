from cryptography.fernet import Fernet

# 生成密钥（妥善保存，不要存入代码库）
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# 加密你的私钥
cipher_text = cipher_suite.encrypt(b"你的明文私钥")
print(f"MASTER_KEY: {key.decode()}")
print(f"ENCRYPTED_NADO_KEY: {cipher_text.decode()}")