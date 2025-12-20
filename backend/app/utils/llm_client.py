# backend/app/utils/llm_client.py
import json
import logging
import re
import requests
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

# 适配 Nado 环境的配置读取
try:
    from app.config import settings
except ImportError:
    # 回退机制，防止因劫持导致的导入问题
    import sys

    if 'app.config' in sys.modules:
        settings = sys.modules['app.config'].settings
    else:
        raise

logger = logging.getLogger("LLMClient")


class LLMClient:
    def __init__(self):
        self.api_key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else ""
        # 兼容 services.llm.base_url 写法
        self.base_url = settings.services.llm.base_url
        self.model = settings.services.llm.model
        self.timeout = 30

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def consult(self, prompt: str) -> Dict[str, Any]:
        """
        发送 Prompt 给 LLM 并获取结构化 JSON 响应
        """
        if not self.api_key:
            logger.warning("⚠️ 未配置 LLM API Key，跳过咨询")
            return {}

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # DeepSeek 建议的 payload 结构
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You are a professional crypto trading assistant. You output ONLY JSON."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,  # 降低随机性，保证 JSON 格式稳定
            "max_tokens": 1024,
            "stream": False
        }

        try:
            # 1. 发起请求
            url = f"{self.base_url}/chat/completions"
            response = requests.post(url, json=payload, headers=headers, timeout=self.timeout)

            # 2. 检查 HTTP 状态码
            if response.status_code != 200:
                logger.error(f"❌ LLM API Error [{response.status_code}]: {response.text}")
                return {}

            # 3. 解析原始响应
            resp_json = response.json()

            # 兼容 OpenAI 格式
            if "choices" in resp_json and len(resp_json["choices"]) > 0:
                raw_content = resp_json["choices"][0]["message"]["content"]
            else:
                logger.error(f"❌ LLM 返回结构异常: {resp_json.keys()}")
                return {}

            # 4. 清洗与提取 JSON
            clean_json = self._clean_and_extract_json(raw_content)

            if not clean_json:
                logger.error(f"❌ 无法从 LLM 响应中提取 JSON. Raw: {raw_content[:100]}...")
                return {}

            return clean_json

        except Exception as e:
            logger.error(f"❌ LLM Request Failed: {e}")
            return {}

    def _clean_and_extract_json(self, content: str) -> Optional[Dict[str, Any]]:
        """
        增强型 JSON 解析器：处理 Markdown、嵌套和格式错误
        """
        try:
            # 步骤 A: 移除 Markdown 代码块 (```json ... ```)
            content = content.strip()
            if "```" in content:
                # 正则匹配 ```json {...} ``` 或 ``` {...} ```
                pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    content = match.group(1)
                else:
                    # 简单的移除尝试
                    content = content.replace("```json", "").replace("```", "").strip()

            # 步骤 B: 尝试直接解析
            data = json.loads(content)

            # 步骤 C: 处理嵌套的 {'response': ...} 情况 (这是你报错的主要原因)
            if isinstance(data, dict):
                # 如果包含 'response' 键，且它是唯一的主键，尝试解包
                if "response" in data:
                    inner = data["response"]
                    # 如果 inner 还是字符串，说明是二次编码的 JSON
                    if isinstance(inner, str):
                        try:
                            return json.loads(inner)
                        except:
                            return data  # 解析失败，保留原样
                    elif isinstance(inner, dict):
                        return inner
                    else:
                        # 可能是 {'response': 'UPDATE'} 这种简单结构
                        pass

                # 如果包含 'message' 和 'status'，这通常是 API 的错误信息被当成了数据
                if "message" in data and "status" in data:
                    logger.error(f"⚠️ 捕获到 API 错误体作为响应: {data}")
                    return None

            return data

        except json.JSONDecodeError:
            # 最后的尝试：使用正则提取最外层的 {}
            try:
                match = re.search(r"(\{.*\})", content, re.DOTALL)
                if match:
                    return json.loads(match.group(1))
            except:
                pass

            logger.error(f"JSON 解析失败. Content: {content[:50]}...")
            return None