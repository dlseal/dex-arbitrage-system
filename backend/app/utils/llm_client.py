# backend/app/utils/llm_client.py
import json
import logging
import re
import requests
import asyncio
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from app.config import settings
except ImportError:
    import sys
    # 兼容 Nado 模式下的劫持配置
    if 'app.config' in sys.modules:
        settings = sys.modules['app.config'].settings
    else:
        raise

logger = logging.getLogger("LLMClient")


class LLMClient:
    def __init__(self):
        self.api_key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else ""
        self.base_url = settings.services.llm.base_url
        self.model = settings.services.llm.model
        self.timeout = 120

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
    def consult(self, prompt: str) -> Dict[str, Any]:
        if not self.api_key:
            return {}

        logger.info(f"📤 [LLM Prompt] {prompt[:200]}... (len={len(prompt)})")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        system_content = (
            "You are a professional crypto trading assistant. "
            "You output ONLY valid JSON. "
            "Keep the 'reason' field concise (under 50 words)."
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_content},
                {"role": "user", "content": prompt}
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
            "max_tokens": 4096,
            "stream": False
        }

        try:
            url = f"{self.base_url}/chat/completions"
            response = requests.post(url, json=payload, headers=headers, timeout=self.timeout)

            if response.status_code != 200:
                raise ValueError(f"API Error [{response.status_code}]: {response.text}")

            resp_json = response.json()
            # 兼容不同的 API 响应结构
            if "choices" in resp_json and len(resp_json["choices"]) > 0:
                raw_content = resp_json["choices"][0]["message"]["content"]
            else:
                raw_content = json.dumps(resp_json)

            logger.info(f"📥 [LLM Response] {raw_content[:100]}...")

            clean_json = self._clean_and_extract_json(raw_content)
            if not clean_json:
                logger.error(f"❌ JSON Parse Failed. Full Raw Content:\n{raw_content}")
                return {}

            return clean_json

        except Exception as e:
            logger.warning(f"⚠️ LLM Error (will retry): {e}")
            raise e

    def _clean_and_extract_json(self, content: str) -> Optional[Dict[str, Any]]:
        try:
            content = content.strip()
            # 尝试直接解析
            try:
                return json.loads(content)
            except:
                pass

            # 去除 Markdown 代码块标记
            if "```" in content:
                # 移除 ```json 和 结尾的 ```
                content = re.sub(r'```json\s*', '', content)
                content = re.sub(r'```\s*$', '', content)
                content = content.strip()

            # 简单的 JSON 补全尝试 (应对 max_tokens 截断)
            if content and not content.endswith("}"):
                if content.endswith('"'):
                    content += '}'
                elif content.endswith(','):
                    content = content[:-1] + '}'
                else:
                    content += '}'

            return json.loads(content)
        except Exception as e:
            logger.debug(f"JSON extract error: {e}")
            return None


async def fetch_grid_advice(symbol: str, current_price: float, current_params: Dict[str, Any],
                            status_str: str = "ACTIVE") -> Dict[str, Any]:
    """
    异步获取网格策略建议
    """
    try:
        client = LLMClient()

        # [CRITICAL FIX] 必须使用 **current_params 解包字典
        # 否则 prompt 模板中的 {atr}, {rsi} 等占位符无法被替换，导致 KeyError
        prompt = settings.llm_prompt_template.format(
            symbol=symbol,
            price=current_price,
            current_status=status_str,
            current_params=current_params,  # 对应模板中的 {current_params} 占位符
            **current_params                # 对应模板中的 {atr}, {rsi}, {recent_candles} 等占位符
        )

        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, lambda: client.consult(prompt))
        return response

    except KeyError as e:
        logger.error(f"❌ Fetch Advice Template Error: Missing key {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Fetch Advice Final Failure: {e}")
        return {}