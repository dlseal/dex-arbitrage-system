# backend/app/utils/llm_client.py
import aiohttp
import json
import logging
from app.config import settings

logger = logging.getLogger("LLM_Client")


async def fetch_grid_advice(symbol: str, current_price: float, current_params: dict = None):
    """
    调用 LLM API 获取网格策略建议
    """
    # 安全获取 API Key
    api_key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else None

    if not api_key:
        logger.error("❌ 未配置 LLM_API_KEY")
        return None

    # 1. 构造上下文描述
    status_str = "RUNNING" if current_params else "NONE"
    params_str = json.dumps(current_params) if current_params else "None"

    # 使用 settings 中的模板
    prompt = settings.llm_prompt_template.format(
        symbol=symbol,
        price=current_price,
        current_status=status_str,
        current_params=params_str
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # 兼容 OpenAI / DeepSeek 接口
    # 参数从 settings.services.llm 读取
    payload = {
        "model": settings.services.llm.model,
        "messages": [
            {"role": "system", "content": "You are a crypto trading expert. Output strictly JSON."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "response_format": {"type": "json_object"}
    }

    try:
        # 基础 URL 处理
        base = settings.services.llm.base_url.rstrip('/')
        url = f"{base}/chat/completions" if "chat/completions" not in base else base

        logger.info(f"🧠 [AI] 正在思考 {symbol} 策略 (当前: {status_str})...")

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=60) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"❌ LLM API Error {resp.status}: {text}")
                    return None

                result = await resp.json()
                content = result['choices'][0]['message']['content']

                # 清洗 Markdown
                clean_content = content.replace("```json", "").replace("```", "").strip()
                data = json.loads(clean_content)

                # 校验必要字段
                required_keys = ["action", "upper_price", "lower_price", "duration_hours"]
                if all(k in data for k in required_keys):
                    return data
                else:
                    logger.error(f"❌ LLM 返回缺少字段: {data.keys()}")
                    return None

    except Exception as e:
        logger.error(f"❌ LLM 请求异常: {e}")
        return None