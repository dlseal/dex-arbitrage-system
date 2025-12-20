import logging
import time
from typing import Dict


class ThrottledLogger:
    """
    节流日志记录器
    确保相同的 log_key 在 interval 秒内只输出一次。
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._last_log_times: Dict[str, float] = {}

    def log(self, level: int, key: str, msg: str, interval: float = 10.0):
        now = time.time()
        last_time = self._last_log_times.get(key, 0)

        if now - last_time >= interval:
            self.logger.log(level, msg)
            self._last_log_times[key] = now

    def info(self, key: str, msg: str, interval: float = 10.0):
        self.log(logging.INFO, key, msg, interval)

    def warning(self, key: str, msg: str, interval: float = 10.0):
        self.log(logging.WARNING, key, msg, interval)

    def error(self, key: str, msg: str, interval: float = 10.0):
        self.log(logging.ERROR, key, msg, interval)