"""
General-purpose Celery tasks.

WebCrawler Pipeline:
- tasks_WebCrawler_real_estate.py
- tasks_WebCrawler_cpi.py
- tasks_WebCrawler_hpi.py
"""
# 從 worker.py 匯入 Celery app 實例
# 所有的 task 都要透過 app 來註冊, 才能被 Celery worker 識別
from asyncio.log import logger
from datetime import datetime, timezone
from crawler.worker import app


# @app.task() 是 Celery 的裝飾器 (decorator)
# 有了這個裝飾器, 普通的 Python 函式就會變成「可派送的任務」
# 沒有裝飾的函式只能在本地呼叫, 無法透過 RabbitMQ 派送
@app.task(name="crawler.tasks.health_check")
def health_check():
    """確認 Celery Worker 可以正常執行任務。"""

    result = {
        "status": "success",
        "task": "health_check",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(f"Celery health check: {result}")

    return result


@app.task(name="crawler.tasks.echo")
def echo(message: str):
    """測試 Celery 任務的參數傳遞與回傳值。"""

    logger.info(f"Received message: {message}")

    return {
        "status": "success",
        "message": message,
    }
