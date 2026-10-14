# 從 worker.py 匯入 Celery app 實例
# tasks.py 定義要交給 Celery worker 執行的任務
# 所有的 task 都要透過 app 來註冊, 才能被 Celery worker 識別
from crawler.worker import app

from loguru import logger

# run_pipeline() 執行 ETL，tasks.py 把 ETL 包裝成 Celery 任務
from crawler.WebCrawler_real_estate import run_pipeline

# @app.task() 是 Celery 的裝飾器 (decorator)
# 有了這個裝飾器, 普通的 Python 函式就會變成「可派送的任務」
# 沒有裝飾的函式只能在本地呼叫, 無法透過 RabbitMQ 派送
@app.task(name="crawler.tasks.run_real_estate_pipeline")
def run_real_estate_pipeline(
    start_year=105,
    start_quarter=1,
    end_year=None,
    end_quarter=None,
    sleep_seconds=1,
):
    """Celery task：執行實價登錄歷史資料 ETL。"""

    logger.info(
        "Starting real estate pipeline: "
        f"{start_year}S{start_quarter}")

    result = run_pipeline(
        start_year=start_year,
        start_quarter=start_quarter,
        end_year=end_year,
        end_quarter=end_quarter,
        sleep_seconds=sleep_seconds,
    )

    logger.info(
        f"Pipeline finished: "
        f"{result['succeeded']}/{result['total']} succeeded")

    if result["failed"]:
        logger.warning(f"Failed quarters: {result['failed']}")

    return result
