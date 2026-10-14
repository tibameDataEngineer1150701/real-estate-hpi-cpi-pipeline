# 從 worker.py 匯入 Celery app 實例
# tasks.py 定義要交給 Celery worker 執行的任務
# 所有的 task 都要透過 app 來註冊, 才能被 Celery worker 識別
from crawler.worker import app

from loguru import logger

# run_cpi() 執行爬蟲，tasks.py 把爬蟲包裝成 Celery 任務
from crawler.WebCrawler_cpi import (
    run_cpi,
    OUTPUT_PARQUET_FILE,
    OUTPUT_CSV_FILE)

# @app.task() 是 Celery 的裝飾器 (decorator)
# 有了這個裝飾器, 普通的 Python 函式就會變成「可派送的任務」
# 沒有裝飾的函式只能在本地呼叫, 無法透過 RabbitMQ 派送
@app.task(name="crawler.tasks_cpi.run_cpi_pipeline")
def run_cpi_pipeline():
    """Execute the quarterly CPI ETL pipeline."""

    logger.info("Starting CPI ETL pipeline")

    try:
        # Reuse the existing CPI ETL function.
        cpi_df = run_cpi()

        # Return a JSON-serializable summary, not a DataFrame.
        result = {
            "status": "success",
            "rows": len(cpi_df),
            "start_period": str(cpi_df["period"].min()),
            "end_period": str(cpi_df["period"].max()),
            "parquet_file": str(OUTPUT_PARQUET_FILE),
            "csv_file": str(OUTPUT_CSV_FILE),
        }

        logger.success(f"CPI ETL pipeline completed: {result}")

        return result

    except Exception:
        logger.exception("CPI ETL pipeline failed")
        raise
