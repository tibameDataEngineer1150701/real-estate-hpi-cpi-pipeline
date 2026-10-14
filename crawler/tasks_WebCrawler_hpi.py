# 從 worker.py 匯入 Celery app 實例
# tasks.py 定義要交給 Celery worker 執行的任務
# 所有的 task 都要透過 app 來註冊, 才能被 Celery worker 識別
from crawler.worker import app

from loguru import logger

# run_hpi() 執行爬蟲，tasks.py 把爬蟲包裝成 Celery 任務
from crawler.worker import app
from crawler.WebCrawler_hpi import (
    run_hpi,
    OUTPUT_PARQUET_FILE,
    OUTPUT_CSV_FILE)

# @app.task() 是 Celery 的裝飾器 (decorator)
# 有了這個裝飾器, 普通的 Python 函式就會變成「可派送的任務」
# 沒有裝飾的函式只能在本地呼叫, 無法透過 RabbitMQ 派送
@app.task(name="crawler.tasks_hpi.run_hpi_pipeline")
def run_hpi_pipeline():
    """執行官方住宅價格指數（HPI）爬蟲。"""

    logger.info("Starting HPI pipeline")

    try:
        # 呼叫已經測試成功的 HPI 爬蟲
        hpi_df = run_hpi()

        # Celery 回傳 JSON 相容的摘要，不直接回傳 DataFrame
        result = {
            "status": "success",
            "rows": len(hpi_df),
            "period_count": int(hpi_df["period"].nunique()),
            "region_count": int(hpi_df["region"].nunique()),
            "parquet_file": str(OUTPUT_PARQUET_FILE),
            "csv_file": str(OUTPUT_CSV_FILE)}

        logger.success(f"HPI pipeline completed: {result}")
        # return result：回傳筆數、季度數、地區數和輸出路徑
        return result

    except Exception:
        logger.exception("HPI pipeline failed")
        raise