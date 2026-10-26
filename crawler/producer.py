# Producer (生產者): 負責把任務送進 RabbitMQ 佇列
# 對應的 Consumer (消費者) 就是 worker.py 啟動的 Celery worker
# 流程: producer.py → RabbitMQ (訊息佇列) → worker 取出並執行任務

# 從 tasks.py 匯入 crawler 這個 Celery task 函式
# 注意這裡不是直接呼叫函式, 而是把它當作「任務」送出去
from crawler.tasks_WebCrawler_real_estate import run_real_estate_pipeline
from crawler.tasks_WebCrawler_hpi import run_hpi_pipeline
from crawler.tasks_WebCrawler_cpi import run_cpi_pipeline

# .delay() 是 Celery 提供的快捷方法, 用來「非同步」派送任務
# 寫 crawler.delay(x=0) 等同於 crawler.apply_async(kwargs={"x": 0})
# 呼叫後會立刻回傳, 任務會被丟到 RabbitMQ, 等 worker 拿去執行
# 如果改成 crawler(x=0) 則會在本地「同步」執行, 非分散式
if __name__ == "__main__":
    # 派送實價登錄爬蟲任務
    result_real_estate = run_real_estate_pipeline.delay()

    # 派送 HPI 爬蟲任務
    result_hpi = run_hpi_pipeline.delay()

    # 派送 CPI 爬蟲任務
    result_cpi = run_cpi_pipeline.delay()

    # 印出任務 ID，以利後續追蹤
    print(f"Real Estate task ID: {result_real_estate.id}")
    print(f"HPI task ID: {result_hpi.id}")
    print(f"CPI task ID: {result_cpi.id}")
