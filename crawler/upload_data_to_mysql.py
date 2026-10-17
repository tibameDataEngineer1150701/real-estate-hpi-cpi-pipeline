# 匯入必要套件
from pathlib import Path

import pandas as pd  # 用來處理資料表（DataFrame）
from loguru import logger  # 日誌工具，用來輸出 log 訊息
from sqlalchemy import URL, create_engine, text  # 建立資料庫連線的工具（SQLAlchemy）

from crawler.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_ACCOUNT,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

# 專案根目錄
PROJECT_DIR = Path(__file__).resolve().parent.parent

# 資料輸出位置一致
PROCESSED_DIR = PROJECT_DIR / "processed"

# 只列出已確認的資料檔案
DATASETS = {
    "housing_txn_qtr": PROCESSED_DIR / "residential_transactions_quarterly.csv",
    "housing_price_index": PROCESSED_DIR / "housing_price_index.csv",
    "cpi": PROCESSED_DIR / "cpi.csv",
    "hpi": PROCESSED_DIR / "real_hpi.csv"
}


def create_db_engine():
    """使用 crawler.config 的設定建立 MySQL 連線
    MYSQL_ACCOUNT：使用現有設定中的帳號名稱
    MYSQL_PASSWORD：沿用 config.py 從環境變數或 local.ini 取得的密碼
    MYSQL_HOST、MYSQL_PORT、MYSQL_DATABASE：由 config.py 管理。
    """
    url = URL.create(
        drivername="mysql+pymysql",
        username=MYSQL_ACCOUNT,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def upload_csv(engine, table_name: str, file_path: Path):
    """讀取 CSV 並匯入指定的 MySQL 資料表。"""
    if not file_path.exists():
        raise FileNotFoundError(f"找不到資料檔案：{file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError(f"資料檔案沒有資料：{file_path}")

    if df.columns.duplicated().any():
        raise ValueError(f"存在重複欄位：{file_path}")

    logger.info(
        f"準備匯入 {table_name}："
        f"{len(df)} rows，{len(df.columns)} columns"
    )

    # append：新增資料，不主動覆蓋整張表
    # 注意：append 本身不會防止重複資料
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )

    logger.success(
        f"{table_name} 匯入完成，共 {len(df)} rows"
    )


def main():
    engine = create_db_engine()

    try:
        # 先確認資料庫可以連線
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.success("MySQL connection successful")

        # 逐一匯入資料表
        for table_name, file_path in DATASETS.items():
            upload_csv(engine, table_name, file_path)

    except Exception:
        logger.exception("MySQL data upload failed")
        raise

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
