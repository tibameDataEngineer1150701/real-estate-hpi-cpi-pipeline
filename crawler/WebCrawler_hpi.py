
"""
抓取內政部官方住宅價格指數（HPI），整理為季度資料。

輸出：
- processed/housing_price_index.parquet
- processed/housing_price_index.csv
"""

import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------
# 1. 設定
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

PROCESSED_DIR = PROJECT_DIR / "processed"

OUTPUT_PARQUET_FILE = (PROCESSED_DIR / "housing_price_index.parquet")
OUTPUT_CSV_FILE = (PROCESSED_DIR / "housing_price_index.csv")

URL = "https://pip.moi.gov.tw/Publicize/Info/E1060"

# 直接擷取官方已發布的 HPI，不在爬蟲內重設基期
VERIFY_SSL = False

print("Project directory:", PROJECT_DIR)
print("Processed directory:", PROCESSED_DIR)

EXPECTED_REGIONS = {
    "全國",
    "新北市",
    "臺北市",
    "桃園市",
    "臺中市",
    "臺南市",
    "高雄市"}


# --------------------------------------------------
# 2. Extract：取得 HTML
# --------------------------------------------------

def fetch_hpi_html(url=URL):
    response = requests.get(
        url,
        timeout=60,
        verify=VERIFY_SSL)
    response.raise_for_status()

    print("HTTP Status:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))
    print("HTML length:", len(response.text))

    return response.text


# --------------------------------------------------
# 3. Transform：民國季度轉換為西元季度
# --------------------------------------------------

def roc_quarter_to_period(value):
    value = str(value).strip()

    match = re.fullmatch(r"(\d+)年第([1-4])季", value)

    if not match:
        raise ValueError(f"無法解析季度：{value}")

    roc_year = int(match.group(1))
    quarter = match.group(2)

    return f"{roc_year + 1911}Q{quarter}"


# --------------------------------------------------
# 4. Transform + Validate + Export
# --------------------------------------------------

def run_hpi():

    # Extract
    html = fetch_hpi_html()

    tables = pd.read_html(StringIO(html))
    print(f"{len(tables)} tables found.")

    # 不固定使用 tables[1]，改以必要欄位尋找目標表格
    required_columns = {"縣市"} | EXPECTED_REGIONS
    hpi_raw = None

    for table in tables:
        table.columns = [
            str(column).strip()
            for column in table.columns]

        if required_columns.issubset(set(table.columns)):
            hpi_raw = table.copy()
            break

    if hpi_raw is None:
        raise ValueError(
            "找不到住宅價格指數歷史資料表，"
            "請檢查官方網站表格欄位是否變更。")

    # Transform：欄位名稱標準化
    hpi_raw = hpi_raw.rename(columns={"縣市": "period"})

    # Transform：民國季度轉換為西元季度
    hpi_raw["period"] = (hpi_raw["period"].apply(roc_quarter_to_period))

    # Transform：Wide format → Long format
    hpi_long = hpi_raw.melt(
        id_vars=["period"],
        value_vars=sorted(EXPECTED_REGIONS),
        var_name="region",
        value_name="housing_price_index")

    # Transform：轉為數值
    hpi_long["housing_price_index"] = pd.to_numeric(
        hpi_long["housing_price_index"],
        errors="coerce")

    # Validate：地區
    if set(hpi_long["region"].unique()) != EXPECTED_REGIONS:
        raise ValueError("HPI region validation failed")

    # Validate：缺漏值
    if hpi_long["housing_price_index"].isna().any():
        raise ValueError("HPI contains missing or invalid values")

    if hpi_long["period"].isna().any():
        raise ValueError("HPI contains missing periods")

    # Validate：季度格式
    if not hpi_long["period"].str.fullmatch(r"\d{4}Q[1-4]").all():
        raise ValueError("Invalid period format")

    # Validate：不可有重複的季度與地區組合
    if hpi_long.duplicated(["period", "region"]).any():
        raise ValueError("Duplicate period-region combinations found")

    if hpi_long.empty:
        raise ValueError("HPI contains no valid data")

    # Validate：每季必須有全國加六都，共 7 個地區
    region_count = (hpi_long.groupby("period")["region"].nunique())

    if not (region_count == len(EXPECTED_REGIONS)).all():
        raise ValueError("Each period must contain exactly 7 regions")

    # Export：建立輸出目錄
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Export：Parquet
    hpi_long.to_parquet(OUTPUT_PARQUET_FILE, index=False)
    print(f"HPI Parquet saved to: {OUTPUT_PARQUET_FILE}")

    # Export：CSV
    hpi_long.to_csv(
        OUTPUT_CSV_FILE,
        index=False,
        encoding="utf-8-sig")
    print(f"HPI CSV saved to: {OUTPUT_CSV_FILE}")

    # Validate：讀回匯出檔案
    hpi_parquet = pd.read_parquet(OUTPUT_PARQUET_FILE)
    hpi_csv = pd.read_csv(
        OUTPUT_CSV_FILE,
        encoding="utf-8-sig")

    expected_columns = [
        "period",
        "region",
        "housing_price_index"]

    if hpi_parquet.columns.tolist() != expected_columns:
        raise ValueError("Parquet columns validation failed")

    if hpi_csv.columns.tolist() != expected_columns:
        raise ValueError("CSV columns validation failed")

    if len(hpi_parquet) != len(hpi_long):
        raise ValueError("Parquet row count validation failed")

    if len(hpi_csv) != len(hpi_long):
        raise ValueError("CSV row count validation failed")

    print("\n=== HPI Preview ===")
    print(hpi_long.head(20))

    print("\n=== Row Count ===")
    print(len(hpi_long))

    print("\n=== Period Count ===")
    print(hpi_long["period"].nunique())

    print("\n=== Region Count ===")
    print(hpi_long["region"].nunique())

    # 回傳 DataFrame，供 Celery task 使用
    return hpi_long


# 只有直接執行此檔案時，才啟動爬蟲
if __name__ == "__main__":
    run_hpi()
