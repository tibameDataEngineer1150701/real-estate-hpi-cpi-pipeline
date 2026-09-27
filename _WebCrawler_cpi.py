# 房價改變、現金購買力也改變
# nstatdb = 國民/國家統計資料庫 = National Statistics Database
# DGBAS = 行政院主計總處 = Directorate-General of Budget, Accounting and Statistics
# nstatdb.dgbas.gov.tw = 行政院主計總處官方統計資料系統
# 主計總處 CPI (DGBAS CPI): 衡量一般家庭購買商品與服務的價格水準變動，屬於價格水準指數，須以季度平均.mean()代表該季度價格水準
# 2026 實際房價 → HPI 推估未來「名目」房價 → 2036 預估名目房價 → 設定頭期款比例 → 2036 名目頭期款需求 → 與 ETF 10 年後的「名目資產」比較 → Funding Gap
# 這份正式 CPI 季資料的基期：民國105年（2016年）= 100 → Real HPI = HPI_2016_base100 / CPI_2016_base100 x 100%

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# ============================================================
# Configuration
# ============================================================

PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = PROCESSED_DIR / "cpi.parquet"  ## 直接輸出.parquet + CPI.parquet -> Real HPI.parquet

# API_URL = (
#     "https://nstatdb.dgbas.gov.tw/"
#     "dgbasall/webMain.aspx?"
#     "sdmx/a030101015/1+2+29+36+52...M."
#     "&startTime=2021-M1"
#     "&endTime=2026-M12")

def build_api_url():
    current_date = datetime.now()

    current_year = current_date.year
    current_month = current_date.month

    return (
        "https://nstatdb.dgbas.gov.tw/"
        "dgbasall/webMain.aspx?"
        "sdmx/a030101015/1+2+29+36+52...M."
        f"&startTime=2016-M1"
        f"&endTime={current_year}-M{current_month}")


# ============================================================
# 1. Fetch CPI data from DGBAS API
# API 已按照資料結構將資料整理整齊，且不須下載及解析整個 HTML 文件，Python 不需先理解整個網頁的 HTML 結構，因此執行速度較快
# ============================================================

def fetch_cpi_api(url: str) -> dict:
    """Fetch CPI data from DGBAS SDMX API."""

    response = requests.get(url, timeout=60)

    print("HTTP Status:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))

    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "").lower()

    if "json" not in content_type:
        raise ValueError("DGBAS API 沒有回傳 JSON。")

    return response.json()


# ============================================================
# 2. Parse API JSON
# ============================================================

def parse_cpi_json(data: dict) -> pd.DataFrame:
    """
    Parse DGBAS API-JSON and extract Total CPI Index.
    """

    structure = data["data"]["structure"]

    # --------------------------------------------------------
    # Get observation periods
    # --------------------------------------------------------

    observation_dimensions = structure["dimensions"]["observation"]

    periods = [item["id"] for item in observation_dimensions[0]["values"]]

    # --------------------------------------------------------
    # Find Total CPI Index
    # --------------------------------------------------------

    series_values = structure["dimensions"]["series"][0]["values"]

    total_index = next((item for item in series_values if item["id"] == "1"), None)

    if total_index is None:
        raise ValueError("找不到 CPI「總指數」對應的 series。")

    total_series_key = str(series_values.index(total_index))

    # --------------------------------------------------------
    # Get observations
    # --------------------------------------------------------

    observations = (data["data"]["dataSets"][0]["series"][total_series_key]["observations"])

    records = []

    for observation_index, period in enumerate(periods):

        observation = observations.get(str(observation_index))

        if observation is None:
            raise ValueError(f"找不到 {period} 的 CPI observation。")

        records.append({
                "period_raw": period,
                "cpi_index": observation[0],})

    cpi = pd.DataFrame(records)

    return cpi


# ============================================================
# 3. Clean CPI
# ============================================================

def clean_cpi(cpi: pd.DataFrame) -> pd.DataFrame:
    """Clean monthly CPI data."""

    cpi = cpi.copy()

    # Convert CPI to numeric
    cpi["cpi_index"] = pd.to_numeric(cpi["cpi_index"], errors="coerce")

    # --------------------------------------------------------
    # DGBAS API returns 0 for unpublished observations.
    # Treat 0 as missing rather than a valid CPI value.
    # --------------------------------------------------------

    unpublished = cpi["cpi_index"] == 0

    if unpublished.any():
        unpublished_periods = cpi.loc[unpublished, "period_raw"].tolist()

        print("以下月份 CPI 尚未公布，API 回傳 0，將排除：")
        print(unpublished_periods)

        cpi = cpi.loc[~unpublished].copy()

    # --------------------------------------------------------
    # Validate missing values
    # --------------------------------------------------------

    if cpi["cpi_index"].isna().any():
        raise ValueError("CPI 資料包含缺失值。")

    # --------------------------------------------------------
    # Validate period format
    # --------------------------------------------------------

    if not cpi["period_raw"].astype(str).str.fullmatch(r"\d{4}-M\d{1,2}").all():

        invalid_periods = cpi.loc[~cpi["period_raw"].astype(str).str.fullmatch(r"\d{4}-M\d{1,2}"),"period_raw"].tolist()

        raise ValueError(f"CPI period 格式異常：{invalid_periods}")

    return cpi


# ============================================================
# 4. Convert monthly period to quarter
# ============================================================

def transform_cpi(cpi: pd.DataFrame) -> pd.DataFrame:
    """Convert monthly CPI period to quarterly period."""

    cpi = cpi.copy()

    # Convert 2024-M1 → 2024-01
    cpi["date"] = pd.to_datetime(
        cpi["period_raw"].str.replace(
            "-M",
            "-",
            regex=False), format="%Y-%m")

    # Convert monthly date → quarterly period
    cpi["period"] = (
        cpi["date"]
        .dt.to_period("Q")
        .astype(str)
        .str.replace(
            "Q",
            "Q",
            regex=False))

    return cpi


# ============================================================
# 5. Aggregate monthly CPI to quarterly CPI (完整季度 → 保留；不完整季度 → 排除並 print warning)
# ============================================================

def aggregate_quarterly(cpi: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate quarterly CPI as the mean of three monthly CPI values.

    Incomplete quarters are excluded because the latest month
    may not have been published yet.
    """

    quarterly = (cpi.groupby("period")
        .agg(cpi_index=("cpi_index", "mean"),
             month_count=("cpi_index", "count")).reset_index())

    # --------------------------------------------------------
    # Exclude incomplete quarters
    # --------------------------------------------------------

    incomplete = quarterly[quarterly["month_count"] != 3]

    if not incomplete.empty:
        invalid_periods = incomplete["period"].tolist()

        print(
            "警告：以下季度 CPI 月資料不完整，"
            "將不納入季度資料：")
        print(invalid_periods)

    quarterly = quarterly[quarterly["month_count"] == 3].copy()

    quarterly = quarterly.drop(columns="month_count")

    return quarterly


# ============================================================
# 6. Validate quarterly CPI
# ============================================================

def validate_quarterly_cpi(cpi: pd.DataFrame) -> pd.DataFrame:
    """Validate final quarterly CPI dataset."""

    # Check empty periods
    if cpi.empty:
        raise ValueError("CPI 資料為空。")

    # Check duplicate periods
    if cpi["period"].duplicated().any():
        raise ValueError("CPI 存在重複季度。")

    # Check missing values
    if cpi["cpi_index"].isna().any():
        raise ValueError("CPI 存在缺失值。")

    # Check period format
    if not cpi["period"].astype(str).str.fullmatch(r"\d{4}Q[1-4]").all():
        raise ValueError("CPI period 格式錯誤，應為 YYYYQn。")

    # Check positive CPI values
    if (cpi["cpi_index"] <= 0).any():
        raise ValueError("CPI 應為正數。")

    return cpi


# ============================================================
# 7. Save CPI
# ============================================================

def save_cpi(cpi: pd.DataFrame) -> None:
    """Save quarterly CPI data to Parquet."""

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    cpi.to_parquet(OUTPUT_FILE, index=False)

    print(f"CPI 已儲存至：{OUTPUT_FILE}")


# ============================================================
# 8. Main CPI pipeline
# ============================================================

def run_cpi() -> pd.DataFrame:
    """Run complete CPI ETL pipeline."""

    print("開始取得 DGBAS CPI 資料...")

    api_url = build_api_url()

    # Extract
    data = fetch_cpi_api(api_url)

    # Parse
    cpi = parse_cpi_json(data)

    print(f"取得 {len(cpi)} 筆月資料。")

    # Transform
    cpi = clean_cpi(cpi)
    cpi = transform_cpi(cpi)
    cpi = aggregate_quarterly(cpi)

    # Validate
    cpi = validate_quarterly_cpi(cpi)

    # Keep final columns
    cpi = cpi[["period","cpi_index"]]

    # Save
    save_cpi(cpi)

    print(f"完成 CPI ETL，共 {len(cpi)} 筆季度資料。")

    return cpi

# Read back and inspect CPI.parquet
hpi = pd.read_parquet("data/processed/cpi.parquet")

print(hpi.head(20))
print(hpi.info())
print(hpi.columns)

# ============================================================
# Run directly
# ============================================================

if __name__ == "__main__":
    cpi = run_cpi()

    print("\nCPI DataFrame:")
    print(cpi)

    print("\n資料型態:")
    print(cpi.dtypes)