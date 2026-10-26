# 房價改變、現金購買力也改變
# nstatdb = 國民/國家統計資料庫 = National Statistics Database
# DGBAS = 行政院主計總處 = Directorate-General of Budget, Accounting and Statistics
# nstatdb.dgbas.gov.tw = 行政院主計總處官方統計資料系統
# 主計總處 CPI (DGBAS CPI): 衡量一般家庭購買商品與服務的價格水準變動，屬於價格水準指數，須以季度平均.mean()代表該季度價格水準
# 2026 實際房價 → HPI 推估未來「名目」房價 → 2036 預估名目房價 → 設定頭期款比例 → 2036 名目頭期款需求 → 與 ETF 10 年後的「名目資產」比較 → Funding Gap
# 這份正式 CPI 季資料的基期：民國105年（2016年）= 100 → Real HPI = HPI_2016_base100 / CPI_2016_base100 x 100%

# ============================================================
# CPI ETL Pipeline
# Source: DGBAS National Statistics Database
# Output: Quarterly CPI (period, cpi_index)
# ============================================================

import requests
import pandas as pd

from pathlib import Path
from datetime import datetime


# ============================================================
# 0. Configuration
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent

PROCESSED_DIR = PROJECT_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PARQUET_FILE = PROCESSED_DIR / "cpi.parquet"
OUTPUT_CSV_FILE = PROCESSED_DIR / "cpi.csv"

BASE_YEAR = 2016

print("Project directory:", PROJECT_DIR)
print("Processed directory:", PROCESSED_DIR)


# ============================================================
# 1. Build DGBAS CPI API URL
# ============================================================

def build_api_url() -> str:

    current_date = datetime.now()

    current_year = current_date.year
    current_month = current_date.month

    return (
        "https://nstatdb.dgbas.gov.tw/"
        "dgbasall/webMain.aspx?"
        "sdmx/a030101015/1+2+29+36+52...M."
        f"&startTime={BASE_YEAR}-M1"
        f"&endTime={current_year}-M{current_month}")


# ============================================================
# 2. Fetch CPI data from DGBAS API
# ============================================================

def fetch_cpi_api(url: str) -> dict:

    response = requests.get(url, timeout=60)

    print("HTTP Status:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))

    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "").lower()

    if "json" not in content_type:
        raise ValueError("DGBAS API 無回傳 JSON，請檢查 API URL 和回應內容。")

    return response.json()


# ============================================================
# 3. Parse DGBAS API JSON and extract Total CPI Index
# ============================================================

def parse_cpi_json(data: dict) -> pd.DataFrame:

    try:
        structure = data["data"]["structure"]
        datasets = data["data"]["dataSets"]

        observation_dimensions = (structure["dimensions"]["observation"])

        series_dimensions = structure["dimensions"]["series"]

    except (KeyError, TypeError) as exc:
        raise ValueError("DGBAS API JSON 結構與預期不符。") from exc

    if not observation_dimensions or not series_dimensions:
        raise ValueError("API 缺少必要資料維度。")

    # --------------------------------------------------------
    # Get monthly periods
    # --------------------------------------------------------

    periods = [
        item["id"]
        for item in observation_dimensions[0]["values"]]

    # --------------------------------------------------------
    # Find Total CPI Index
    # --------------------------------------------------------

    series_values = series_dimensions[0]["values"]

    total_index = next((item 
                        for item in series_values
                        if item["id"] == "1"),None)

    if total_index is None:
        raise ValueError("找不到 CPI「總指數」對應的 series。")

    total_series_key = str(series_values.index(total_index))

    # --------------------------------------------------------
    # Get observations
    # --------------------------------------------------------

    try:
        observations = (datasets[0]["series"][total_series_key]["observations"])

    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("找不到 CPI 總指數的 observations。") from exc

    records = []

    for observation_index, period in enumerate(periods):

        observation = observations.get(str(observation_index))

        if observation is None or not observation:
            raise ValueError(f"找不到 {period} 的 CPI observation。")

        records.append({"period_raw": period, 
                        "cpi_index": observation[0]})

    cpi = pd.DataFrame(records)

    if cpi.empty:
        raise ValueError("API 沒有回傳 CPI 資料。")

    return cpi


# ============================================================
# 4. Clean monthly CPI
# ============================================================

def clean_cpi(cpi_data: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate monthly CPI data, then rebase to 2016=100."""

    cleaned_cpi = cpi_data.copy()

    required_columns = {"period_raw", "cpi_index"}

    if not required_columns.issubset(cleaned_cpi.columns):
        raise ValueError(f"CPI 缺少必要欄位：{required_columns}")

    if cleaned_cpi.empty:
        raise ValueError("原始 CPI 資料為空。")

    # 1. Validate monthly period format
    period_text = cleaned_cpi["period_raw"].astype(str)

    valid_period = period_text.str.fullmatch(r"\d{4}-M\d{1,2}")

    if not valid_period.all():
        invalid_periods = cleaned_cpi.loc[
            ~valid_period, "period_raw"].tolist()
        raise ValueError(f"CPI period 格式異常：{invalid_periods}")

    # 2. Convert period to datetime
    year_month = (period_text
                  .str.replace("-M", "-", regex=False)
                  .str.split("-", expand=True))

    cleaned_cpi["date"] = pd.to_datetime(
        year_month[0] + "-" + year_month[1].str.zfill(2),
        format="%Y-%m", errors="raise")

    # 3. Check duplicate months
    if cleaned_cpi["date"].duplicated().any():
        duplicates = cleaned_cpi.loc[
            cleaned_cpi["date"].duplicated(keep=False),
            "period_raw"].tolist()
        raise ValueError(f"CPI 存在重複月份：{duplicates}")

    # 4. Convert CPI to numeric
    cleaned_cpi["cpi_index"] = pd.to_numeric(
        cleaned_cpi["cpi_index"], errors="coerce")

    if cleaned_cpi["cpi_index"].isna().any():
        raise ValueError("CPI 指數包含缺失值或非數值資料。")

    # 5. Handle non-positive CPI values
    invalid_mask = cleaned_cpi["cpi_index"] <= 0

    if invalid_mask.any():
        invalid_data = cleaned_cpi.loc[
            invalid_mask, ["period_raw", "date", "cpi_index"]].copy()

        invalid_data["month"] = invalid_data["date"].dt.to_period("M")

        # 以執行當下的月份為基準
        current_month = pd.Timestamp.today().to_period("M")

        # 僅將當月或未來月份的非正數視為「可能尚未發布」
        unpublished_mask = invalid_data["month"] >= current_month

        unpublished_data = invalid_data.loc[unpublished_mask]
        historical_invalid_data = invalid_data.loc[~unpublished_mask]

        # 歷史月份非正數不能直接忽略
        if not historical_invalid_data.empty:
            invalid_periods = historical_invalid_data["period_raw"].tolist()
            raise ValueError(f"歷史 CPI 資料包含非正數：{invalid_periods}")

        # 排除可能尚未發布的月份
        if not unpublished_data.empty:
            unpublished_periods = unpublished_data["period_raw"].tolist()

            print(f"Warning: 排除當月或未來月份的非正數 CPI：{unpublished_periods}")

            cleaned_cpi = cleaned_cpi.loc[~cleaned_cpi["period_raw"]
                                          .isin(unpublished_periods)].copy()

    if cleaned_cpi.empty:
        raise ValueError("排除無效 CPI 資料後，無可用資料。")

    # 6. Rebase CPI to 2016 annual average = 100
    base_year_data = cleaned_cpi.loc[
        cleaned_cpi["date"].dt.year == BASE_YEAR, "cpi_index"]

    if len(base_year_data) != 12:
        raise ValueError(
            f"{BASE_YEAR} 年 CPI 資料應有 12 個月，"
            f"實際取得 {len(base_year_data)} 個月。")

    base_2016_mean = base_year_data.mean()

    if pd.isna(base_2016_mean) or base_2016_mean <= 0:
        raise ValueError("無法計算有效的 2016 年 CPI 基期。")

    cleaned_cpi.loc[:, "cpi_index"] = (
        cleaned_cpi["cpi_index"] / base_2016_mean * 100)

    return cleaned_cpi.sort_values("date").reset_index(drop=True)


# ============================================================
# 5. Convert monthly CPI to quarterly period
# ============================================================

def transform_cpi(cpi_data: pd.DataFrame) -> pd.DataFrame:
    """Convert monthly dates to quarterly periods."""

    cpi_data = cpi_data.copy()

    if "date" not in cpi_data.columns:
        raise ValueError("缺少 date 欄位，請先執行 clean_cpi()。")

    cpi_data["period"] = (
        cpi_data["date"]
        .dt.to_period("Q")
        .astype(str))

    return cpi_data


# ============================================================
# 6. Aggregate monthly CPI to quarterly CPI
# ============================================================

def aggregate_quarterly(
    cpi: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate quarterly CPI using the mean of three months.

    Complete quarters: retain.
    Incomplete quarters: exclude and print warning.
    """

    quarterly = (
        cpi.groupby("period")
        .agg(
            cpi_index=("cpi_index", "mean"),
            month_count=("cpi_index", "count")).reset_index())

    incomplete = quarterly[quarterly["month_count"] != 3]

    if not incomplete.empty:
        print(
            "警告：以下季度的 CPI 月資料不完整，"
            "將排除：")
        print(incomplete["period"].tolist())

    quarterly = quarterly[quarterly["month_count"] == 3].copy()

    quarterly = quarterly.drop(columns=["month_count"])

    quarterly = quarterly.sort_values("period").reset_index(drop=True)

    if quarterly.empty:
        raise ValueError("沒有任何完整季度，無法產生季度 CPI。")

    return quarterly


# ============================================================
# 7. Validate quarterly CPI
# ============================================================

def validate_quarterly_cpi(
    cpi: pd.DataFrame,) -> pd.DataFrame:
    """Validate final quarterly CPI dataset."""

    cpi = cpi.copy()

    required_columns = {"period", "cpi_index"}

    if not required_columns.issubset(cpi.columns):
        raise ValueError(f"季度 CPI 缺少必要欄位：{required_columns}")

    # Check empty dataset
    if cpi.empty:
        raise ValueError("CPI 資料為空。")

    # Check missing values
    if cpi[["period", "cpi_index"]].isna().any().any():
        raise ValueError("CPI 存在缺失值。")

    # Check period format
    valid_period = cpi["period"].astype(str).str.fullmatch(
        r"\d{4}Q[1-4]")

    if not valid_period.all():
        raise ValueError("CPI period 格式錯誤，應為 YYYYQn。")

    # Check duplicate quarters
    if cpi["period"].duplicated().any():
        duplicates = cpi.loc[
            cpi["period"].duplicated(keep=False),
            "period"].tolist()

        raise ValueError(f"CPI 存在重複季度：{duplicates}")

    # Check positive CPI values
    if (cpi["cpi_index"] <= 0).any():
        raise ValueError("CPI 指數必須大於 0。")

    # Sort chronologically
    cpi = cpi.sort_values("period").reset_index(drop=True)

    # Check quarterly continuity
    quarter_periods = pd.PeriodIndex(
        cpi["period"],
        freq="Q")

    expected_periods = pd.period_range(
        start=quarter_periods.min(),
        end=quarter_periods.max(),
        freq="Q")

    if not quarter_periods.equals(expected_periods):
        actual = set(quarter_periods)
        expected = set(expected_periods)

        missing_periods = sorted(
            str(period)
            for period in expected - actual)

        raise ValueError(f"CPI 存在缺漏季度：{missing_periods}")

    return cpi[["period", "cpi_index"]]


# ============================================================
# 8. Save quarterly CPI to Parquet and CSV
# ============================================================

def save_cpi(cpi: pd.DataFrame) -> None:
    """Save quarterly CPI data to Parquet and CSV."""

    PROCESSED_DIR.mkdir(
        parents=True,
        exist_ok=True)

    # Export Parquet
    cpi.to_parquet(
        OUTPUT_PARQUET_FILE,
        index=False)

    print(f"CPI Parquet saved to: {OUTPUT_PARQUET_FILE}")

    # Export CSV
    cpi.to_csv(
        OUTPUT_CSV_FILE,
        index=False,
        encoding="utf-8-sig")

    print(f"CPI CSV saved to: {OUTPUT_CSV_FILE}")


# ============================================================
# 9. Execute the complete CPI ETL pipeline
# ============================================================

def run_cpi() -> pd.DataFrame:

    print("\n=== Step 1: Fetch CPI ===")

    url = build_api_url()
    raw_data = fetch_cpi_api(url)

    print("\n=== Step 2: Parse CPI ===")

    cpi = parse_cpi_json(raw_data)

    print("\n=== Step 3: Clean CPI ===")

    cpi = clean_cpi(cpi)

    print("\n=== Step 4: Transform CPI ===")

    cpi = transform_cpi(cpi)

    print("\n=== Step 5: Aggregate Quarterly CPI ===")

    cpi = aggregate_quarterly(cpi)

    print("\n=== Step 6: Validate Quarterly CPI ===")

    cpi = validate_quarterly_cpi(cpi)

    print("\n=== Step 7: Save CPI ===")

    save_cpi(cpi)

    print("\nCPI ETL pipeline completed successfully.")

    return cpi


# ============================================================
# 10. Main
# ============================================================

if __name__ == "__main__":
    # run_cpi() 回傳處理完成的 DataFrame
    cpi = run_cpi()

    print("\n=== Quarterly CPI Preview ===")
    print(cpi.head(20))

    print("\n=== Data Types ===")
    print(cpi.dtypes)

    print("\n=== DataFrame Info ===")
    cpi.info()

    # --------------------------------------------------------
    # Read back Parquet
    # --------------------------------------------------------

    cpi_parquet = pd.read_parquet(OUTPUT_PARQUET_FILE)

    print("\n=== Parquet Preview ===")
    print(cpi_parquet.head())

    # --------------------------------------------------------
    # Read back CSV
    # --------------------------------------------------------

    cpi_csv = pd.read_csv(
        OUTPUT_CSV_FILE,
        encoding="utf-8-sig")

    print("\n=== CSV Preview ===")
    print(cpi_csv.head())

    # --------------------------------------------------------
    # Verify exports
    # --------------------------------------------------------

    expected_columns = ["period", "cpi_index"]

    assert list(cpi_parquet.columns) == expected_columns
    assert list(cpi_csv.columns) == expected_columns

    assert len(cpi_parquet) == len(cpi)
    assert len(cpi_csv) == len(cpi)

    pd.testing.assert_frame_equal(
        cpi_parquet,
        cpi,
        check_dtype=False)

    pd.testing.assert_frame_equal(
        cpi_csv,
        cpi,
        check_dtype=False,
        check_exact=False,
        rtol=1e-9,
        atol=1e-9)

    print("\nParquet and CSV export verification passed.")
