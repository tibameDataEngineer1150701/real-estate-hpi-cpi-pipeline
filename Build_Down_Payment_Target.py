
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger


# ============================================================
# 1. Project Configuration and Parameters
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = PROJECT_DIR / "crawler" / "processed"
OUTPUT_DIR = PROJECT_DIR / "processed"

HPI_FILE = PROCESSED_DIR / "housing_price_index.parquet"
TRANSACTION_FILE = (PROCESSED_DIR / "residential_transactions_quarterly.parquet")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 購屋假設
PROPERTY_AREA_PING = 30
PING_TO_SQM = 3.3058
DOWN_PAYMENT_RATIO = 0.20
FORECAST_YEARS = 10

# 樂觀情境調整幅度：暫定值，需由團隊確認
OPTIMISTIC_ADJUSTMENT = 0.02

# 六都名稱必須與兩份資料的地區欄位一致
MUNICIPALITIES = [
    "臺北市",
    "新北市",
    "桃園市",
    "臺中市",
    "臺南市",
    "高雄市"]


# ============================================================
# 2. Load Source Data
# ============================================================

def load_data():
    """載入 HPI 與住宅交易資料。"""

    if not HPI_FILE.exists():
        raise FileNotFoundError(f"找不到 HPI 檔案：{HPI_FILE}")

    if not TRANSACTION_FILE.exists():
        raise FileNotFoundError(f"找不到住宅交易檔案：{TRANSACTION_FILE}")

    hpi = pd.read_parquet(HPI_FILE)
    transactions = pd.read_parquet(TRANSACTION_FILE)

    logger.info(
        f"HPI：{len(hpi)} rows，"
        f"住宅交易：{len(transactions)} rows")

    return hpi, transactions


# ============================================================
# 3. Validate Source Data
# ============================================================

def validate_source_data(hpi, transactions):
    """檢查必要欄位、資料型態與六都資料完整性。"""

    required_hpi = {
        "period",
        "region",
        "housing_price_index"}

    required_transactions = {
        "period",
        "municipality",
        "median_unit_price",
        "transaction_type"}

    # Check "transaction_type"
    valid_types = {"成屋", "預售屋"}

    actual_types = set(transactions["transaction_type"].dropna().unique())

    if not actual_types.issubset(valid_types):
        raise ValueError(f"發現未預期的交易類型：{actual_types - valid_types}")

    if actual_types != valid_types:
        raise ValueError(f"交易類型不完整，目前只有：{actual_types}")

    missing_hpi = required_hpi - set(hpi.columns)
    missing_transactions = (required_transactions - set(transactions.columns))

    if missing_hpi:
        raise ValueError(f"HPI 缺少欄位：{missing_hpi}")

    if missing_transactions:
        raise ValueError(f"住宅交易資料缺少欄位：{missing_transactions}")

    if hpi.empty or transactions.empty:
        raise ValueError("來源資料不可為空")

    hpi["housing_price_index"] = pd.to_numeric(
        hpi["housing_price_index"],
        errors="coerce")

    transactions["median_unit_price"] = pd.to_numeric(
        transactions["median_unit_price"],
        errors="coerce")

    if (hpi["housing_price_index"].isna().any()
        or (hpi["housing_price_index"] <= 0).any()):
        raise ValueError("HPI 存在空值或非正數")

    if (transactions["median_unit_price"].isna().any()
        or (transactions["median_unit_price"] <= 0).any()):
        raise ValueError("住宅交易單價存在空值或非正數")

    hpi_regions = set(hpi["region"].unique())
    transaction_regions = set(transactions["municipality"].unique())

    missing_hpi_regions = set(MUNICIPALITIES) - hpi_regions
    missing_transaction_regions = (set(MUNICIPALITIES) - transaction_regions)

    if missing_hpi_regions:
        raise ValueError(f"HPI 缺少六都資料：{missing_hpi_regions}")

    if missing_transaction_regions:
        raise ValueError(f"住宅交易資料缺少六都資料："f"{missing_transaction_regions}")

    return hpi, transactions


# ============================================================
# 4. Calculate HPI CAGR
# ============================================================

def calculate_hpi_cagr(
    hpi: pd.DataFrame,
    years: int) -> pd.DataFrame:
    """
    計算各地區近 N 年 HPI CAGR

    季資料：
    5 年 = 20 季
    10 年 = 40 季
    """

    df = hpi.copy()

    df["quarter"] = pd.PeriodIndex(
        df["period"],
        freq="Q")

    df = df.sort_values(["region", "quarter"])

    quarter_interval = years * 4

    previous = df[["region", "quarter", "housing_price_index"]].copy()

    previous["quarter"] = (previous["quarter"] + quarter_interval)

    previous = previous.rename(columns={"housing_price_index": "previous_hpi"})

    merged = df.merge(
        previous,
        on=["region", "quarter"],
        how="left")

    merged["hpi_growth"] = (
        merged["housing_price_index"]
        / merged["previous_hpi"]) ** (1 / years) - 1

    latest = (
        merged.sort_values("quarter")
        .groupby("region")
        .tail(1)
        [["region", "quarter", "hpi_growth"]]
        .copy())

    latest = latest.rename(columns={"quarter": "growth_period"})

    latest["growth_period"] = (latest["growth_period"].astype(str))

    latest["hpi_growth"] = latest["hpi_growth"].replace(
        [np.inf, -np.inf],
        np.nan)

    if latest["hpi_growth"].isna().any():
        missing = latest.loc[
            latest["hpi_growth"].isna(),
            "region"].tolist()

        raise ValueError(f"以下地區缺少近 {years} 年 HPI 成長率：{missing}")

    return latest


# ============================================================
# 5. Calculate Current House Price Baseline
# ============================================================

def calculate_house_price_base(transactions: pd.DataFrame) -> pd.DataFrame:
    """依六都及成屋/預售屋分別計算目前房價基準。"""

    df = transactions.copy()

    df["quarter"] = pd.PeriodIndex(
        df["period"],
        freq="Q")

    latest_period = (
        df.groupby(
            ["municipality", "transaction_type"]
        )["quarter"]
        .max()
        .rename("latest_quarter")
        .reset_index())

    df = df.merge(
        latest_period,
        on=["municipality", "transaction_type"],
        how="inner")

    df = df[df["quarter"] == df["latest_quarter"]]

    house_price = (
        df.groupby(["municipality", "transaction_type"])
        .agg(
            house_price_period=("quarter", "first"),
            median_unit_price=("median_unit_price", "median"),
        ).reset_index())

    house_price["house_price"] = (
        house_price["median_unit_price"]
        * PROPERTY_AREA_PING
        * PING_TO_SQM)

    house_price = house_price.rename(columns={"municipality": "region"})

    house_price["house_price_period"] = (house_price["house_price_period"].astype(str))

    return house_price[[
            "house_price_period",
            "region",
            "transaction_type",
            "median_unit_price",
            "house_price"]]


# ============================================================
# 6. Build Growth Scenarios
# ============================================================

def build_scenarios(
    hpi_5y: pd.DataFrame,
    hpi_10y: pd.DataFrame) -> pd.DataFrame:
    """建立保守、中性、樂觀三種成長率情境。"""

    conservative = hpi_5y[["region", "growth_period", "hpi_growth"]].copy()

    conservative["scenario"] = "保守"

    neutral = hpi_10y[["region", "growth_period", "hpi_growth"]].copy()

    neutral["scenario"] = "中性"

    optimistic = hpi_10y[["region", "growth_period", "hpi_growth"]].copy()

    optimistic["hpi_growth"] = (optimistic["hpi_growth"] + OPTIMISTIC_ADJUSTMENT)

    optimistic["scenario"] = "樂觀"

    scenarios = pd.concat([
        conservative,
        neutral,
        optimistic], ignore_index=True)

    return scenarios


# ============================================================
# 7. Calculate Future House Prices and Down Payment Targets
# ============================================================

def calculate_down_payment_target(
    house_price: pd.DataFrame,
    scenarios: pd.DataFrame) -> pd.DataFrame:
    """計算六都 × 交易類型 × 三種情境。"""

    result = house_price.merge(
        scenarios,
        on="region",
        how="inner")

    result["forecast_years"] = FORECAST_YEARS
    result["down_payment_ratio"] = DOWN_PAYMENT_RATIO

    result["future_house_price"] = (
        result["house_price"]
        * (1 + result["hpi_growth"])
        ** result["forecast_years"])

    result["down_payment_target"] = (
        result["future_house_price"]
        * result["down_payment_ratio"])

    return result[[
            "house_price_period",
            "growth_period",
            "region",
            "transaction_type",
            "scenario",
            "house_price",
            "hpi_growth",
            "forecast_years",
            "down_payment_ratio",
            "future_house_price",
            "down_payment_target"]]


# ============================================================
# 8. Validate Results
# ============================================================

def validate_results(result: pd.DataFrame):
    """驗證 18 列、無空值、無重複資料及公式正確性。"""

    expected_rows = len(MUNICIPALITIES) * 3

    if len(result) != expected_rows:
        raise ValueError(f"預期 {expected_rows} 列，實際 {len(result)} 列")

    if result.isna().any().any():
        raise ValueError("結果存在空值")

    if not np.isfinite(
        result.select_dtypes(include=[np.number]).to_numpy()).all():
        raise ValueError("結果存在無限值或非有限數值")

    expected_scenarios = {"保守", "中性", "樂觀"}

    if set(result["scenario"]) != expected_scenarios:
        raise ValueError("情境種類不完整")

    if set(result["region"]) != set(MUNICIPALITIES):
        raise ValueError("六都資料不完整")

    if result.duplicated(subset=["region", "scenario"]).any():
        raise ValueError("存在重複的地區與情境組合")

    expected_future_price = (
        result["house_price"]
        * (1 + result["hpi_growth"])
        ** result["forecast_years"])

    if not np.allclose(
        result["future_house_price"],
        expected_future_price):
        raise ValueError("未來房價公式驗證失敗")

    expected_down_payment = (
        result["future_house_price"]
        * result["down_payment_ratio"])

    if not np.allclose(
        result["down_payment_target"],
        expected_down_payment):
        raise ValueError("頭期款公式驗證失敗")

    logger.success(f"結果驗證完成：{len(result)} rows")


# ============================================================
# 9. Export Results
# ============================================================

def save_results(result: pd.DataFrame):
    """輸出 Parquet 與 CSV。"""

    parquet_path = (OUTPUT_DIR / "down_payment_target.parquet")
    csv_path = (OUTPUT_DIR / "down_payment_target.csv")

    result.to_parquet(
        parquet_path,
        index=False)

    result.to_csv(
        csv_path,
        index=False,
        encoding="utf-8-sig")

    logger.success(f"已輸出：{parquet_path}")
    logger.success(f"已輸出：{csv_path}")


# ============================================================
# 10. Main
# ============================================================

def main():
    hpi, transactions = load_data()

    hpi, transactions = validate_source_data(
        hpi,
        transactions)

    logger.info("計算近 5 年 HPI CAGR")
    hpi_5y = calculate_hpi_cagr(
        hpi,
        years=5)

    logger.info("計算近 10 年 HPI CAGR")
    hpi_10y = calculate_hpi_cagr(
        hpi,
        years=10)

    logger.info("計算目前房價基準")
    house_price = calculate_house_price_base(transactions)

    logger.info("建立三種成長率情境")
    scenarios = build_scenarios(
        hpi_5y,
        hpi_10y)

    logger.info("計算未來房價與頭期款")
    result = calculate_down_payment_target(
        house_price,
        scenarios)

    logger.info("驗證結果")
    validate_results(result)

    save_results(result)

    logger.success("Down Payment Target pipeline completed")

    print("\nDown Payment Target Preview:")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
