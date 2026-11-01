
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger


# ============================================================
# 1. Project Configuration and Parameters
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = PROJECT_DIR / "processed"
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
    """Validate required columns, data types, and regional coverage."""

    required_hpi = {
        "period",
        "region",
        "housing_price_index"}

    required_transactions = {
        "period",
        "municipality",
        "median_unit_price",
        "transaction_type",
        "data_status",
        "transaction_count"}

    # Check required columns
    missing_hpi = required_hpi - set(hpi.columns)
    missing_transactions = required_transactions - set(transactions.columns)

    if missing_hpi:
        raise ValueError(f"Missing HPI columns: {missing_hpi}")

    if missing_transactions:
        raise ValueError(f"Missing transaction columns: {missing_transactions}")

    # Check empty datasets
    if hpi.empty or transactions.empty:
        raise ValueError("Source data cannot be empty")

    # Validate transaction data status
    if transactions["data_status"].isna().any():
        raise ValueError("Transaction data_status contains missing values")

    valid_status = {
        "available",
        "no_eligible_transactions"}

    if not transactions["data_status"].isin(valid_status).all():
        raise ValueError("Unexpected transaction data_status")

    transactions = transactions[
        transactions["data_status"] == "available"].copy()

    if transactions.empty:
        raise ValueError("No eligible transaction records available")

    # Validate transaction count
    transactions["transaction_count"] = pd.to_numeric(
        transactions["transaction_count"],
        errors="coerce")

    if (transactions["transaction_count"].isna().any()
        or not np.isfinite(
            transactions["transaction_count"].to_numpy()).all()
        or (transactions["transaction_count"] <= 0).any()
        or (transactions["transaction_count"] % 1 != 0).any()):
        raise ValueError("Available transactions must have positive integer transaction_count")

    # Check missing period and region values
    if hpi[["period", "region"]].isna().any().any():
        raise ValueError("HPI period or region contains missing values")

    if hpi["region"].astype("string").str.strip().eq("").any():
        raise ValueError("HPI region contains empty strings")

    if hpi["region"].ne(
        hpi["region"].astype("string").str.strip()).any():
        raise ValueError("HPI region contains leading or trailing spaces")

    if transactions[["period", "municipality"]].isna().any().any():
        raise ValueError("Transaction period or municipality contains missing values")

    if transactions["municipality"].astype("string").str.strip().eq("").any():
        raise ValueError("Transaction municipality contains empty strings")

    if transactions["municipality"].ne(
        transactions["municipality"].astype("string").str.strip()).any():
        raise ValueError("Transaction municipality contains leading or trailing spaces")

    # Validate quarter formats
    try:
        hpi = hpi.copy()
        transactions = transactions.copy()

        hpi["period"] = pd.PeriodIndex(hpi["period"], freq="Q")
        transactions["period"] = pd.PeriodIndex(
            transactions["period"],
            freq="Q")

    except (ValueError, TypeError) as exc:
        raise ValueError("Period 欄位必須是有效的季度格式，例如 2024Q1") from exc

    # Validate transaction types
    valid_types = {"成屋買賣", "預售屋買賣"}

    if transactions["transaction_type"].isna().any():
        raise ValueError("Transaction type contains missing values")

    if transactions["transaction_type"].astype("string").str.strip().eq("").any():
        raise ValueError("Transaction type contains empty strings")

    if transactions["transaction_type"].ne(
        transactions["transaction_type"].astype("string").str.strip()).any():
        raise ValueError("Transaction type contains leading or trailing spaces")

    actual_types = set(transactions["transaction_type"].unique())

    if actual_types != valid_types:
        raise ValueError(f"Transaction types incomplete or unexpected: {actual_types}")

    # Validate numeric columns
    hpi["housing_price_index"] = pd.to_numeric(
        hpi["housing_price_index"],
        errors="coerce")

    transactions["median_unit_price"] = pd.to_numeric(
        transactions["median_unit_price"],
        errors="coerce")

    if (hpi["housing_price_index"].isna().any()
        or not np.isfinite(hpi["housing_price_index"].to_numpy()).all()
        or (hpi["housing_price_index"] <= 0).any()):
        raise ValueError("HPI contains missing or invalid values")

    if (transactions["median_unit_price"].isna().any()
        or not np.isfinite(
            transactions["median_unit_price"].to_numpy()).all()
        or (transactions["median_unit_price"] <= 0).any()):
        raise ValueError("Transaction prices contain missing or invalid values")

    # Check duplicate records
    if hpi.duplicated(subset=["period", "region"]).any():
        raise ValueError("Duplicate HPI records found for the same period and region")

    if transactions.duplicated(
        subset=["period", "municipality", "transaction_type"]).any():
        raise ValueError(
            "Duplicate transaction records found for the same period, "
            "municipality, and transaction type")

    # Check regional coverage
    hpi_regions = set(hpi["region"].unique())
    transaction_regions = set(transactions["municipality"].unique())

    missing_hpi_regions = set(MUNICIPALITIES) - hpi_regions
    missing_transaction_regions = (set(MUNICIPALITIES) - transaction_regions)

    if missing_hpi_regions:
        raise ValueError(f"Missing HPI regions: {missing_hpi_regions}")

    if missing_transaction_regions:
        raise ValueError(f"Missing transaction regions: {missing_transaction_regions}")

    # Check transaction types for each municipality
    for municipality in MUNICIPALITIES:
        municipality_types = set(
            transactions.loc[
                transactions["municipality"] == municipality,
                "transaction_type"].unique())

        if municipality_types != valid_types:
            raise ValueError(
                f"{municipality} transaction types incomplete: "
                f"{municipality_types}")

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
    # CAGR 只計算六都，符合預設條件
    df = hpi[hpi["region"].isin(MUNICIPALITIES)].copy()

    # Python 中 bool 是 int 的子類別
    if (isinstance(years, (bool, np.bool_))
        or not isinstance(years, (int, np.integer))
        or years <= 0):
        raise ValueError("years must be a positive integer")

    df["quarter"] = pd.PeriodIndex(
        df["period"],
        freq="Q")

    df = df.sort_values(["region", "quarter"])

    quarter_interval = years * 4

    previous = df[["region", "quarter", "housing_price_index"]].copy()

    previous["quarter"] = (previous["quarter"] + quarter_interval)

    previous = previous.rename(columns={"housing_price_index": "previous_hpi"})

    # Check duplicate region-quarter keys
    if df.duplicated(subset=["region", "quarter"]).any():
        raise ValueError("HPI 存在重複的地區與季度組合")

    for region, group in df.groupby("region"):
        available_quarters = set(group["quarter"])

        latest_quarter = group["quarter"].max()
        required_previous_quarter = latest_quarter - quarter_interval

        if required_previous_quarter not in available_quarters:
            raise ValueError(
                f"{region} 缺少計算近 {years} 年 CAGR "
                f"所需的基準季度：{required_previous_quarter}")

        # 檢查基準季度至最新季度之間是否缺少任何季度
        expected_quarters = pd.period_range(
            start=required_previous_quarter,
            end=latest_quarter,
            freq="Q")

        missing_quarters = set(expected_quarters) - available_quarters

        if missing_quarters:
            raise ValueError(
                f"{region} 近 {years} 年 HPI 資料缺少季度："
                f"{sorted(missing_quarters)}")
        
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
            house_price_period=("latest_quarter", "first"),
            median_unit_price=("median_unit_price", "median")).reset_index())

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
    """Validate 36 rows, missing values, duplicate keys, and formulas."""

    expected_rows = len(MUNICIPALITIES) * 2 * 3

    if result.empty:
        raise ValueError("Result dataset cannot be empty")

    if len(result) != expected_rows:
        raise ValueError(f"預期 {expected_rows} 列，實際 {len(result)} 列")

    required_columns = {
        "house_price_period",
        "growth_period",
        "region",
        "transaction_type",
        "scenario",
        "hpi_growth",
        "house_price",
        "forecast_years",
        "down_payment_ratio",
        "future_house_price",
        "down_payment_target"}

    missing_columns = required_columns - set(result.columns)

    if missing_columns:
        raise ValueError(f"結果缺少必要欄位：{missing_columns}")

    if (result["hpi_growth"] <= -1).any():
        raise ValueError("HPI growth rate must be greater than -100%")

    if result.isna().any().any():
        raise ValueError("結果存在空值")

    # 驗證數值欄位沒有 NaN 或正負無限大
    numeric_columns = result.select_dtypes(include=[np.number]).columns

    for column in numeric_columns:
        values = pd.to_numeric(result[column], 
                               errors="coerce").to_numpy(dtype="float64", na_value=np.nan)

        if not np.isfinite(values).all():
            raise ValueError(f"結果欄位 {column} 存在 NaN 或無限大數值")

    # Check forecast parameters
    if not (result["forecast_years"] == FORECAST_YEARS).all():
        raise ValueError("預測年數與設定值不一致")

    if not np.allclose(result["down_payment_ratio"], DOWN_PAYMENT_RATIO):
        raise ValueError("頭期款比例與設定值不一致")

    expected_scenarios = {"保守", "中性", "樂觀"}

    if set(result["scenario"]) != expected_scenarios:
        raise ValueError("情境種類不完整")

    if set(result["region"]) != set(MUNICIPALITIES):
        raise ValueError("六都資料不完整")

    if result.duplicated(subset=["region", "transaction_type", "scenario"]).any():
        raise ValueError("存在重複的地區、交易類型與情境組合")

    expected_types = {"成屋買賣", "預售屋買賣"}

    for region in MUNICIPALITIES:
        region_data = result[result["region"] == region]

        if set(region_data["transaction_type"]) != expected_types:
            raise ValueError(f"{region} 的交易類型不完整")

        for transaction_type in expected_types:
            subset = region_data[region_data["transaction_type"] == transaction_type]

            if set(subset["scenario"]) != {"保守", "中性", "樂觀"}:
                raise ValueError(f"{region} {transaction_type} 的情境不完整")
        
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
