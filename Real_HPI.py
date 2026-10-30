# Real HPI = HPI_2016_base100 / CPI_2016_base100 * 100
#
# HPI：名目房價指數（Nominal House Price Index）
# CPI：消費者物價指數（Consumer Price Index）
# Real HPI：以 CPI 調整通膨後的實質房價指數
#
# 用途：
# 1. 排除一般物價上漲因素，觀察房價的實質變化。
# 2. 比較不同時期房價的實質購買力變化。
#
# 基期：
# HPI 與 CPI 均須使用 2016 年平均 = 100。
# 注意：執行計算前，必須確認原始 HPI 的基期定義。
#
# Example -- 2016Q1:
# HPI_2016_base100 = 99.32
# CPI_2016_base100 = 99.1828
# Real HPI = 99.32 / 99.1828 * 100 ≈ 100.1383

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = PROJECT_DIR / "processed"

HPI_FILE = PROCESSED_DIR / "housing_price_index.parquet"
CPI_FILE = PROCESSED_DIR / "cpi.parquet"
TRANSACTION_FILE = PROCESSED_DIR / "residential_transactions_quarterly.parquet"

OUTPUT_PARQUET_FILE = PROCESSED_DIR / "real_hpi.parquet"
OUTPUT_CSV_FILE = PROCESSED_DIR / "real_hpi.csv"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

print("Project directory:", PROJECT_DIR)
print("Processed directory:", PROCESSED_DIR)

SQUARE_METERS_PER_PING = 3.3058
DEFAULT_PROPERTY_AREA_PING = 30

# 1. Load HPI, CPI, and transaction Parquet files

def load_data():

    required_files = {
        "HPI": HPI_FILE,
        "CPI": CPI_FILE,
        "Transactions": TRANSACTION_FILE}

    for name, file_path in required_files.items():
        if not file_path.exists():
            raise FileNotFoundError(f"{name} file not found: {file_path}")

    hpi = pd.read_parquet(HPI_FILE)
    cpi = pd.read_parquet(CPI_FILE)
    transactions = pd.read_parquet(TRANSACTION_FILE)

    print("\n=== HPI Preview ===")
    print(hpi.head())

    print("\n=== CPI Preview ===")
    print(cpi.head())

    print("\n=== Transactions Columns ===")
    print(transactions.columns.tolist())

    print("\n=== Transactions Data Types ===")
    print(transactions.dtypes)

    print("\n=== Transactions Preview ===")
    print(transactions.head())

    return hpi, cpi, transactions


# 2. Validate HPI and CPI before merging
def validate_source_data(hpi: pd.DataFrame, cpi: pd.DataFrame, transactions: pd.DataFrame):

    # Required columns

    required_hpi = {"period", "region", "housing_price_index"}
    required_cpi = {"period", "cpi_index"}

    if not required_hpi.issubset(hpi.columns):
        raise ValueError(
            f"HPI missing columns: "
            f"{required_hpi - set(hpi.columns)}")

    if not required_cpi.issubset(cpi.columns):
        raise ValueError(
            f"CPI missing columns: "
            f"{required_cpi - set(cpi.columns)}")

    hpi = hpi.copy()
    cpi = cpi.copy()
    transactions = transactions.copy()

    required_transactions = {
        "period",
        "municipality",
        "median_unit_price"}

    if not required_transactions.issubset(transactions.columns):
        raise ValueError(
            f"Transaction data missing columns: "
            f"{required_transactions - set(transactions.columns)}")

    transactions["period"] = transactions["period"].astype("string")

    transactions["municipality"] = (
        transactions["municipality"].astype("string").str.strip())

    transactions["median_unit_price"] = pd.to_numeric(
        transactions["median_unit_price"],
        errors="coerce")

    # Normalize period data types (string) and check for missing values

    hpi["period"] = hpi["period"].astype("string")
    cpi["period"] = cpi["period"].astype("string")

    if hpi["period"].isna().any():
        raise ValueError("HPI period contains missing values.")

    if cpi["period"].isna().any():
        raise ValueError("CPI period contains missing values.")

    # Validate quarterly period format (YYYYQn)

    hpi_valid = hpi["period"].str.fullmatch(r"\d{4}Q[1-4]")

    cpi_valid = cpi["period"].str.fullmatch(r"\d{4}Q[1-4]")

    if not hpi_valid.all():
        raise ValueError("HPI period format must be YYYYQn.")

    if not cpi_valid.all():
        raise ValueError("CPI period format must be YYYYQn.")

    # Validate numeric values

    hpi["housing_price_index"] = pd.to_numeric(
        hpi["housing_price_index"], errors="coerce")

    cpi["cpi_index"] = pd.to_numeric(
        cpi["cpi_index"], errors="coerce")

    if not np.isfinite(hpi["housing_price_index"]).all():
        raise ValueError("HPI 包含無限大或非有限數值。")

    if not np.isfinite(cpi["cpi_index"]).all():
        raise ValueError("CPI 包含無限大或非有限數值。")

    if not np.isfinite(transactions["median_unit_price"]).all():
        raise ValueError("Transaction median unit price contains non-finite values.")
    
    if hpi[["region", "housing_price_index"]].isna().any().any():
        raise ValueError("HPI contains missing region or index values.")

    if cpi["cpi_index"].isna().any():
        raise ValueError("CPI contains missing index values.")

    if (hpi["housing_price_index"] <= 0).any():
        raise ValueError("HPI index must be greater than zero.")

    if (cpi["cpi_index"] <= 0).any():
        raise ValueError("CPI index must be greater than zero.")

    hpi["region"] = hpi["region"].astype("string").str.strip()

    transactions["municipality"] = (transactions["municipality"].astype("string").str.strip())

    # Validate duplicate keys

    if hpi.duplicated(["period", "region"]).any():
        raise ValueError("Duplicate HPI period-region combinations found.")

    if cpi["period"].duplicated().any():
        raise ValueError("Duplicate CPI quarters found.")

    # Validate 2016 base-year CPI data and ensure it contains all four quarters

    cpi_2016 = cpi[cpi["period"].str.startswith("2016")]

    expected_2016_periods = {
        "2016Q1",
        "2016Q2",
        "2016Q3",
        "2016Q4"}

    actual_2016_periods = set(cpi_2016["period"])

    if actual_2016_periods != expected_2016_periods:
        raise ValueError("2016 年 CPI 季資料不完整，請確認是否包含 Q1 至 Q4。")

    transactions = transactions.copy()

    required_transactions = {
        "period",
        "municipality",
        "median_unit_price"}

    if not required_transactions.issubset(transactions.columns):
        raise ValueError(
            f"Transaction data missing columns: "
            f"{required_transactions - set(transactions.columns)}")

    transactions["period"] = transactions["period"].astype("string")

    if transactions["period"].isna().any():
        raise ValueError("Transaction period contains missing values.")

    if not transactions["period"].str.fullmatch(r"\d{4}Q[1-4]").all():
        raise ValueError("Transaction period format must be YYYYQn.")

    if transactions[
        ["period", "municipality", "median_unit_price"]].isna().any().any():
        raise ValueError("Transaction data contains missing values.")

    if not np.isfinite(transactions["median_unit_price"]).all():
        raise ValueError("Transaction median unit price contains non-finite values.")

    if (transactions["median_unit_price"] <= 0).any():
        raise ValueError("Transaction median unit price must be positive.")
    if hpi["region"].isna().any() or hpi["region"].eq("").any():
        raise ValueError("HPI region contains missing or empty values.")

    if (transactions["municipality"].isna().any()
        or transactions["municipality"].eq("").any()):
        raise ValueError("Transaction municipality contains missing or empty values.")
    
    if transactions.duplicated(["period", "municipality"]).any():
        raise ValueError("Duplicate transaction period-municipality combinations found.")

    return hpi, cpi, transactions


# 3. Merge HPI and CPI for common periods

def normalize_cpi_base_year(
    cpi: pd.DataFrame, base_year: int = 2016) -> pd.DataFrame:
    """
    Normalize quarterly CPI so that the annual average
    of the base year 2016 equals to 100.
    """

    cpi = cpi.copy()

    base_periods = {
        f"{base_year}Q1",
        f"{base_year}Q2",
        f"{base_year}Q3",
        f"{base_year}Q4"}

    base_data = cpi[cpi["period"].isin(base_periods)]

    if set(base_data["period"]) != base_periods:
        raise ValueError(f"{base_year} 年 CPI 必須完整包含 Q1 至 Q4。")

    # 2016 年四季的平均 CPI
    base_average = base_data["cpi_index"].mean()

    if not np.isfinite(base_average) or base_average <= 0:
        raise ValueError("CPI 基期平均值無效。")

    # 保留原始 CPI，另外建立標準化指數
    cpi["cpi_2016_base100"] = (cpi["cpi_index"] / base_average * 100)

    print(f"\nCPI 基期年度：{base_year}")
    print(f"CPI 基期年度平均：{base_average:.6f}")
    print(
        f"CPI {base_year} 年平均標準化後："
        f"{cpi.loc[cpi['period'].isin(base_periods), 
                   'cpi_2016_base100'].mean():.6f}")

    return cpi


def merge_hpi_cpi(hpi: pd.DataFrame, cpi: pd.DataFrame) -> pd.DataFrame:
    """Merge HPI and normalized CPI over common quarters."""

    if cpi["period"].duplicated().any():
        raise ValueError("CPI 存在重複季度。")

    if "cpi_2016_base100" not in cpi.columns:
        raise ValueError("CPI 尚未完成基期標準化。")

    common_periods = (set(hpi["period"]) & set(cpi["period"]))

    if not common_periods:
        raise ValueError("HPI 與 CPI 沒有共同季度，無法計算 Real HPI。")

    hpi_filtered = hpi[hpi["period"].isin(common_periods)].copy()

    real_hpi = hpi_filtered.merge(
        cpi[["period", "cpi_index", "cpi_2016_base100"]],
        on="period",
        how="left",
        validate="many_to_one",
        indicator=True)

    if (real_hpi["_merge"] != "both").any():
        raise ValueError("HPI 與 CPI 合併後仍存在未匹配資料。")

    real_hpi = real_hpi.drop(columns=["_merge"])

    print(
        "HPI 原始期間：",
        min(hpi["period"]),
        "至",
        max(hpi["period"]))

    print(
        "Real HPI 共同期間：",
        min(real_hpi["period"]),
        "至",
        max(real_hpi["period"]))

    print("合併後資料筆數：", len(real_hpi))

    return real_hpi


# 4. Calculate Real HPI = HPI / CPI * 100

def calculate_real_hpi(data: pd.DataFrame) -> pd.DataFrame:

    real_hpi = data.copy()

    real_hpi["real_hpi"] = (
        real_hpi["housing_price_index"]
        / real_hpi["cpi_2016_base100"] * 100)

    return real_hpi


# 5. Validate final Real HPI dataset

def validate_real_hpi(real_hpi: pd.DataFrame) -> pd.DataFrame:

    if real_hpi.empty:
        raise ValueError("Real HPI contains no data.")

    required_columns = [
        "period",
        "region",
        "housing_price_index",
        "cpi_index",
        "cpi_2016_base100",
        "real_hpi"]
    
    # 1. Validate required columns
    missing_columns = set(required_columns) - set(real_hpi.columns)

    if missing_columns:
        raise ValueError(f"Real HPI missing columns: {missing_columns}")

    # 2. Validate missing values
    if real_hpi[required_columns].isna().any().any():
        raise ValueError("Real HPI contains missing values.")
    
    # 3. Validate numeric values
    numeric_columns = [
        "housing_price_index",
        "cpi_index",
        "cpi_2016_base100",
        "real_hpi"]
    
    if not np.isfinite(
        real_hpi[numeric_columns].to_numpy(dtype=float)).all():
        raise ValueError("Real HPI 包含無限大或非有限數值。")

    # 4. Validate duplicate keys
    if real_hpi.duplicated(["period", "region"]).any():
        raise ValueError("Duplicate period-region combinations found.")

    # 5. Validate positive index values
    if (real_hpi["real_hpi"] <= 0).any():
        raise ValueError("Real HPI contains invalid values.")

    # 6. Sort by period and region
    real_hpi = real_hpi.sort_values(["period", 
                                     "region"]).reset_index(drop=True)

    return real_hpi[required_columns]


# 6. Export Real HPI to Parquet and CSV

def save_real_hpi(real_hpi: pd.DataFrame) -> None:
    PROCESSED_DIR.mkdir(
        parents=True,
        exist_ok=True)

    # Export Parquet
    real_hpi.to_parquet(
        OUTPUT_PARQUET_FILE,
        index=False)

    print(
        f"Real HPI Parquet saved to: "
        f"{OUTPUT_PARQUET_FILE}")

    # Export CSV
    real_hpi.to_csv(
        OUTPUT_CSV_FILE,
        index=False,
        encoding="utf-8-sig")

    print(
        f"Real HPI CSV saved to: "
        f"{OUTPUT_CSV_FILE}")


def calculate_hpi_growth(hpi_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the 10-year annualized nominal HPI growth rate.

    Compare each quarter with the same region's HPI
    exactly 40 quarters earlier.

    If historical data is unavailable, hpi_growth_10y
    will remain NaN.
    """

    df = hpi_df.copy()

    # Convert quarterly strings into pandas Period for calculation
    df["period"] = pd.PeriodIndex(df["period"], freq="Q")

    if df.duplicated(["period", "region"]).any():
        raise ValueError("Duplicate HPI period-region combinations found.")

    current = df[["period", "region", "housing_price_index"]].copy()

    # Identify the corresponding quarter 10 years earlier
    current["period_10y_ago"] = current["period"] - 40

    historical = df[["period", "region", "housing_price_index"]].copy()

    historical = historical.rename(
        columns={
            "period": "period_10y_ago",
            "housing_price_index": "hpi_10y_ago",})

    result = current.merge(
        historical,
        on=["period_10y_ago", "region"],
        how="left",
        validate="one_to_one")

    # Calculate annualized growth rate
    result["hpi_growth_10y"] = (
        result["housing_price_index"]
        / result["hpi_10y_ago"]) ** (1 / 10) - 1

    # Remove the intermediate matching column
    result = result.drop(columns=["period_10y_ago"])

    # Convert Period back to string for downstream processing
    result["period"] = result["period"].astype(str)

    return result.sort_values(["period", "region"]).reset_index(drop=True)


# 僅擷取各縣市最新季度的交易資料計算購屋頭期款
# 使用最新季度各縣市住宅交易單價中位數估算一戶標準 30 坪住宅的總價
def calculate_house_price_base(
    transaction_df: pd.DataFrame,
    property_area_ping: float = 30) -> pd.DataFrame:
    """
    使用最新季度各縣市的住宅單價中位數，
    估算一戶 30 坪住宅的總價。

    median_unit_price 單位：元／平方公尺
    1 坪 = 3.3058 平方公尺
    """

    df = transaction_df.copy()

    required_columns = [
        "period",
        "municipality",
        "median_unit_price"]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns]

    if not np.isfinite(property_area_ping) or property_area_ping <= 0:
        raise ValueError("property_area_ping must be a positive finite number.")

    if missing_columns:
        raise ValueError(f"交易資料缺少必要欄位：{missing_columns}")

    df["median_unit_price"] = pd.to_numeric(
        df["median_unit_price"],
        errors="coerce")

    if not np.isfinite(df["median_unit_price"]).all():
        raise ValueError("median_unit_price contains non-finite values.")

    # 檢查必要欄位是否有缺值
    if df[required_columns].isna().any().any():
        raise ValueError("交易資料必要欄位存在缺值")

    # 確認單價為正數
    if (df["median_unit_price"] <= 0).any():
        raise ValueError("median_unit_price 必須大於 0")

    # 確認每個季度、縣市只有一筆資料
    if df.duplicated(subset=["period", "municipality"]).any():
        raise ValueError("交易資料存在重複的 period、municipality")

    # 找出最新季度
    latest_period = df["period"].max()

    latest_df = df[df["period"] == latest_period].copy()

    # 計算住宅總價
    latest_df["property_area_ping"] = property_area_ping

    latest_df["house_price"] = (
        latest_df["median_unit_price"]
        * property_area_ping
        * SQUARE_METERS_PER_PING)

    # 將 municipality 統一命名為 region，
    # 以便後續與 HPI 資料合併
    latest_df = latest_df.rename(columns={"municipality": "region"})

    result = latest_df[[
            "period",
            "region",
            "median_unit_price",
            "property_area_ping",
            "house_price"]].copy()

    print(f"房價估算基準季度：{latest_period}")
    print(f"房價估算資料筆數：{len(result)}")

    return result

 
# 以縣市為primary key, merge 最新交易房價與最新可用的 HPI 成長率
# 房價基準季度與 HPI 成長率基準季度不一定相同，
# 因此保留 house_price_period 與 growth_period
def calculate_down_payment_target(
    house_price_df: pd.DataFrame,
    growth_df: pd.DataFrame,
    down_payment_ratio: float = 0.20,
    forecast_years: int = 10) -> pd.DataFrame:
    """
    使用最新交易房價及各縣市最新可取得的 HPI 十年年化成長率估算未來頭期款
    """

    house_price = house_price_df.copy()
    growth = growth_df.copy()

    required_house_columns = [
        "period",
        "region",
        "house_price"]

    required_growth_columns = [
        "period",
        "region",
        "hpi_growth_10y"]
    
    for col in required_house_columns:
        if col not in house_price.columns:
            raise ValueError(f"房價資料缺少欄位：{col}")

    for col in required_growth_columns:
        if col not in growth.columns:
            raise ValueError(f"HPI 成長率資料缺少欄位：{col}")

    if (growth["hpi_growth_10y"].dropna() <= -1).any():
        raise ValueError("HPI annualized growth rate must be greater than -100%.")

    if not 0 < down_payment_ratio <= 1:
        raise ValueError("down_payment_ratio must be between 0 and 1.")

    if not isinstance(forecast_years, int) or forecast_years <= 0:
        raise ValueError("forecast_years must be a positive integer.")

    # 記錄房價基準季度
    house_price = house_price.rename(columns={"period": "house_price_period"})

    # 僅保留有效的十年成長率
    growth = growth.dropna(subset=["hpi_growth_10y"]).copy()

    # 各縣市只保留最新可用的成長率
    growth = growth.sort_values("period")

    # 取得各地區最新一筆有效的十年 HPI 成長率
    latest_growth = (
        growth.groupby("region", as_index=False)
        .tail(1)
        .rename(columns={"period": "growth_period"}))

    common_regions = (set(house_price["region"]) & set(latest_growth["region"]))

    print("Common regions:", sorted(common_regions))

    # 以地區合併，不要求兩份資料季度相同
    result = house_price.merge(
        latest_growth[[
                "region",
                "growth_period",
                "hpi_growth_10y"]],
        on="region",
        how="inner",
        validate="one_to_one")

    if result.empty:
        raise ValueError(
            "No matching transaction and HPI growth data found. "
            "請確認兩份資料的 region 名稱是否一致。")

    # 未來十年名目房價
    result["forecast_years"] = forecast_years

    result["future_house_price"] = (
        result["house_price"]
        * (1 + result["hpi_growth_10y"]) ** forecast_years)

    # 未來名目頭期款
    result["down_payment_ratio"] = down_payment_ratio

    result["down_payment_target"] = (
        result["future_house_price"]
        * down_payment_ratio)

    return result


def validate_results(df: pd.DataFrame) -> pd.DataFrame:
    """Validate down payment target results."""

    if df.empty:
        raise ValueError("Down payment target contains no data.")

    required_columns = [
        "house_price_period",
        "growth_period",
        "region",
        "house_price",
        "hpi_growth_10y",
        "future_house_price",
        "down_payment_ratio",
        "down_payment_target"]

    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(f"Down payment target missing columns: {missing_columns}")

    if df[required_columns].isna().any().any():
        raise ValueError("Down payment target contains missing values.")

    numeric_columns = [
        "house_price",
        "hpi_growth_10y",
        "future_house_price",
        "down_payment_ratio",
        "down_payment_target"]

    if not np.isfinite(
        df[numeric_columns].to_numpy(dtype=float)).all():
        raise ValueError("Down payment target contains invalid numbers.")

    if (df["house_price"] <= 0).any():
        raise ValueError("House price must be positive.")

    if (df["future_house_price"] <= 0).any():
        raise ValueError("Future house price must be positive.")

    if not df["down_payment_ratio"].between(0, 1).all():
        raise ValueError("Down payment ratio must be between 0 and 1.")

    if df.duplicated(["region"]).any():
        raise ValueError("Duplicate municipality results found.")

    if (df["hpi_growth_10y"] <= -1).any():
        raise ValueError("HPI annualized growth rate must be greater than -100%.")

    expected_down_payment = (df["future_house_price"] * df["down_payment_ratio"])

    if not np.allclose(
        df["down_payment_target"],
        expected_down_payment,
        rtol=1e-9,
        atol=1e-6):
        raise ValueError("Down payment target calculation is inconsistent.")

    return df.sort_values("region").reset_index(drop=True)


HPI_GROWTH_PARQUET = PROCESSED_DIR / "hpi_growth.parquet"
HPI_GROWTH_CSV = PROCESSED_DIR / "hpi_growth.csv"

DOWN_PAYMENT_PARQUET = PROCESSED_DIR / "down_payment_target.parquet"
DOWN_PAYMENT_CSV = PROCESSED_DIR / "down_payment_target.csv"

def save_results(hpi_growth, down_payment):

    hpi_growth.to_parquet(
        HPI_GROWTH_PARQUET,
        index=False)

    hpi_growth.to_csv(
        HPI_GROWTH_CSV,
        index=False,
        encoding="utf-8-sig")

    down_payment.to_parquet(
        DOWN_PAYMENT_PARQUET,
        index=False)

    down_payment.to_csv(
        DOWN_PAYMENT_CSV,
        index=False,
        encoding="utf-8-sig")

    print("HPI Growth and Down Payment Target exported successfully.")


# 確認每個 HPI 地區基期四季平均值：
def validate_hpi_base_year(
    hpi: pd.DataFrame,
    base_year: int = 2016,
    tolerance: float = 0.01,) -> None:
    """確認原始 HPI 是否已採用指定年度平均 = 100 的基期。"""

    base_periods = {
        f"{base_year}Q1",
        f"{base_year}Q2",
        f"{base_year}Q3",
        f"{base_year}Q4"}

    base_data = hpi[hpi["period"].isin(base_periods)].copy()

    if set(base_data["period"]) != base_periods:
        raise ValueError(f"HPI 缺少 {base_year} 年完整季度資料，無法確認基期。")
    
    # 針對每個 region，計算基期年度四季平均值
    base_average = (base_data.groupby("region")["housing_price_index"].mean())

    # 確認是否接近 100，tolerance=0.01 允許平均值與 100 相差 0.01 個指數點
    invalid_regions = base_average[
        ~np.isclose(
            base_average,
            100,
            atol=tolerance,
            rtol=0)]

    if not invalid_regions.empty:
        raise ValueError(
            f"HPI 並非 {base_year} 年平均 = 100：\n"
            f"{invalid_regions}")

    print(f"HPI 已確認：{base_year} 年平均 = 100。")

# 7. Run Real HPI pipeline

def run_real_hpi() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Execute the complete Real HPI pipeline."""

    # Step 1: Load data
    hpi, cpi, transactions = load_data()

    # Step 2: Validate source data
    hpi, cpi, transactions = validate_source_data(hpi, cpi, transactions)
    validate_hpi_base_year(hpi, base_year=2016)

    # Step 3: Normalize CPI to 2016 annual average = 100
    cpi = normalize_cpi_base_year(cpi, base_year=2016)

    # Step 4: Merge HPI and CPI over common quarters,
    # then calculate and validate Real HPI.
    merged = merge_hpi_cpi(hpi, cpi)

    real_hpi = calculate_real_hpi(merged)
    real_hpi = validate_real_hpi(real_hpi)

    # Step 5: Calculate 10-year annualized HPI growth
    # using the same region and the quarter 40 periods earlier.
    hpi_growth = calculate_hpi_growth(hpi)

    # Step 6: Calculate current house price
    house_price = calculate_house_price_base(transactions)

    # Step 7: Estimate the future nominal down payment
    # using the latest transaction price and latest available HPI growth rate.
    print("\n=== HPI Regions ===")
    print(sorted(hpi["region"].unique().tolist()))

    print("\n=== Transaction Regions ===")
    print(sorted(transactions["municipality"].unique().tolist()))

    print("\n=== House Price Regions ===")
    print(house_price["region"].unique())  # 確認縣市名稱是否一致或有混用情形

    print("\n=== HPI Growth Regions ===")
    print(hpi_growth["region"].unique())

    print("\n=== HPI Growth Preview ===")
    print(hpi_growth.head(10))  # 確認 HPI 成長率是否真的有六都資料

    print("\n=== HPI Growth Latest Period ===")
    print(hpi_growth["period"].max())

    down_payment = calculate_down_payment_target(house_price, hpi_growth)

    # Step 8: Validate and export
    down_payment = validate_results(down_payment)
    save_real_hpi(real_hpi)
    save_results(hpi_growth, down_payment)

    return real_hpi, hpi_growth, down_payment


# 8. Main

if __name__ == "__main__":

    real_hpi, hpi_growth, down_payment = run_real_hpi()

    print("\n=== Real HPI Preview ===")
    print(real_hpi.head(20))

    print("\n=== Data Types ===")
    print(real_hpi.dtypes)

    print("\n=== DataFrame Info ===")
    real_hpi.info()

    # Read back Parquet and CSV

    real_hpi_parquet = pd.read_parquet(OUTPUT_PARQUET_FILE)

    real_hpi_csv = pd.read_csv(OUTPUT_CSV_FILE, encoding="utf-8-sig")

    # Validate exports

    assert len(real_hpi_parquet) == len(real_hpi)
    assert len(real_hpi_csv) == len(real_hpi)

    assert list(real_hpi_parquet.columns) == list(real_hpi.columns)

    assert list(real_hpi_csv.columns) == list(real_hpi.columns)

    pd.testing.assert_frame_equal(
        real_hpi_parquet,
        real_hpi,
        check_dtype=False)

    pd.testing.assert_frame_equal(
        real_hpi_csv,
        real_hpi,
        check_dtype=False,
        check_exact=False,
        rtol=1e-9,
        atol=1e-9)

    print("\n=== CSV Preview ===")
    print(real_hpi_csv.head(20))

    print(
        "\nReal HPI Parquet and CSV "
        "export verification passed.")