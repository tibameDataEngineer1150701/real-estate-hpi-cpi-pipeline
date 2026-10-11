# Real HPI = HPI_2016_base100 / CPI_2016_base100 x 100%
# 排除通膨後觀察房價趨勢: HPI -> 名目房價指數(Nominal HPI)，Real HPI -> CPI 扣除通膨因素後的房價指數，輔助分析"扣除通膨後，房價實質上漲多少?"
# 比較不同時期的實質房價變化: 比 HPI 更適合觀察"實際購買力意義上的房價變化"
# 2016Q1: HPI_2016_base100 = 99.32, CPI_2016_base100 = 99.1828, Real HPI = 100.1383, 99.32 / 99.1828 × 100 ≈ 100.1383

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = PROJECT_DIR / "processed"

HPI_FILE = PROCESSED_DIR / "housing_price_index.parquet"
CPI_FILE = PROCESSED_DIR / "cpi.parquet"

OUTPUT_PARQUET_FILE = PROCESSED_DIR / "real_hpi.parquet"
OUTPUT_CSV_FILE = PROCESSED_DIR / "real_hpi.csv"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

print("Project directory:", PROJECT_DIR)
print("Processed directory:", PROCESSED_DIR)


# 1. Load HPI and CPI

def load_data():
    """Load quarterly HPI and CPI Parquet files."""

    if not HPI_FILE.exists():
        raise FileNotFoundError(f"HPI file not found: {HPI_FILE}")

    if not CPI_FILE.exists():
        raise FileNotFoundError(f"CPI file not found: {CPI_FILE}")

    hpi = pd.read_parquet(HPI_FILE)
    cpi = pd.read_parquet(CPI_FILE)

    print("\n=== HPI Preview ===")
    print(hpi.head())

    print("\n=== CPI Preview ===")
    print(cpi.head())

    return hpi, cpi


# 2. Validate HPI and CPI before merging
def validate_source_data(hpi: pd.DataFrame, cpi: pd.DataFrame):

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

    # Normalize period data types

    hpi["period"] = hpi["period"].astype("string")
    cpi["period"] = cpi["period"].astype("string")

    if hpi["period"].isna().any():
        raise ValueError("HPI period contains missing values.")

    if cpi["period"].isna().any():
        raise ValueError("CPI period contains missing values.")

    # Validate quarterly period format

    hpi_valid = hpi["period"].str.fullmatch(r"\d{4}Q[1-4]")

    cpi_valid = cpi["period"].str.fullmatch(r"\d{4}Q[1-4]")

    if not hpi_valid.all():
        raise ValueError("HPI period format must be YYYYQn.")

    if not cpi_valid.all():
        raise ValueError("CPI period format must be YYYYQn.")

    # Validate numeric values

    hpi["housing_price_index"] = pd.to_numeric(
        hpi["housing_price_index"],
        errors="coerce")

    cpi["cpi_index"] = pd.to_numeric(
        cpi["cpi_index"],
        errors="coerce")

    if not np.isfinite(hpi["housing_price_index"]).all():
        raise ValueError("HPI 包含無限大或非有限數值。")

    if not np.isfinite(cpi["cpi_index"]).all():
        raise ValueError("CPI 包含無限大或非有限數值。")
    
    if hpi[["region", "housing_price_index"]].isna().any().any():
        raise ValueError("HPI contains missing region or index values.")

    if cpi["cpi_index"].isna().any():
        raise ValueError("CPI contains missing index values.")

    if (hpi["housing_price_index"] <= 0).any():
        raise ValueError("HPI index must be greater than zero.")

    if (cpi["cpi_index"] <= 0).any():
        raise ValueError("CPI index must be greater than zero.")

    # Validate duplicate keys

    if hpi.duplicated(["period", "region"]).any():
        raise ValueError("Duplicate HPI period-region combinations found.")

    if cpi["period"].duplicated().any():
        raise ValueError("Duplicate CPI quarters found.")

    # Validate 2016 base-year CPI data

    cpi_2016 = cpi[cpi["period"].str.startswith("2016")]

    expected_2016_periods = {
        "2016Q1",
        "2016Q2",
        "2016Q3",
        "2016Q4"}

    actual_2016_periods = set(cpi_2016["period"])

    if actual_2016_periods != expected_2016_periods:
        raise ValueError(
            "2016 年 CPI 季資料不完整，"
            "請確認是否包含 Q1 至 Q4。")

    return hpi, cpi


# 3. Merge HPI and CPI for common periods

def normalize_cpi_base_year(
    cpi: pd.DataFrame, base_year: int = 2016) -> pd.DataFrame:
    """
    Normalize quarterly CPI so that the annual average
    of the base year equals 100.
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
    """Both indices must use 2016 annual average = 100."""

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


# 7. Run Real HPI pipeline

def run_real_hpi() -> pd.DataFrame:
    """Execute the complete Real HPI pipeline."""

    hpi, cpi = load_data()

    hpi, cpi = validate_source_data(hpi, cpi)

    # Step 1: Normalize CPI to 2016 annual average = 100
    cpi = normalize_cpi_base_year(cpi, base_year=2016)

    # Step 2: Merge common quarters
    merged = merge_hpi_cpi(hpi, cpi)

    # Step 3: Calculate Real HPI
    real_hpi = calculate_real_hpi(merged)

    # Step 4: Validate
    real_hpi = validate_real_hpi(real_hpi)

    # Step 5: Export
    save_real_hpi(real_hpi)

    return real_hpi



# 8. Main

if __name__ == "__main__":

    real_hpi = run_real_hpi()

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