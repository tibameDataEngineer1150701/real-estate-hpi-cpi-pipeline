# Real HPI = HPI_2016_base100 / CPI_2016_base100 x 100%
# 2016Q1: HPI_2016_base100 = 99.32, CPI_2016_base100 = 99.1828, Real HPI = 100.1383, 99.32 / 99.1828 × 100 ≈ 100.1383
import pandas as pd
from pathlib import Path

PROCESSED_DIR_REAL_HPI = Path("data/processed")
OUTPUT_FILE_REAL_HPI = PROCESSED_DIR_REAL_HPI / "real_hpi.parquet"

# Read HPI and CPI data
hpi = pd.read_parquet("data/processed/housing_price_index.parquet")
cpi = pd.read_parquet("data/processed/cpi.parquet")

# Validate 2016 CPI data
cpi_2016 = cpi[cpi["period"].str.startswith("2016")].copy()

if len(cpi_2016) != 4:
    raise ValueError("2016 年 CPI 季資料不完整，無法建立 2016 年基期")

# 計算 2016 年 CPI 平均
cpi_2016_base = (cpi[cpi["period"].str.startswith("2016")]["cpi_index"].mean())

if pd.isna(cpi_2016_base) or cpi_2016_base <= 0:
    raise ValueError("2016 年 CPI 基期無效")

# Normalize CPI to 2016 annual average = 100
cpi["cpi_2016_base100"] = (cpi["cpi_index"] / cpi_2016_base * 100)

# Validate HPI and CPI periods before merging
hpi_periods = set(hpi["period"])
cpi_periods = set(cpi["period"])

missing_cpi_periods = hpi_periods - cpi_periods

if missing_cpi_periods:
    raise ValueError(f"CPI 缺少以下 HPI 季度：{sorted(missing_cpi_periods)}")

# HPI merge CPI on period -> Real HPI
real_hpi = hpi.merge(cpi[["period", "cpi_index", "cpi_2016_base100"]],
    on="period",
    how="inner")

# Calculate Real HPI
real_hpi["real_hpi"] = (
    real_hpi["housing_price_index"]
    / real_hpi["cpi_2016_base100"]
    * 100)

# Validate Real HPI
if real_hpi.empty:
    raise ValueError("Real HPI contains no data")

if real_hpi[
    ["housing_price_index",
     "cpi_index",
     "cpi_2016_base100",
     "real_hpi"]].isna().any().any():
    raise ValueError("Real HPI contains missing values")

if real_hpi.duplicated(["period", "region"]).any():
    raise ValueError("Duplicate period-region combinations found")

if (real_hpi["real_hpi"] <= 0).any():
    raise ValueError("Real HPI contains invalid values")

# output Real HPI.parquet to folder
PROCESSED_DIR_REAL_HPI.mkdir(parents=True, exist_ok=True)

real_hpi.to_parquet(OUTPUT_FILE_REAL_HPI, index=False)

print(f"✅ Real HPI saved to: {OUTPUT_FILE_REAL_HPI}")

# Data Validation
# Real HPI 不可為空
# period + regions 不可重複
# HPI 不可有 NaN
# CPI 不可有 NaN
# Real HPI 不可有 NaN
# 2016 CPI 基期不可為 NaN 或 0

# Read back and inspect Real HPI.parquet
real_hpi_check = pd.read_parquet("data/processed/real_hpi.parquet")

print(real_hpi_check.head(20))
print(real_hpi_check.info())
print(real_hpi_check.columns)