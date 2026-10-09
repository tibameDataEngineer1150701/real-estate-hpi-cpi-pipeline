"""
Design a data quality validation process across Taiwan real-estate transaction CSV files (sale and presale).

The pipeline automatically:
- removes the English metadata row,
- normalizes A/B schemas,
- handles known CSV formatting inconsistencies,
- investigates duplicate transactions,
- converts ROC dates and numeric fields,
- validates quarterly coverage,
- applies residential and analysis-eligibility filters.

Current coverage:2016Q1 → 2026Q3

1. CSV ingestion: 自動移除英文 metadata row (row 2) skiprows=[1]
a, b CSV 並非每季一個檔案，而是每季依縣市/區域拆成多個 CSV
a csv
b csv: 自動處理 trailing comma
不修改原始 CSV

2. Schema normalization
a csv: 33 欄, sale 不動產買賣
b csv: 31 欄, presale 預售屋買賣
-> 建立共同欄位: transaction_type

3. Data cleaning / validation
- Convert ROC dates and numeric fields
- Identify missing values
- Identify non-positive / invalid prices
- Investigate duplicate transactions
- Preserve raw records while excluding analytically ineligible records

4. Analytical datasets for aggregation

Load CSV
  ↓
Validate schema
  ↓
Investigate duplicates
  ↓
Remove exact duplicates
  ↓
Convert data types
  ↓
Transaction period coverage
  ↓
Data quality validation
  ↓
Residential filter
  ↓
Analysis eligibility
"""
from pathlib import Path
import pandas as pd

# ============================================================
# Configuration
# ============================================================

EXTRACTED_DIR = Path("C:/Users/user/real-estate-hpi-cpi-pipeline/extracted")
PROCESSED_DIR = Path("C:/Users/user/real-estate-hpi-cpi-pipeline/data/processed")
OUTPUT_PARQUET = (PROCESSED_DIR / "residential_transactions_quarterly.parquet")

# Analysis data are restricted to transactions with transaction dates on or before the analysis cutoff date.
# 明確定義資料截止日期，period: quarterly aggregation key
# 只分析 2016-01-01 ~ 2026-09-30, 具"有效正數單價"的不動產、預售屋交易
ANALYSIS_START_DATE = pd.Timestamp("2016-01-01")
ANALYSIS_CUTOFF_DATE = pd.Timestamp("2026-09-30")

ANALYSIS_START_PERIOD = "2016Q1"
ANALYSIS_END_PERIOD = "2026Q3"

# ============================================================
# Common columns
# ============================================================
# A / B 都有的欄位，先保留這些欄位供後續分析

COMMON_COLUMNS = [
    "鄉鎮市區",
    "交易標的",
    "土地位置建物門牌",
    "交易年月日",
    "交易筆棟數",
    "建物型態",
    "總價元",
    "單價元平方公尺",
    "車位類別",
    "車位總價元",
    "編號"]

# ============================================================
# Import / Read a CSV
# ============================================================

def read_a_csv(file_path):
    """
    Read one a CSV file.

    a CSV:
    - First row: Chinese header
    - Second row: English metadata which can be removed
    - Actual transaction data starts from the third row
    """

    df = pd.read_csv(file_path, encoding="utf-8-sig", dtype=str, skiprows=[1])

    return df


# ============================================================
# Import / Read b CSV
# 先讀 header
#       ↓
# 取得該檔案應有幾個 fields
#       ↓
# 每一筆資料檢查
#       ↓
# 如果多出的是最末端空欄位
#       ↓
# 刪除多餘最末端空欄位
#       ↓
# 如果仍然不符合
#       ↓
# 直接報錯並印出 file + line + raw data + Expected fields + Actual fields
# ============================================================

import csv
import sys

# 只在「多餘欄位可合理視為最末欄的一部分」時合併；若欄位數少於 header, error
def read_b_csv(file_path):
    """
    Read one B CSV file.

    B CSV:
    - First row: Chinese header
    - Second row: English metadata
    - Transaction data starts from the third row
    - Expected schema is determined from the actual header
    - Some historical files contain unquoted commas in the final
      「棟及號」 field.
    - Raw CSV files are never modified.

    Processing logic:
        Detect
          ↓
        Normalize
          ↓
        Validate
          ↓
        Preserve
    """

    with open(
        file_path,
        "r",
        encoding="utf-8-sig",
        newline="") as f:

        csv.field_size_limit(sys.maxsize)

        reader = csv.reader(f)

        # ----------------------------------------------------
        # 1. Read header
        # ----------------------------------------------------

        header = next(reader)

        expected_fields = len(header)

        print(
            f"Reading B CSV: {file_path.name} | "
            f"Expected fields: {expected_fields}")

        # ----------------------------------------------------
        # 2. Skip English metadata row
        # ----------------------------------------------------

        next(reader)

        # ----------------------------------------------------
        # 3. Read transaction rows
        # ----------------------------------------------------

        rows = []

        for line_number, row in enumerate(reader, start=3):

            # Empty row
            if not row:
                continue

            actual_fields = len(row)

            # ------------------------------------------------
            # Case 1: Normal row
            # ------------------------------------------------

            if actual_fields == expected_fields:

                rows.append(row)
                continue

            # ------------------------------------------------
            # Case 2: Extra fields
            # ------------------------------------------------

            if actual_fields > expected_fields:

                extra_fields = actual_fields - expected_fields

                print(
                    f"! B CSV field-count adjustment: "
                    f"{file_path.name}, "
                    f"line {line_number}, "
                    f"{actual_fields} → {expected_fields}; "
                    f"extra fields: {extra_fields}; "
                    f"merged into {header[-1]}")

                normalized_last_field = ",".join(
                    row[expected_fields - 1:])

                normalized_row = (
                    row[:expected_fields - 1]
                    + [normalized_last_field])

                if len(normalized_row) != expected_fields:

                    raise ValueError(
                        f"B CSV normalization failed: "
                        f"{file_path.name}, "
                        f"line {line_number}, "
                        f"expected {expected_fields}, "
                        f"got {len(normalized_row)}")

                rows.append(normalized_row)
                continue

            # ------------------------------------------------
            # Case 3: Fewer fields
            # ------------------------------------------------

            print("\n" + "=" * 70)
            print("B CSV field-count inconsistency")
            print("=" * 70)

            print(f"File: {file_path}")
            print(f"Line: {line_number}")
            print(f"Expected fields: {expected_fields}")
            print(f"Actual fields: {actual_fields}")
            print("Parsed row:")
            print(row)

            raise ValueError(
                f"B CSV field-count mismatch: "
                f"{file_path.name}, "
                f"line {line_number}, "
                f"expected {expected_fields}, "
                f"got {actual_fields}")

    # --------------------------------------------------------
    # 4. Convert normalized rows to DataFrame
    # --------------------------------------------------------

    df = pd.DataFrame(rows, columns=header)

    return df

# ============================================================
# Clean and standardize a DataFrame
# ============================================================

def clean_a_dataframe(df):

    # Keep only common analytical columns
    df = df[COMMON_COLUMNS].copy()

    # Identify transaction type
    df["transaction_type"] = "不動產買賣"

    return df


# ============================================================
# Clean and standardize b DataFrame
# ============================================================

def clean_b_dataframe(df):

    # Keep only common analytical columns
    df = df[COMMON_COLUMNS].copy()

    # Identify transaction type: presale
    df["transaction_type"] = "預售屋買賣"

    return df


# ============================================================
# Load and clean all a CSV files
# ============================================================

def load_all_a_csv():
    files = sorted(EXTRACTED_DIR.rglob("*_lvr_land_a.csv"))

    # 確認總共匯入幾份 a CSV
    print(f"\nFound {len(files)} A CSV files.")
    for file_path in files[:20]:
        print(file_path)

    dataframes = []

    for file_path in files:
        df = read_a_csv(file_path)
        df = clean_a_dataframe(df)

        # 保留原始資料來源，供 data lineage（資料來源追蹤）使用
        # source_file 不作為交易本身的 duplicate key
        df["source_file"] = file_path.name

        dataframes.append(df)

    if not dataframes:
        raise ValueError("No A CSV files found.")

    return pd.concat(dataframes, ignore_index=True)


# ============================================================
# Load and clean all b CSV files
# ============================================================

def load_all_b_csv():

    files = sorted(EXTRACTED_DIR.rglob("*_lvr_land_b.csv"))

    print(f"\nFound {len(files)} B CSV files.")

    dataframes = []

    for file_path in files:

        print(f"Reading: {file_path}")

        try:
            df = read_b_csv(file_path)

        except Exception as e:
            print("\n" + "=" * 70)
            print("B CSV parsing error")
            print("=" * 70)
            print(f"File: {file_path}")
            print(f"Error: {e}")
            raise

        df = clean_b_dataframe(df)

        # 保留原始資料來源，供 data lineage（資料來源追蹤）使用
        # source_file 不作為交易本身的 duplicate key
        df["source_file"] = file_path.name

        dataframes.append(df)

    if not dataframes:
        raise ValueError("No B CSV files found.")

    return pd.concat(dataframes, ignore_index=True)


# ============================================================
# Data Quality Check: Basic validation after ingestion and normalization.
# ============================================================
# source_file 係為了資料 lineage（資料來源追蹤）新增的 pipeline 衍生欄位，非原始交易資料欄位

def validate_dataframe(df, name):
    """
    Validate the structure and basic data quality of a dataset.
    """

    print(f"\n{'=' * 70}")
    print(f"{name} Validation")
    print(f"{'=' * 70}")

    print(f"Shape： {df.shape}")

    required_columns = [
        "鄉鎮市區",
        "交易標的",
        "土地位置建物門牌",
        "交易年月日",
        "交易筆棟數",
        "建物型態",
        "總價元",
        "單價元平方公尺",
        "車位類別",
        "車位總價元",
        "編號",
        "transaction_type",
        "source_file"]

    # Check required columns
    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(
            f"{name} 缺少必要欄位：\n"
            f"{missing_columns}")

    print("Required columns: OK")

    # Check transaction_type
    print(f"Transaction types: "
        f"{df['transaction_type'].dropna().unique().tolist()}")

def validate_dataframe(
    df,
    name,
    expected_transaction_type=None
):
    """
    Validate the structure and basic data quality of a dataset.
    """

    print(f"\n{'=' * 70}")
    print(f"{name} Validation")
    print(f"{'=' * 70}")

    print(f"Shape: {df.shape}")

    required_columns = [
        "鄉鎮市區",
        "交易標的",
        "土地位置建物門牌",
        "交易年月日",
        "交易筆棟數",
        "建物型態",
        "總價元",
        "單價元平方公尺",
        "車位類別",
        "車位總價元",
        "編號",
        "transaction_type",
        "source_file"
    ]

    # --------------------------------------------------------
    # 1. Check required columns
    # --------------------------------------------------------

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{name} 缺少必要欄位：\n"
            f"{missing_columns}"
        )

    print("Required columns: OK")

    # --------------------------------------------------------
    # 2. Check transaction_type
    # --------------------------------------------------------

    if df["transaction_type"].isna().any():
        raise ValueError(
            f"{name} transaction_type contains missing values."
        )

    if (df["transaction_type"].str.strip() == "").any():
        raise ValueError(
            f"{name} transaction_type contains empty values."
        )

    actual_types = set(
        df["transaction_type"]
        .dropna()
        .unique()
    )

    print(f"Transaction types: {sorted(actual_types)}")

    if expected_transaction_type is not None:

        if actual_types != {expected_transaction_type}:

            raise ValueError(
                f"{name} transaction_type mismatch: "
                f"expected {expected_transaction_type}, "
                f"got {actual_types}"
            )

        print(
            f"Transaction type validation: OK "
            f"({expected_transaction_type})"
        )

    # --------------------------------------------------------
    # 3. Check source_file
    # --------------------------------------------------------

    if df["source_file"].isna().any():
        raise ValueError(
            f"{name} source_file contains missing values."
        )

    if (df["source_file"].str.strip() == "").any():
        raise ValueError(
            f"{name} source_file contains empty values."
        )

    # --------------------------------------------------------
    # 4. Full-row duplicate check
    # --------------------------------------------------------

    duplicate_count = df.duplicated().sum()

    print(
        f"Full-row duplicates: {duplicate_count}"
    )

    print("Validation completed.")

# ============================================================
# Data Quality Check: a CSV contains duplicates.
# 列出交易編號，確認 a CSV 中 10 筆 duplicates 性質、判斷是否為同一筆交易
# ============================================================

def investigate_exact_duplicates(df, name):
    """
    Investigate exact duplicate transaction records
    and show their source files.
    """

    print(f"\n========== {name} Exact Duplicate Investigation ==========")

    duplicate_mask = df.duplicated(
        subset=[
            "鄉鎮市區",
            "交易標的",
            "土地位置建物門牌",
            "交易年月日",
            "交易筆棟數",
            "建物型態",
            "總價元",
            "單價元平方公尺",
            "車位類別",
            "車位總價元",
            "編號",
            "transaction_type",],keep=False)

    duplicate_df = df[duplicate_mask].copy()

    print(f"Exact duplicate rows: {len(duplicate_df)}")

    if duplicate_df.empty:
        print("No exact duplicate rows found.")
        return

    duplicate_df = duplicate_df.sort_values(by=["編號", "交易年月日", "鄉鎮市區"])

    # 顯示 source_file，確認 exact duplicate 原始資料來源
    # source_file 僅用於 lineage，不納入交易本身的 duplicate key
    print("\n--- Exact duplicate records and source files ---")

    # a CSV 10 組 exact duplicates 皆源自 j_lvr_land_a.csv, 且每組都在同個檔案中出現兩次
    # 這 10 組 exact duplicates 並非是concat不同季度 CSV 而致的跨檔案重複，而是原始 j_lvr_land_a.csv 中即重複記錄
    print(duplicate_df[[
        "鄉鎮市區",
        "交易標的",
        "土地位置建物門牌",
        "交易年月日",
        "總價元",
        "單價元平方公尺",
        "編號",
        "source_file"]].to_string(index=False))


def investigate_transaction_id_duplicates(df, name):
    """
    Check whether transaction IDs are duplicated.
    """

    print(f"\n========== {name} 編號 Duplicate Investigation ==========")

    duplicated_id_mask = df["編號"].duplicated(keep=False)
    duplicated_id_df = df[duplicated_id_mask].copy()

    print(f"Rows with duplicated 編號: {len(duplicated_id_df)}")
    print(f"Unique duplicated 編號: "f"{duplicated_id_df['編號'].nunique()}")

    if duplicated_id_df.empty:
        print("No duplicated transaction IDs found.")
        return

    duplicated_id_df = duplicated_id_df.sort_values(by=["編號", "交易年月日"])

    print("\n--- Records with duplicated 編號 ---")
    print(duplicated_id_df.to_string(index=False))



# after investigation -> data cleaning
def remove_exact_duplicates(df, name):
    """
    Remove exact duplicate transaction records.

    The transaction ID (編號) is intentionally NOT used as the deduplication key
    because duplicated IDs may represent different records.
    """

    before_count = len(df)

    # "source_file" 不加入 deduplicate_columns, 因需判斷"交易資料本身"是否完全相同，故"資料來源"並非主要key
    deduplicate_columns = [
        "鄉鎮市區",
        "交易標的",
        "土地位置建物門牌",
        "交易年月日",
        "交易筆棟數",
        "建物型態",
        "總價元",
        "單價元平方公尺",
        "車位類別",
        "車位總價元",
        "編號",
        "transaction_type"]

    df_clean = df.drop_duplicates(subset=deduplicate_columns, keep="first").copy()

    after_count = len(df_clean)

    removed_count = before_count - after_count

    print(f"\n========== {name} Deduplication ==========")
    print(f"Rows before deduplication: {before_count}")
    print(f"Rows after deduplication:  {after_count}")
    print(f"Exact duplicates removed:  {removed_count}")

    return df_clean


# Convert raw string columns into appropriate data types after deduplication: 把 CSV 裡可分析的文字轉為datetime and number
def convert_data_types(df, name):
    """
    Convert raw string columns into appropriate data types.

    Supported ROC date formats:
    - YYMMDD
    - YYYMMDD
    """

    df = df.copy()

    print(f"\n========== {name} Type Conversion ==========")

    # --------------------------------------------------
    # 1. Convert ROC transaction date
    # --------------------------------------------------

    def convert_roc_date(value):

        if value is None:
            return pd.NaT

        value = str(value).strip()

        if not value or value.lower() == "nan":
            return pd.NaT

        if not value.isdigit():
            return pd.NaT

        try:

            if len(value) == 6:
                # Example:
                # 990311 -> ROC 99/03/11
                roc_year = int(value[:2])
                month = int(value[2:4])
                day = int(value[4:6])

            elif len(value) == 7:
                # Example:
                # 1150211 -> ROC 115/02/11
                roc_year = int(value[:3])
                month = int(value[3:5])
                day = int(value[5:7])

            else:
                return pd.NaT

            ad_year = roc_year + 1911

            return pd.Timestamp(
                year=ad_year,
                month=month,
                day=day)

        except (ValueError, TypeError):
            return pd.NaT

    df["transaction_date"] = (df["交易年月日"].apply(convert_roc_date))

    # --------------------------------------------------
    # 2. Convert numeric columns
    # --------------------------------------------------

    numeric_columns = [
        "總價元",
        "單價元平方公尺",
        "車位總價元"]

    for column in numeric_columns:

        df[column] = pd.to_numeric(df[column], errors="coerce")

    # --------------------------------------------------
    # 3. Create quarter period
    # --------------------------------------------------

    df["period"] = (df["transaction_date"].dt.to_period("Q").astype(str))

    print("Type conversion completed.")

    return df

# Check transaction period coverage
def check_transaction_period_coverage(
    df,
    name,
    start_period=ANALYSIS_START_PERIOD,
    end_period=ANALYSIS_END_PERIOD):

    print("\n" + "=" * 70)
    print(f"{name} Transaction Period Coverage")
    print("=" * 70)

    expected_periods = pd.period_range(
        start=start_period,
        end=end_period,
        freq="Q").astype(str)

    actual_periods = set(
        df["period"]
        .dropna()
        .astype(str))

    expected_periods = set(expected_periods)

    missing_periods = sorted(expected_periods - actual_periods)

    unexpected_periods = sorted(actual_periods - expected_periods)

    print(f"預期季度數：{len(expected_periods)}")
    print(f"實際季度數：{len(actual_periods)}")

    if missing_periods:
        print("\n! 缺少季度：")
        print(missing_periods)
    else:
        print("\nV 沒有缺少季度")

    if unexpected_periods:
        print("\n! 預期範圍外季度：")
        print(unexpected_periods)

    coverage_complete = (
        not missing_periods
        and not unexpected_periods)

    if coverage_complete:
        print("\nV Transaction period coverage complete")
    else:
        print("\n! Transaction period coverage incomplete")

    return {
        "expected_periods": sorted(expected_periods),
        "actual_periods": sorted(actual_periods),
        "missing_periods": missing_periods,
        "unexpected_periods": unexpected_periods,
        "coverage_complete": coverage_complete,}


# Validate data quality after type conversion
def validate_data_quality(df, name):

    print(f"\n========== {name} Data Quality ==========")

    # --------------------------------------------------
    # 1. Missing values
    # --------------------------------------------------
    # "車位總價元"為條件欄位: 有車位 → 應有車位總價元、無車位 → 車位總價元可能空白
    # 元 / 平方公尺 → 元 / 坪: df["單價元每坪"] = (df["單價元平方公尺"] * 3.305785)
    # "交易標的"是分析時的重要欄位
    check_columns = [
        "交易標的",
        "transaction_date",
        "總價元",
        "單價元平方公尺",
        "period"]

    print("\n--- Missing values ---")

    missing = df[check_columns].isna().sum()

    for column, count in missing.items():
        print(f"{column}: {count}")

    # --------------------------------------------------
    # 2. Invalid transaction dates 無效交易日期
    # --------------------------------------------------
    # 檢查無法解析的交易日期
    # 不直接刪除原始交易紀錄
    # 後續分析再排除無法建立 transaction period 的資料

    invalid_dates = df["transaction_date"].isna().sum()

    print(f"\nInvalid transaction dates: {invalid_dates}")

    # Profile → Investigate → Define Rule → Transform / Filter → Validate
    invalid_date_rows = df[df["transaction_date"].isna()].copy()

    if not invalid_date_rows.empty:
        print("\n--- Invalid transaction date records ---")
        print(invalid_date_rows[[
            "交易年月日",
            "鄉鎮市區",
            "交易標的",
            "土地位置建物門牌",
            "總價元",
            "單價元平方公尺",
            "編號",
            "source_file"]].to_string(index=False))

    # --------------------------------------------------
    # 3. Invalid total prices 無效總價元
    # --------------------------------------------------

    invalid_total_price = (df["總價元"].notna() & (df["總價元"] <= 0)).sum()

    print(f"Non-positive total prices: {invalid_total_price}")

    invalid_total_price_rows = df[df["總價元"].notna() & (df["總價元"] <= 0)].copy()

    if not invalid_total_price_rows.empty:
        print("\n--- Non-positive total price records ---")
        print(invalid_total_price_rows[[
            "交易年月日",
            "鄉鎮市區",
            "交易標的",
            "土地位置建物門牌",
            "總價元",
            "單價元平方公尺",
            "編號",
            "source_file"]].to_string(index=False))

    # --------------------------------------------------
    # 4. Invalid unit prices 無效單價元平方公尺
    # --------------------------------------------------

    invalid_unit_price = (df["單價元平方公尺"].notna() & (df["單價元平方公尺"] <= 0)).sum()

    print(f"Non-positive unit prices: {invalid_unit_price}")

    # --------------------------------------------------
    # 5. Missing unit prices 單價元平方公尺遺漏
    # --------------------------------------------------

    missing_unit_price = df["單價元平方公尺"].isna().sum()

    print(f"Missing unit prices: {missing_unit_price}")

    # --------------------------------------------------
    # 6. Invalid period 無效期間
    # --------------------------------------------------

    invalid_period = (df["period"].notna()
                      & ~df["period"].astype(str).str.fullmatch(r"\d{4}Q[1-4]")).sum()

    print(f"Invalid period values: {invalid_period}")

    # --------------------------------------------------
    # 7. Print data types 資料型態
    # --------------------------------------------------

    print("\n--- Data types ---")

    # "車位總價元"為條件欄位: 有車位 → 應有車位總價元、無車位 → 車位總價元可能空白
    print(df[["transaction_date",
              "總價元",
              "單價元平方公尺",
              "車位總價元",
              "period"]].dtypes)

    # --------------------------------------------------
    # 8. Raise errors for critical problems 關鍵問題報錯
    # --------------------------------------------------
    # 分析資格（analysis eligibility）問題: 執行資料清理時發現一筆土地交易年月日為41208，無法解析日期，由於土地交易不影響房價計算，故保留所有原始交易、只取適合房價分析的交易
    if invalid_dates > 0:
        print(f"⚠️ Warning: {name} contains "f"{invalid_dates} unparseable transaction date(s).")

    if invalid_total_price > 0:
        print(f"⚠️ Warning: {name} contains "f"{invalid_total_price} non-positive total price(s).")

    if invalid_unit_price > 0:
        print(f"⚠️ Warning: {name} contains "f"{invalid_unit_price} non-positive unit price(s).")

    if invalid_period > 0:
        print(f"⚠️ Warning: {name} contains "f"{invalid_period} invalid period value(s).")

    print("\nData quality validation completed.")

    # --------------------------------------------------
    # 9. Transaction type distribution
    # --------------------------------------------------

    print("\n" + "=" * 70)
    print(f"{name} Transaction Type Distribution")
    print("=" * 70)

    print(df["交易標的"].value_counts(dropna=False).to_string())

    # --------------------------------------------------
    # 10. Unit price quality by transaction type
    # --------------------------------------------------

    unit_price_quality = (
        df.groupby("交易標的")["單價元平方公尺"]
        .agg(
            total_count="size",
            missing_count=lambda x: x.isna().sum(),
            non_positive_count=lambda x: (x <= 0).sum()))

    print("\n--- Unit Price Quality ---")
    print(unit_price_quality)

# 函式: 定義"不動產買賣"、"預售屋買賣"
def filter_residential_transactions(df):

    # 不將單獨的土地、車位、建物納入不動產平均單價計算
    residential_types = ["房地(土地+建物)", "房地(土地+建物)+車位"]

    residential_df = df[df["交易標的"].isin(residential_types)].copy()

    return residential_df


# 函式: 六都房價分析可用的不動產與預售屋買賣資料品質確認
def filter_analysis_eligible_transactions(df,
                                          start_date=ANALYSIS_START_DATE,
                                          cutoff_date=ANALYSIS_CUTOFF_DATE):
    """
    Filter transactions eligible for housing-price analysis.

    Eligibility:
    - valid transaction date
    - analysis start date <= transaction date <= analysis cutoff date
    - valid positive unit price
    """
    start_date = pd.Timestamp(start_date)
    cutoff_date = pd.Timestamp(cutoff_date)

    analysis_df = df[
        df["transaction_date"].notna()
        & (df["transaction_date"] >= start_date)
        & (df["transaction_date"] <= cutoff_date)
        & df["單價元平方公尺"].notna()
        & (df["單價元平方公尺"] > 0)].copy()

    return analysis_df

# ============================================================
# Six-Municipality Filter 篩選六都資料
# ============================================================

SIX_MUNICIPALITIES = ["臺北市","新北市","桃園市","臺中市","臺南市","高雄市",]

def extract_municipality(df):
    """
    從「土地位置建物門牌」地址前綴辨識六都
    其他縣市標記為NaN
    不修改原始地址欄位
    """

    df = df.copy()

    address = (df["土地位置建物門牌"].fillna("").astype(str).str.strip())

    df["municipality"] = pd.NA

    for city in SIX_MUNICIPALITIES:
        mask = address.str.startswith(city)
        df.loc[mask, "municipality"] = city

    return df


def filter_six_municipalities(df):

    df = extract_municipality(df)

    six_city_df = df[df["municipality"].isin(SIX_MUNICIPALITIES)].copy()

    return six_city_df


# ============================================================
# Quarterly Aggregation & Parquet Export
# 六都住宅交易季度整合與 Parquet 輸出
# ============================================================

def aggregate_quarterly_transactions(df):
    """
    依季度與六都彙總住宅交易資料

    Output columns:
    - period
    - municipality
    - avg_unit_price
    - median_unit_price
    - transaction_count

    單價單位：新臺幣元／平方公尺
    """

    required_columns = [
        "period",
        "municipality",
        "單價元平方公尺"]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns]

    if missing_columns:
        raise ValueError(f"缺少必要欄位：{missing_columns}")

    analysis_df = df.copy()

    # 確保單價為數值
    analysis_df["單價元平方公尺"] = pd.to_numeric(
        analysis_df["單價元平方公尺"],errors="coerce")

    # 排除無效資料
    analysis_df = analysis_df[
        analysis_df["period"].notna()
        & analysis_df["municipality"].isin(SIX_MUNICIPALITIES)
        & analysis_df["單價元平方公尺"].notna()
        & (analysis_df["單價元平方公尺"] > 0)].copy()

    # 依季度、縣市彙總平均單價、成交筆數
    quarterly_df = (analysis_df
        .groupby(
            ["period", "municipality"],
            as_index=False)
        .agg(
            avg_unit_price=(
                "單價元平方公尺",
                "mean"),
            median_unit_price=(
                "單價元平方公尺",
                "median"),
            transaction_count=(
                "單價元平方公尺",
                "size"))
        .sort_values(["period", "municipality"]).reset_index(drop=True))

    return quarterly_df


def export_quarterly_parquet(df,output_path=OUTPUT_PARQUET):
    """
    將季度彙總結果輸出為 Parquet
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True)

    df.to_parquet(
        output_path,
        index=False,
        engine="pyarrow")

    print("\n========== Parquet Export ==========")
    print(f"輸出路徑：{output_path}")
    print(f"資料筆數：{len(df):,}")
    print(f"季度數量：{df['period'].nunique():,}")
    print(f"縣市數量：{df['municipality'].nunique():,}")
    print("\n資料預覽：")
    print(df.head(10).to_string(index=False))

    print("\n各縣市交易筆數：")
    print(df.groupby("municipality")["transaction_count"]
          .sum().sort_values(ascending=False))

    return output_path

# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    # -----------------------------------------
    # 1. Load raw transaction data
    # -----------------------------------------

    df_a = load_all_a_csv()
    df_b = load_all_b_csv()

    # -----------------------------------------
    # 2. Validate schema
    # -----------------------------------------

    validate_dataframe(df_a,"A",expected_transaction_type="不動產買賣")
    validate_dataframe(df_b,"B",expected_transaction_type="預售屋買賣")

    # -----------------------------------------
    # 3. Investigate duplicates
    # -----------------------------------------

    investigate_exact_duplicates(df_a, "A")
    investigate_exact_duplicates(df_b, "B")

    investigate_transaction_id_duplicates(df_a, "A")
    investigate_transaction_id_duplicates(df_b, "B")

    # -----------------------------------------
    # 4. Remove exact duplicates
    # -----------------------------------------

    df_a = remove_exact_duplicates(df_a, "A")
    df_b = remove_exact_duplicates(df_b, "B")

    # -----------------------------------------
    # 5. Convert data types
    # -----------------------------------------

    df_a = convert_data_types(df_a, "A")
    df_b = convert_data_types(df_b, "B")

    # -----------------------------------------
    # 6. Transaction period coverage A/B CSV 中是否出現該季度
    # -----------------------------------------

    check_transaction_period_coverage(df_a, "A")
    check_transaction_period_coverage(df_b, "B")

    # -----------------------------------------
    # 7. Data quality validation
    # -----------------------------------------

    validate_data_quality(df_a, "A")
    validate_data_quality(df_b, "B")

    # -----------------------------------------
    # 8. Residential transaction filter
    # -----------------------------------------

    df_a_residential = filter_residential_transactions(df_a)
    df_b_residential = filter_residential_transactions(df_b)

    print("\n" + "=" * 70)
    print("Residential Transactions")
    print("=" * 70)

    print(
        f"A residential transactions: "
        f"{len(df_a_residential):,}")

    print(
        f"B residential transactions: "
        f"{len(df_b_residential):,}")

    # -----------------------------------------
    # 9. Future-dated residential transactions
    # -----------------------------------------

    future_columns = [
        "交易年月日",
        "transaction_date",
        "period",
        "鄉鎮市區",
        "交易標的",
        "總價元",
        "單價元平方公尺",
        "編號",
        "source_file"]

    future_rows_a = df_a_residential.loc[
        df_a_residential["transaction_date"]
        > ANALYSIS_CUTOFF_DATE,
        future_columns].copy()

    future_rows_b = df_b_residential.loc[
        df_b_residential["transaction_date"]
        > ANALYSIS_CUTOFF_DATE,
        future_columns].copy()

    future_rows = pd.concat(
        [future_rows_a, future_rows_b],
        ignore_index=True)

    print("\n" + "=" * 70)
    print("Future-dated Residential Transactions")
    print("=" * 70)

    print(f"Count: {len(future_rows):,}")

    if not future_rows.empty:
        print(future_rows.to_string(index=False))
    else:
        print("No future-dated residential transactions found.")

    # -----------------------------------------
    # 10. Residential unit price quality
    # -----------------------------------------

    print("\n" + "=" * 70)
    print("Residential Unit Price Quality")
    print("=" * 70)

    for name, df in [
        ("A", df_a_residential),
        ("B", df_b_residential)]:

        missing_unit_price = (
            df["單價元平方公尺"].isna().sum())

        non_positive_unit_price = (
            df["單價元平方公尺"].notna()
            & (df["單價元平方公尺"] <= 0)).sum()

        print(
            f"{name} residential transactions: "
            f"{len(df):,}")

        print(
            f"{name} missing unit prices: "
            f"{missing_unit_price:,}")

        print(
            f"{name} non-positive unit prices: "
            f"{non_positive_unit_price:,}")

    # -----------------------------------------
    # 11. Analysis eligibility
    # -----------------------------------------

    df_a_analysis = filter_analysis_eligible_transactions(df_a_residential)
    df_b_analysis = filter_analysis_eligible_transactions(df_b_residential)

    print("\n" + "=" * 70)
    print("Analysis-Eligible Transactions")
    print("=" * 70)

    print(f"A analysis rows: {len(df_a_analysis):,}")
    print(f"B analysis rows: {len(df_b_analysis):,}")

    print(f"Total analysis rows: "
          f"{len(df_a_analysis) + len(df_b_analysis):,}")

    # -----------------------------------------
    # 12. Six municipalities
    # -----------------------------------------

    six_city_a_analysis = filter_six_municipalities(df_a_analysis)
    six_city_b_analysis = filter_six_municipalities(df_b_analysis)

    print("\n" + "=" * 70)
    print("Six Municipalities")
    print("=" * 70)

    print("\nA:")
    print(
        six_city_a_analysis["municipality"]
        .value_counts()
        .sort_index())

    print("\nB:")
    print(
        six_city_b_analysis["municipality"]
        .value_counts()
        .sort_index())

    # -----------------------------------------
    # 13. Combine A and B analysis data for further analysis
    # -----------------------------------------

    six_city_analysis = pd.concat([six_city_a_analysis,
                                   six_city_b_analysis],
                                   ignore_index=True)

    print("\n" + "=" * 70)
    print("Combined Six-Municipality Analysis Data")
    print("=" * 70)

    print(f"Total six-municipality analysis rows: "
          f"{len(six_city_analysis):,}")

    print("\nTransaction type:")
    print(six_city_analysis["transaction_type"].value_counts())

    # -----------------------------------------
    # 14. Quarterly six-municipality aggregation 季度 x 六都
    # -----------------------------------------

    quarterly_six_city = (six_city_analysis
                          .groupby(["period", "municipality"],as_index=False)
                          .agg(avg_unit_price=("單價元平方公尺","mean"),
                               transaction_count=("編號","count"))
                               .sort_values(["period", "municipality"]))

    print("\n" + "=" * 70)
    print("Quarterly Six-Municipality Analysis")
    print("=" * 70)

    print(quarterly_six_city.head(20).to_string(index=False))


    # -----------------------------------------
    # A + B → Quarterly Aggregation → Parquet
    # -----------------------------------------

    # 1. 合併不動產買賣 A 與預售屋買賣 B
    six_city_analysis = pd.concat([six_city_a_analysis,
                                   six_city_b_analysis],ignore_index=True)

    print("\n========== Combine A/B ==========")
    print(f"A 資料筆數：{len(six_city_a_analysis):,}")
    print(f"B 資料筆數：{len(six_city_b_analysis):,}")
    print(f"合併後筆數：{len(six_city_analysis):,}")


    # 2. 依季度與六都整合
    quarterly_six_city = aggregate_quarterly_transactions(six_city_analysis)

    print("\n========== Quarterly Aggregation ==========")
    print(quarterly_six_city.head(10).to_string(index=False))


    # 3. Export Parquet
    export_quarterly_parquet(quarterly_six_city)

    # --------------------------------------------------------
    # Preview
    # --------------------------------------------------------

    preview_columns = [
        "transaction_date",
        "period",
        "municipality",
        "鄉鎮市區",
        "交易標的",
        "總價元",
        "單價元平方公尺",
        "編號",
        "source_file"]

    print("\n" + "=" * 70)
    print("A Analysis DataFrame Preview")
    print("=" * 70)

    print(six_city_a_analysis[preview_columns].head(10)
          .to_string(index=False))

    print("\n" + "=" * 70)
    print("B Analysis DataFrame Preview")
    print("=" * 70)

    print(six_city_b_analysis[preview_columns].head(10)
          .to_string(index=False))
