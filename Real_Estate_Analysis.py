"""
Designed a data quality validation process across 85 Taiwan real-estate transaction CSV files(sale and presale),
identifying and safely resolving schema inconsistencies before downstream analysis.
1. CSV ingestion: 自動移除英文 metadata row (row 2) skiprows=[1]
44 a csv
41 b csv: 自動處理 trailing comma
不修改原始 CSV

2. Schema normalization
a csv: 33 欄, sale 不動產買賣
b csv: 31 欄, presale 預售屋買賣
-> 建立共同欄位: transaction_type

3. Data cleaning / type conversion
處理 null 值, 0, 不合理價格, datetime, duplicate transactions 重複交易, metadata row

4. Analytical datasets for aggregation
可用資料
A: 146,790
B: 19,484
total: 166,274
"""

from pathlib import Path
import pandas as pd

# ============================================================
# Configuration
# ============================================================

EXTRACTED_DIR = Path("C:/Users/user/PycharmProjects/PythonProject")

# Analysis data are restricted to transactions with transaction dates on or before the analysis cutoff date.
# 明確定義資料截止日期
ANALYSIS_CUTOFF_DATE = pd.Timestamp("2026-09-30")

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
# Read a CSV
# a csv Shape: (筆數146790, 欄位數12)
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
# Read b CSV
# b csv Shape: (筆數19443, 欄位數12)
# ============================================================

from io import StringIO

def read_b_csv(file_path):
    """
    Read one b CSV file.

    b CSV:
    - First row: Chinese header
    - Second row: English metadata which can be removed
    - Actual transaction data starts from the third row
    - One known row contains a trailing comma
    """

    with open(file_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    fixed_lines = []

    for line_number, line in enumerate(lines, start=1):

        content = line.rstrip("\r\n")

        # Header
        if line_number == 1:
            fixed_lines.append(content + "\n")
            continue

        fields = content.split(",")

        # B schema = 31 fields
        # Remove one extra empty field caused by trailing comma.
        if len(fields) == 32 and fields[-1] == "":
            content = ",".join(fields[:-1])

        fixed_lines.append(content + "\n")

    df = pd.read_csv(
        StringIO("".join(fixed_lines)),
        encoding="utf-8-sig",
        dtype=str,
        skiprows=[1])

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

    dataframes = []

    for file_path in files:
        df = read_a_csv(file_path)
        df = clean_a_dataframe(df)

        # 保留原始資料來源: 同一筆資料只要來自不同檔案，則被視為「不完全相同」
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

    # 確認總共匯入幾份 b CSV
    print(f"\nFound {len(files)} B CSV files.")

    dataframes = []

    for file_path in files:
        df = read_b_csv(file_path)
        df = clean_b_dataframe(df)

        # 新增一欄: source_file，確認原始資料來源(顯示檔名)
        # 原始資料來源: 同一筆資料只要來自不同檔案，則被視為「不完全相同」
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
    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns]

    if missing_columns:
        raise ValueError(
            f"{name} 缺少必要欄位：\n"
            f"{missing_columns}")

    print("Required columns: OK")

    # Check transaction_type
    if df["transaction_type"].isna().any():
        raise ValueError(f"{name} transaction_type contains missing values.")

    if (df["transaction_type"].str.strip() == "").any():
        raise ValueError(f"{name} transaction_type contains empty values.")

    # Check source_file
    if df["source_file"].isna().any():
        raise ValueError(f"{name} source_file contains missing values.")

    if (df["source_file"].str.strip() == "").any():
        raise ValueError(f"{name} source_file contains empty values.")

    # Full-row duplicate check
    duplicate_count = df.duplicated().sum()

    print(f"Full-row duplicates： {duplicate_count}")

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
            "transaction_type",
            "source_file"],keep=False)

    duplicate_df = df[duplicate_mask].copy()

    print(f"Exact duplicate rows: {len(duplicate_df)}")

    if duplicate_df.empty:
        print("No exact duplicate rows found.")
        return

    duplicate_df = duplicate_df.sort_values(by=["編號", "交易年月日", "鄉鎮市區"])

    # 新增一欄: source_file，確認原始資料來源(顯示檔名)
    # 原始資料來源: 同一筆資料只要來自不同檔案，則被視為「不完全相同」
    print("\n--- Exact duplicate records and source files ---")

    # 發現a CSV 10 組 exact duplicates 皆源自 j_lvr_land_a.csv, 且每組都在同個檔案中出現兩次
    # 這 10 組 exact duplicates 並非是concat不同季度 CSV 而致的跨檔案重複，而是原始 j_lvr_land_a.csv 中即重複記錄
    # 146,790 rows -> drop 10 exact duplicates -> 146,780 rows
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

    # 不把 "source_file" 加入 deduplicate_columns, 因需判斷"交易資料本身"是否完全相同，故"資料來源"並非主要key
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

# Validate data quality after type conversion
def validate_data_quality(df, name):

    print(f"\n========== {name} Data Quality ==========")

    # --------------------------------------------------
    # 1. Missing values
    # --------------------------------------------------
    # "車位總價元"為條件欄位: 有車位 → 應有車位總價元、無車位 → 車位總價元可能空白
    # 元 / 平方公尺 → 元 / 坪: df["單價元每坪"] = (df["單價元平方公尺"] * 3.305785)
    check_columns = [
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

    invalid_dates = df["transaction_date"].isna().sum()

    print(f"\nInvalid transaction dates: {invalid_dates}")

    # Invalid transaction dates: 12, 是 transaction_date 12 筆失敗的結果，故先確認原始交易年月日
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

    # Non-positive total prices: 3
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

    print("\n" + "=" * 70)
    print("A Transaction Type Distribution")
    print("=" * 70)

    print(df_a["交易標的"].value_counts(dropna=False).to_string())


    print("\n" + "=" * 70)
    print("B Transaction Type Distribution")
    print("=" * 70)

    print(df_b["交易標的"].value_counts(dropna=False).to_string())

    unit_price_quality_a = (
        df_a.groupby("交易標的")["單價元平方公尺"]
        .agg(
            total_count="size",
            missing_count=lambda x: x.isna().sum(),
            non_positive_count=lambda x: (x <= 0).sum()))

    print(unit_price_quality_a)

    unit_price_quality_b = (
        df_b.groupby("交易標的")["單價元平方公尺"]
        .agg(
            total_count="size",
            missing_count=lambda x: x.isna().sum(),
            non_positive_count=lambda x: (x <= 0).sum()))

    print(unit_price_quality_b)

# 函式: 定義"不動產買賣"、"預售屋買賣"
def filter_residential_transactions(df):

    # 不將單獨的土地、車位、建物納入不動產平均單價計算
    # A residential transactions total_count: 50,552 + 59,822 = 110,374
    # B residential transactions total_count: 3,820 + 15,583 = 19,403
    residential_types = ["房地(土地+建物)", "房地(土地+建物)+車位"]

    residential_df = df[df["交易標的"].isin(residential_types)].copy()

    return residential_df


# 函式: 分析可用資料、不動產與預售屋買賣資料品質確認完成
def filter_analysis_eligible_transactions(df, cutoff_date=ANALYSIS_CUTOFF_DATE):

    analysis_df = df[
        df["transaction_date"].notna()
        & (df["transaction_date"] <= pd.Timestamp.today())
        & df["period"].notna()
        & df["單價元平方公尺"].notna()
        & (df["單價元平方公尺"] > 0)].copy()

    return analysis_df

# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    # -----------------------------------------
    # 1. Load raw transaction data 匯入原始交易資料
    # -----------------------------------------

    df_a = load_all_a_csv()
    df_b = load_all_b_csv()

    # -----------------------------------------
    # 2. Validate schema
    # -----------------------------------------

    validate_dataframe(df_a, "A")
    validate_dataframe(df_b, "B")

    # -----------------------------------------
    # 3. Investigate duplicates 確認重複值
    # -----------------------------------------

    investigate_exact_duplicates(df_a, "A")
    investigate_exact_duplicates(df_b, "B")

    investigate_transaction_id_duplicates(df_a, "A")
    investigate_transaction_id_duplicates(df_b, "B")

    # -----------------------------------------
    # 4. Remove exact duplicates 去重: 移除完全重複的交易資料
    # -----------------------------------------

    df_a = remove_exact_duplicates(df_a, "A")
    df_b = remove_exact_duplicates(df_b, "B")

    # -----------------------------------------
    # 5. Convert data types 資料型態轉換
    # -----------------------------------------

    df_a = convert_data_types(df_a, "A")
    df_b = convert_data_types(df_b, "B")

    # -----------------------------------------
    # 6. Data quality validation 資料品質驗證
    # -----------------------------------------

    validate_data_quality(df_a, "A")
    validate_data_quality(df_b, "B")

    df_a_residential = filter_residential_transactions(df_a)
    df_b_residential = filter_residential_transactions(df_b)

    # -----------------------------------------
    # 7. 定義"不動產買賣"、"預售屋買賣"
    # -----------------------------------------

    print("\n" + "=" * 70)
    print("Residential Transaction Sample")
    print("=" * 70)

    print(f"A residential transactions: {len(df_a_residential):,}")
    print(f"B residential transactions: {len(df_b_residential):,}")

    # 主流程中的資料檢查
    future_rows = df_a.loc[
        df_a["transaction_date"] > pd.Timestamp.today(),
        ["交易年月日",
         "transaction_date",
         "period",
         "鄉鎮市區",
         "交易標的",
         "總價元",
         "編號",
         "source_file"]]

    # 單價缺失 / 小於0的不動產、預售屋買賣交易資料，確認計算六都不動產平均單價時，有多少筆資料無法使用
    print("\n" + "=" * 70)
    print("Future-dated transactions")
    print("=" * 70)

    print(f"Count: {len(future_rows)}")

    if not future_rows.empty:
        print(future_rows.to_string(index=False))
    else:
        print("No future-dated transactions found.")

    print("\n" + "=" * 70)
    print("Residential Unit Price Quality")
    print("=" * 70)

    for name, df in [
        ("A", df_a_residential),
        ("B", df_b_residential)]:
        missing_unit_price = df["單價元平方公尺"].isna().sum()

        non_positive_unit_price = (df["單價元平方公尺"].notna() & (df["單價元平方公尺"] <= 0)).sum()

        print(f"{name} residential transactions: {len(df):,}")
        print(f"{name} missing unit prices: {missing_unit_price:,}")
        print(f"{name} non-positive unit prices: {non_positive_unit_price:,}")

    # -----------------------------------------
    # 8. 分析可用資料
    # -----------------------------------------

    df_a_residential = filter_residential_transactions(df_a)
    df_b_residential = filter_residential_transactions(df_b)

    df_a_analysis = filter_analysis_eligible_transactions(df_a_residential)
    df_b_analysis = filter_analysis_eligible_transactions(df_b_residential)


    # --------------------------------------------------------
    # Preview
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("A DataFrame Preview")
    print("=" * 70)

    print(df_a.head())

    print()
    print("=" * 70)
    print("B DataFrame Preview")
    print("=" * 70)

    print(df_b.head())