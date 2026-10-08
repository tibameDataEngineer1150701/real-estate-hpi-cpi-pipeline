# Real Estate Web Crawler, Taiwan Real Estate Transaction Data Engineering & Analytics Pipeline.
# Built a Python-based ETL pipeline to ingest quarterly Taiwan real-estate transaction data,
# with automated ZIP validation, selective extraction, data-quality checks, and downstream analytical storage.

# 內政部不動產交易實價查詢服務網: 選擇縣市 → 選擇鄉鎮市區 → 設定交易期間 → 按搜尋 → 取得交易資料
# 官方網站明確說明資料每月 1、11、21 日更新，適合做增量資料 Pipeline
# 內政部不動產交易實價查詢服務網 → 資料下載及申請 → 不動產成交案件實際資訊資料供應系統 → 登入https://plvr.land.moi.gov.tw/Index
# 網站不是直接下載一個 ZIP，而是在瀏覽器中先建立 ZIP，再觸發下載

# 觀察下載每季資料的網址邏輯:https://plvr.land.moi.gov.tw//DownloadSeason?season=114S1&type=zip&fileName=lvr_landcsv.zip

# 下載每季資料的網址主要架構
BASE_URL = "https://plvr.land.moi.gov.tw/DownloadSeason"


# ============================================================
# Built a Python-based ETL pipeline to ingest quarterly Taiwan real-estate transaction data, with automated ZIP validation,
# selective extraction, data-quality checks, and downstream analytical storage.
# ============================================================

# Path: 處理檔案與目錄路徑的 Python 內建模組
from pathlib import Path
from datetime import datetime
import requests
import zipfile
import io
import time

# Configuration

BASE_URL = "https://plvr.land.moi.gov.tw/DownloadSeason"

# RAW_DIR = Path("data/raw")
# EXTRACTED_DIR = Path("data/extracted")

RAW_DIR = Path("C:/Users/user/real-estate-hpi-cpi-pipeline/raw")
EXTRACTED_DIR = Path("C:/Users/user/real-estate-hpi-cpi-pipeline/extracted")

# A = 不動產買賣
# B = 預售屋買賣
TARGET_TYPES = {
    "a": "不動產買賣",
    "b": "預售屋買賣",}


# 1. Download
def download_season(
    year,
    quarter,
    output_dir=RAW_DIR):
    """
    下載指定民國年份、季度的實價登錄 OpenData ZIP。
    Example:
        download_season(105, 1)
        -> 105S1
    """

    season = f"{year}S{quarter}"

    url = (
        f"{BASE_URL}"
        f"?season={season}"
        f"&type=zip"
        f"&fileName=lvr_landcsv.zip")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_path = output_dir / f"{season}_lvr_landcsv.zip"

    print("=" * 70)
    print(f"開始下載：{season}")
    print(url)

    # 如果 ZIP 已存在，先確認是否為有效 ZIP

    if zip_path.exists():

        if zipfile.is_zipfile(zip_path):
            print(f"⚠️ ZIP 已存在且有效，跳過下載：{zip_path}")
            return zip_path
        else:
            print("⚠️ 已存在的 ZIP 無效，刪除後重新下載")
            zip_path.unlink()

    # Download

    try:
        response = requests.get(
            url,
            timeout=120,
            verify=False)

        print("HTTP Status:", response.status_code)
        print("Content-Type:", response.headers.get("Content-Type"))
        print("Size:", len(response.content), "bytes")

        response.raise_for_status()

        # 確認是否為有效 ZIP(如為未來年度或季度，則為無效ZIP)

        if not zipfile.is_zipfile(io.BytesIO(response.content)):
            print("❌ 下載結果不是有效 ZIP")
            print(response.text[:500])
            return None

        # 儲存 ZIP

        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"✅ ZIP 已儲存：{zip_path}")
        return zip_path

    except requests.RequestException as e:
        print(f"❌ 下載失敗：{e}")
        return None


# 2. Extract
def extract_season(zip_path, extract_dir=EXTRACTED_DIR):
    """
    解壓縮指定季度 ZIP。

    目前只 Extract：
    - manifest.csv
    - schema
    - *_lvr_land_a.csv
    - *_lvr_land_b.csv

    不處理：
    - *_lvr_land_c.csv
    """

    zip_path = Path(zip_path)
    season = zip_path.name.split("_")[0]
    extract_dir = Path(extract_dir) / season
    extract_dir.mkdir(
        parents=True,
        exist_ok=True)

    print(f"\n開始解壓縮：{season}")

    # 檢查該季度是否已完成解壓縮: 若該季度已有必要資料，跳過(檢查必要檔案，schema is not a must)
    files = identify_files(extract_dir)

    if files["manifest"] and files["land_a"] and files["land_b"]:
        print("⚠️ 該季度資料夾內已存有必要檔案，且已解壓縮，跳過")
        return extract_dir

    # if any(extract_dir.iterdir()):
    #     print(f"⚠️ 已存在解壓縮資料，跳過：{extract_dir}")
    #     return extract_dir

    # Selective extraction

    with zipfile.ZipFile(zip_path,"r") as zip_ref:
        extracted_count = 0

        for member in zip_ref.namelist():
            filename = Path(member).name.lower()

            # manifest
            is_manifest = filename == "manifest.csv"

            # schema
            is_schema = "schema" in filename

            # 不動產買賣 A
            is_land_a = "_lvr_land_a" in filename

            # 預售屋買賣 B
            is_land_b = "_lvr_land_b" in filename

            if (is_manifest
                or is_schema
                or is_land_a
                or is_land_b):

                zip_ref.extract(member, extract_dir)

                extracted_count += 1

    print(f"✅ 解壓縮完成：{season}")
    print(f"共 Extract {extracted_count} 個檔案")
    return extract_dir


# 3. Identify files
def identify_files(extract_dir):
    """
    辨識解壓縮後的資料：

    1. manifest.csv
    2. schema
    3. 不動產買賣 A
    4. 預售屋買賣 B

    C（不動產租賃）暫不處理
    """

    extract_dir = Path(extract_dir)
    result = {
        "manifest": None,
        "schema": [],
        "land_a": [],
        "land_b": []}

    csv_files = list(extract_dir.rglob("*.csv"))

    print(f"\n找到 {len(csv_files)} 個 CSV 檔案")

    for file in csv_files:

        filename = file.name.lower()

        # manifest
        if filename == "manifest.csv":
            result["manifest"] = file

        # schema
        elif "schema" in filename:
            result["schema"].append(file)

        # 不動產買賣
        elif "_lvr_land_a" in filename:
            result["land_a"].append(file)

        # 預售屋買賣
        elif "_lvr_land_b" in filename:
            result["land_b"].append(file)

    return result


# 4. Data Validation
def validate_files(files, season):
    """
    驗證指定季度的必要檔案是否存在
    """

    print(f"\n========== Validate {season} ==========")

    # Manifest（資訊清單）

    if files["manifest"] is None:
        print("❌ 找不到 manifest.csv")
        return False

    print("manifest.csv")

    # Land A

    if not files["land_a"]:
        print("❌ 找不到不動產買賣 A")
        return False

    print(
        f"不動產買賣 A："
        f"{len(files['land_a'])} 份")

    # Land B

    if not files["land_b"]:
        print("❌ 找不到預售屋買賣 B")
        return False

    print(
        f"預售屋買賣 B："
        f"{len(files['land_b'])} 份")

    # Schema

    if not files["schema"]:
        print("⚠️ 找不到 schema")
    else:
        print(
            f"Schema："
            f"{len(files['schema'])} 份")

    return True


# 5. Current quarter

def current_complete_quarter():
    """
    回傳目前已完成的最後一個季度，例如：
    2026-10-01 → 2026Q3
    2026-08-01 → 2026Q2
    2026-04-01 → 2026Q1
    """
    today = datetime.now()
    quarter = (today.month - 1) // 3 + 1

    if quarter == 1:
        year = today.year - 1
        quarter = 4
    else:
        year = today.year
        quarter -= 1

    return year - 1911, quarter

# def current_quarter():
#     month = datetime.now().month
#     return (month - 1) // 3 + 1


# 6. Run Real Estate ETL pipeline: Download → Extract → Identify → Validate

def run_pipeline(
    start_year=105,
    sleep_seconds=1):
    """
    可重複執行：
    已下載且有效的 ZIP → Skip
    已解壓縮資料 → Skip

    Pipeline 只處理「已完成」的季度，
    不會嘗試下載目前尚未完成的季度。
    """

    end_year, end_quarter = current_complete_quarter()

    print("\n")
    print("=" * 70)
    print("Real Estate Data Pipeline")
    print("=" * 70)

    print(
        f"資料期間："
        f"{start_year}S1 → "
        f"{end_year}S{end_quarter}")

    # Loop through seasons

    for year in range(
        start_year,
        end_year + 1):

        for quarter in range(1, 5):
            # 不下載未完成/未來季度
            if year == end_year and quarter > end_quarter:
                break

            season = f"{year}S{quarter}"

            print("\n")
            print("#" * 70)
            print(f"Processing {season}")
            print("#" * 70)

            # Step 1: Download

            zip_path = download_season(
                year,
                quarter)

            if zip_path is None:
                print(f"X {season} Download failed")
                continue

            # Step 2: Extract

            extract_path = extract_season(zip_path)

            if extract_path is None:
                print(f"X {season} Extract failed")
                continue

            # Step 3: Identify

            files = identify_files(extract_path)

            # Step 4: Validate

            is_valid = validate_files(files, season)

            if not is_valid:
                print(f"X {season} validation failed")
                continue

            print(f"V {season} pipeline completed")

            # Avoid hitting server too quickly
            time.sleep(sleep_seconds)

    print("\n")
    print("=" * 70)
    print("Pipeline finished")
    print("=" * 70)

def roc_quarter_to_period(year, quarter):
    """
    Convert ROC year + quarter to standardized YYYYQn format.
    Example: 115, 1 -> 2026Q1
    """
    ad_year = year + 1911
    return f"{ad_year}Q{quarter}"

# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    run_pipeline(start_year=105)