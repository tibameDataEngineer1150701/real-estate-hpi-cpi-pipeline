# Real Estate Web Crawler, Taiwan Real Estate Transaction Data Engineering & Analytics Pipeline.
# Built a Python-based ETL pipeline to ingest quarterly Taiwan real-estate transaction data,
# with automated ZIP validation, selective extraction, data-quality checks, and downstream analytical storage.


# 下載每季資料的網址架構
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

# Path configuration
PROJECT_DIR = Path(__file__).resolve().parent

RAW_DIR = PROJECT_DIR / "raw"
EXTRACTED_DIR = PROJECT_DIR / "extracted"

RAW_DIR.mkdir(parents=True, exist_ok=True)
EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)

print("Project directory:", PROJECT_DIR)
print("Raw directory:", RAW_DIR)
print("Extracted directory:", EXTRACTED_DIR)

# A = 不動產買賣
# B = 預售屋買賣
TARGET_TYPES = {
    "a": "不動產買賣",
    "b": "預售屋買賣",}

# 1. Download (下載檔案預設存到 RAW_DIR)
def download_season(
    year,
    quarter,
    output_dir=RAW_DIR):
    """
    下載指定民國年份、季度的實價登錄 OpenData ZIP
    Example:download_season(105, 1) -> 105S1
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
            print(f"! ZIP 已存在且有效，跳過下載：{zip_path}")
            return zip_path
        else:
            print("! 已存在的 ZIP 無效，刪除後重新下載")
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
            print("X 下載結果不是有效 ZIP")
            print(response.text[:500])
            return None

        # 儲存 ZIP

        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"V ZIP 已儲存：{zip_path}")
        return zip_path

    except requests.RequestException as e:
        print(f"X 下載失敗：{e}")
        return None


# 2. Extract (解壓縮預設使用 EXTRACTED_DIR 中的.zip檔案)
def extract_season(zip_path, extract_dir=EXTRACTED_DIR):
    """
    解壓縮指定季度交易資料 ZIP
    Only extract:
    - manifest.csv: 資料清單
    - schema: 欄位定義
    - *_lvr_land_a.csv: 不動產買賣主檔及相關資料
    - *_lvr_land_b.csv: 預售屋買賣主檔及相關資料
    保留主檔，使用字串包含判斷，因此包含相關建物、土地、車位等資料

    Avoid to extract:
    - *_lvr_land_c.csv
    """
    # 依季度建立解壓縮資料夾
    zip_path = Path(zip_path)
    season = zip_path.name.split("_")[0]
    extract_dir = Path(extract_dir) / season
    extract_dir.mkdir(
        parents=True,
        exist_ok=True)

    print(f"\n開始解壓縮：{season}")

    # 檢查必要檔案是否存在(schema is not a must)，避免重複解壓縮
    files = identify_files(extract_dir)

    if files["manifest"] and files["land_a"] and files["land_b"]:
        print("! 該季度資料夾內已存有必要檔案，且已解壓縮，跳過")
        return extract_dir

    # if any(extract_dir.iterdir()):
    #     print(f"! 已存在解壓縮資料，跳過：{extract_dir}")
    #     return extract_dir

    # Selective extraction 選擇性解壓縮

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

    print(f"V 解壓縮完成：{season}")
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
        print("X 找不到 manifest.csv")
        return False

    print("manifest.csv")

    # Land A

    if not files["land_a"]:
        print("X 找不到不動產買賣 A")
        return False

    print(
        f"不動產買賣 A："
        f"{len(files['land_a'])} 份")

    # Land B

    if not files["land_b"]:
        print("X 找不到預售屋買賣 B")
        return False

    print(
        f"預售屋買賣 B："
        f"{len(files['land_b'])} 份")

    # Schema

    if not files["schema"]:
        print("! 找不到 schema")
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


# 6. Run Real Estate ETL pipeline: Download → Extract → Identify → Validate
def run_pipeline(
    start_year=105,
    start_quarter=1,
    end_year=None,
    end_quarter=None,
    sleep_seconds=1):
    """
    處理指定期間的實價登錄季度資料。

    year 使用民國年，例如 115。
    若未指定結束季度，則處理至最近已完成的季度。
    """

    if end_year is None or end_quarter is None:
        end_year, end_quarter = current_complete_quarter()

    if not 1 <= start_quarter <= 4:
        raise ValueError("start_quarter 必須介於 1 到 4")

    if not 1 <= end_quarter <= 4:
        raise ValueError("end_quarter 必須介於 1 到 4")

    start = (start_year, start_quarter)
    end = (end_year, end_quarter)

    if start > end:
        raise ValueError("起始季度不能晚於結束季度")

    total = 0
    succeeded = 0
    failed = []

    print(f"資料期間：{start_year}S{start_quarter}"
          f" → {end_year}S{end_quarter}")

    for year in range(start_year, end_year + 1):
        first_q = start_quarter if year == start_year else 1
        last_q = end_quarter if year == end_year else 4

        for quarter in range(first_q, last_q + 1):
            season = f"{year}S{quarter}"
            total += 1

            try:
                zip_path = download_season(year, quarter)

                if zip_path is None:
                    raise RuntimeError("ZIP 下載失敗或不是有效 ZIP")
                # 呼叫解壓縮函式時使用 zip_path
                extract_path = extract_season(zip_path)
                files = identify_files(extract_path)

                if not validate_files(files, season):
                    raise RuntimeError("必要檔案驗證失敗")

                succeeded += 1
                print(f"V {season} pipeline completed")

            except Exception as exc:
                failed.append({
                    "season": season,
                    "error": str(exc)})
                print(f"X {season} pipeline failed: {exc}")

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    result = {
        "total": total,
        "succeeded": succeeded,
        "failed": failed}

    print(
        f"Pipeline finished: "
        f"{succeeded}/{total} quarters succeeded")

    return result


# 7. ROC year to AD year conversion
def roc_quarter_to_period(year, quarter):
    """
    Convert ROC year + quarter to standardized YYYYQn format.
    Example: 115, 1 -> 2026Q1
    """
    ad_year = year + 1911
    return f"{ad_year}Q{quarter}"


if __name__ == "__main__":
    run_pipeline(start_year=105)
