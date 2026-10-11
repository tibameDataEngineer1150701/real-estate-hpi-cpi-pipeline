"""
爬取的內政部實價登錄房價資料:季度
CPI:通常為月資料
HPI:季資料，基期:民國105年(2016)=100
CPI 上升不一定使房價上升，影響房價因素: 利率、房貸政策、信用管制、所得、人口、土地價格、供給、交易量、建築成本、CPI

Housing Price Index(HPI, 住宅價格指數) https://pip.moi.gov.tw/Publicize/Info/E1060?
內政部住宅價格指數採特徵價格法、標準住宅與 Laspeyres 公式處理住宅品質差異，適合作為「官方 Housing Price Index」資料源

https://www.dgbas.gov.tw/News_Content.aspx?n=4437&s=207893
主計總處「我國消費者物價指數算法刻意略去『房價』」: 住宅購買屬於固定資產，購屋價格不納入 CPI
內政部「住宅價格指數」並非把所有房屋成交價格取平均，而是透過標準住宅、特徵價格模型等方法處理住宅品質差異，避免某期間忽然大量出現豪宅等高單價不動產買賣，致使平均成交價格大幅上漲

不預設 tables[index]，因政府網站 HTML table 順序可能改變
"""

# Path: 處理檔案與目錄路徑的 Python 內建 module
from pathlib import Path
# python -m pip install lxml: Python 用以解析 XML / HTML 文件的 module, 把 HTML 結構解析成 Python 可以處理的結構 → pandas 再轉成 DataFrame
import requests
import pandas as pd
import urllib3
from io import StringIO

url = "https://pip.moi.gov.tw/Publicize/Info/E1060"

# 1. 關閉網站 SSL 憑證造成的 warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 2. 用 requests 取得 HTML 整個網頁(執行速度較慢): The source website currently requires SSL verification to be disabled in this environment.
VERIFY_SSL = False

def fetch_hpi_html(url):
    response = requests.get(
        url,
        timeout=60,
        verify=VERIFY_SSL)

    response.raise_for_status()

    print("HTTP Status:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))
    print("HTML length:", len(response.text))

    return response.text

html = fetch_hpi_html(url)

tables = pd.read_html(StringIO(html))

# 3. 以 pandas 解析 HTML table, 回傳找到幾張表
print(f"{len(tables)} tables found.")

# 4. 從網頁找到5張表, 只需表2, 為避免政府網站表格順序改變, 改以for loop找欄位中必要的關鍵字"縣市""全國", 單一季度有7個regions(含"全國")
# Instead of relying on a fixed table index, the pipeline identifies the HPI table based on required columns.
"""
找出5 tables的前5列
for i, table in enumerate(tables):
    print("=" * 30)
    print(f"TABLE {i+1}")
    print(table.head())

只印出表2全部資料 
hpi_raw = tables[1]
print(hpi_raw)
"""
required_columns = {
    "縣市",
    "全國",
    "新北市",
    "臺北市",
    "桃園市",
    "臺中市",
    "臺南市",
    "高雄市"}

hpi_raw = None

for table in tables:
    table.columns = [str(column).strip()
        for column in table.columns]

    if required_columns.issubset(table.columns):
        hpi_raw = table
        break

if hpi_raw is None:
    raise ValueError("找不到住宅價格指數歷史資料表")

# 5. Transform: 原始表2格式是wide format, 須轉為long format, 以利join內政部實價登錄資料
# 原始 Table 2： 縣市 | 全國 | 新北市 | 臺北市 | 桃園市 | 臺中市 | 臺南市 | 高雄市
hpi_raw = hpi_raw.rename(columns={"縣市": "period"})

## 民國年轉換為西元年(+1911)
import re
def roc_quarter_to_period(value):
    value = str(value).strip()
    match = re.fullmatch(r"(\d+)年第([1-4])季", value)  ## 只允許Q1-Q4

    if not match:
        raise ValueError(f"無法解析季度：{value}")

    roc_year = int(match.group(1))
    quarter = match.group(2)

    ad_year = roc_year + 1911

    return f"{ad_year}Q{quarter}"

hpi_raw["period"] = hpi_raw["period"].apply(roc_quarter_to_period)

# print(hpi_raw)

## melt
hpi_long = hpi_raw.melt(
    id_vars=["period"],
    var_name="region",
    value_name="housing_price_index")

## HPI 資料型態 to_numeric
hpi_long["housing_price_index"] = pd.to_numeric(
    hpi_long["housing_price_index"],
    errors="coerce")

# 6. Data Validation: 每一個季度必須有全國 + 六都，共 7 個區域
expected_regions = {
    "全國",
    "新北市",
    "臺北市",
    "桃園市",
    "臺中市",
    "臺南市",
    "高雄市"}

## 驗證有哪些 region: assert set(hpi_long["region"].unique()) == expected_regions
if set(hpi_long["region"].unique()) != expected_regions:
    raise ValueError("HPI region validation failed")

## 驗證 HPI 有無 null: assert hpi_long["housing_price_index"].notna().all()
if hpi_long["housing_price_index"].isna().any():
    raise ValueError("HPI contains missing values")

## 驗證 period 有無 null: assert hpi_long["period"].notna().all()
if hpi_long["period"].isna().any():
    raise ValueError("HPI contains missing periods")

## 驗證 period 格式完全符合(季度只允許Q1-Q4)
if not hpi_long["period"].str.fullmatch(r"\d{4}Q[1-4]").all():
    raise ValueError("Invalid period format")

if hpi_long.duplicated(["period", "region"]).any():
    raise ValueError("Duplicate period-region combinations found")

## 驗證資料範圍
if hpi_long["period"].nunique() == 0:
    raise ValueError("HPI contains no valid periods")

region_count = (hpi_long.groupby("period")["region"].nunique())

## 驗證是否 7 個 regions: assert (region_count == 7).all()
if not (region_count == 7).all():
    raise ValueError("Each period must contain exactly 7 regions")


# 7. Export HPI.parquet to folder
# 取得.py所在的專案目錄
PROJECT_DIR = Path(__file__).resolve().parent

# 設定輸出資料夾
PROCESSED_DIR = PROJECT_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Export Parquet
PARQUET_FILE = PROCESSED_DIR / "housing_price_index.parquet"

hpi_long.to_parquet(PARQUET_FILE, index=False)

print(f"HPI Parquet saved to: {PARQUET_FILE}")


# 8. Export validated HPI data to CSV
CSV_FILE = PROCESSED_DIR / "housing_price_index.csv"

# 匯出 CSV，使用 UTF-8 with BOM，方便 Excel 辨識中文
hpi_long.to_csv(CSV_FILE,index=False,encoding="utf-8-sig")

print(f"HPI CSV saved to: {CSV_FILE}")

# 8. Read back and inspect the exported HPI.parquet, CSV
hpi_parquet = pd.read_parquet(PARQUET_FILE)
hpi_csv = pd.read_csv(CSV_FILE,encoding="utf-8-sig")

print(hpi_parquet.head(20))
print(hpi_parquet.info())
print(hpi_parquet.columns)

print("\n=== HPI CSV Preview ===")
print(hpi_csv.head(20))

print("\n=== Data Info ===")
print(hpi_csv.info())

print("\n=== Columns ===")
print(hpi_csv.columns.tolist())

print("\n=== Row Count ===")
print(len(hpi_csv))