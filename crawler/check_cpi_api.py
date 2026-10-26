# after WebCrawler_cpi.py, check the structure of the API response and extract 2016 CPI monthly average
import json

from crawler.WebCrawler_cpi import build_api_url, fetch_cpi_api

# 1. 建立 API URL
url = build_api_url()

print("=== API URL ===")
print(url)

# 2. 呼叫 API
raw_data = fetch_cpi_api(url)

# 3. 檢查最外層結構
print("\n=== Top-level keys ===")
print(list(raw_data.keys()))

# 4. 檢查 data 內的結構
data = raw_data.get("data", {})

print("\n=== data keys ===")
print(list(data.keys()))

# 5. 檢查 dimensions
structure = data.get("structure", {})
dimensions = structure.get("dimensions", {})

print("\n=== Dimensions ===")
print(json.dumps(dimensions, ensure_ascii=False, indent=2))

# 6. 檢查 dataSets 數量
datasets = data.get("dataSets", [])

print("\n=== Dataset count ===")
print(len(datasets))

# 7. 檢查第一個 dataset 的 series keys
if datasets:
    series = datasets[0].get("series", {})

    print("\n=== First 10 series keys ===")
    print(list(series.keys())[:10])

    print("\n=== First series example ===")
    if series:
        first_key = next(iter(series))
        print("Series key:", first_key)
        print(json.dumps(
            series[first_key],
            ensure_ascii=False,
            indent=2)[:3000])

# 取得月份維度
observation_dimensions = (raw_data["data"]["structure"]["dimensions"]["observation"])

# 取得總指數的 observations
series_dimensions = (raw_data["data"]["structure"]["dimensions"]["series"])

series_values = series_dimensions[0]["values"]

total_index = next(i for i, item in enumerate(series_values)
                   if item["id"] == "1")

total_series_key = str(total_index)

observations = (raw_data["data"]["dataSets"][0]
                ["series"][total_series_key]["observations"])

import pandas as pd

cpi_2016 = [observations[str(i)][0] for i in range(12)]

base_2016_mean = sum(cpi_2016) / len(cpi_2016)

print(f"2016 CPI monthly average: {base_2016_mean:.4f}")
# 顯示 2016 年每月總 CPI
# 成功列出 2016 年每月完整 CPI 月資料，皆有數值、無缺漏
# 計算 2016 年每月總 CPI 平均值，得出 95.8567
# 將 CPI 轉換成「2016 年平均 = 100」的基期，以95.8567作為分母
# WebCrawler_cpi.py: CPI(BASE_YEAR=2016) = CPI 原始值 / 95.8567 x 100
print("\n=== 2016 Monthly Total CPI ===")

for i in range(12):
    period = observation_dimensions[0]["values"][i]["id"]
    value = observations.get(str(i))

    print(period, value)