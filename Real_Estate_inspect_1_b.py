from pathlib import Path
import pandas as pd


EXTRACTED_DIR = Path("C:/Users/user/PycharmProjects/PythonProject")
# b CSV 異常資料列僅 1 列，因最後多一個 trailing comma，Header：31 fields，資料列：32 fields，Pandas：Expected 31 fields in line 883, saw 32
b_csv = EXTRACTED_DIR / "115S1" / "a_lvr_land_b.csv"
# 確認 41 個 b CSV 是否只有這一種異常、確認"最後多一個逗號"能否做為 _Real_Estate_Analysis.py 通用清理規則
EXPECTED_FIELDS = 31


# ============================================================
# 1. 找出異常列
# ============================================================

with open(b_csv, "r", encoding="utf-8-sig") as f:
    lines = f.readlines()

bad_lines = []

for line_number, line in enumerate(lines, start=1):

    field_count = len(line.rstrip("\n").split(","))

    if line_number == 1:
        continue

    if field_count != EXPECTED_FIELDS:
        bad_lines.append((line_number, field_count, line))


print("=" * 70)
print("原始 CSV 異常列")
print("=" * 70)

for line_number, field_count, line in bad_lines:
    print(f"第 {line_number} 行：{field_count} fields")
    print(line)


# ============================================================
# 2. 修正 trailing comma
# ============================================================

fixed_lines = []

for line_number, line in enumerate(lines, start=1):

    # 只處理資料列
    if line_number != 1:

        # 移除換行
        content = line.rstrip("\r\n")

        # 如果剛好 32 fields，且最後一個 field 為空
        fields = content.split(",")

        # 如果剛好多一欄，且此欄為空，則移除多餘的結尾逗號(trailing comma)
        # 不使用 on_bad_lines="skip"：因為此筆交易本身是有效資料，只是 CSV 格式多了一個結尾逗號(trailing comma)
        if len(fields) == EXPECTED_FIELDS + 1 and fields[-1] == "":
            content = ",".join(fields[:-1])

        line = content + "\n"

    fixed_lines.append(line)


# ============================================================
# 3. 寫出修正後的暫存 CSV
# ============================================================

fixed_file = b_csv.with_name("a_lvr_land_b_fixed.csv")

with open(fixed_file, "w", encoding="utf-8-sig", newline="") as f:
    f.writelines(fixed_lines)


print()
print("=" * 70)
print("修正完成")
print("=" * 70)

print("輸出檔案：")
print(fixed_file)


# ============================================================
# 4. 用 Pandas 測試讀取: 移除最後一欄空欄位後，Pandas 可正常讀取
# ============================================================

print()
print("=" * 70)
print("測試 Pandas 讀取")
print("=" * 70)

df = pd.read_csv(
    fixed_file,
    encoding="utf-8-sig",
    dtype=str)

print("讀取成功！")
print("Shape：", df.shape)
print("欄位數：", len(df.columns))

print()
print("欄位：")
print(df.columns.tolist())

print()
print("第 883 行附近：")
print(df.iloc[879:884])