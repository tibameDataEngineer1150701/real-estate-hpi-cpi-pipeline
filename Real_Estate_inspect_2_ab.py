# This analysis pipeline handles schema inconsistencies in government open data rather than silently dropping malformed records.
# a: 44 csv + b: 41 csv (所有 85 個 CSV) data quality check 資料品質檢查 -> 再決定 ETL 規則
from pathlib import Path

EXTRACTED_DIR = Path("C:/Users/user/PycharmProjects/PythonProject")

a_csv_files = list(EXTRACTED_DIR.rglob("*_lvr_land_a.csv"))
b_csv_files = list(EXTRACTED_DIR.rglob("*_lvr_land_b.csv"))

# ============================================================
# A CSV
# ============================================================

print("=" * 70)
print("A CSV DATA QUALITY CHECK")
print("=" * 70)

a_total_rows = 0
a_bad_field_rows = 0
a_files_with_bad_rows = 0

for file in a_csv_files:

    with open(file, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    header_fields = len(lines[0].rstrip("\r\n").split(","))

    bad_rows = []

    for line_number, line in enumerate(lines[1:], start=2):

        field_count = len(line.rstrip("\r\n").split(","))

        # A csv 第 2 行是英文 metadata
        if line_number == 2:
            continue

        a_total_rows += 1

        if field_count != header_fields:
            bad_rows.append((line_number, field_count))

    if bad_rows:

        a_files_with_bad_rows += 1
        a_bad_field_rows += len(bad_rows)

        print()
        print("異常檔案：")
        print(file)

        for line_number, field_count in bad_rows:
            print(
                f"  第 {line_number} 行："
                f"{field_count} fields "
                f"(header={header_fields})")

"""
A CSV 檔案數： 44
A CSV 資料列數： 146790
A 有異常的檔案數： 0
A 異常資料列數： 0
"""

print()
print("A CSV 檔案數：", len(a_csv_files))
print("A CSV 資料列數：", a_total_rows)
print("A 有異常的檔案數：", a_files_with_bad_rows)
print("A 異常資料列數：", a_bad_field_rows)


# ============================================================
# B CSV
# ============================================================

print()
print("=" * 70)
print("B CSV DATA QUALITY CHECK")
print("=" * 70)

b_total_rows = 0
b_bad_field_rows = 0
b_files_with_bad_rows = 0

for file in b_csv_files:

    with open(file, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    header_fields = len(lines[0].rstrip("\r\n").split(","))

    bad_rows = []

    for line_number, line in enumerate(lines[1:], start=2):

        field_count = len(line.rstrip("\r\n").split(","))

        b_total_rows += 1

        if field_count != header_fields:
            bad_rows.append((line_number, field_count))

    if bad_rows:

        b_files_with_bad_rows += 1
        b_bad_field_rows += len(bad_rows)

        print()
        print("⚠️ 異常檔案：")
        print(file)

        for line_number, field_count in bad_rows:
            print(
                f"  第 {line_number} 行："
                f"{field_count} fields "
                f"(header={header_fields})")

"""
B CSV 檔案數： 41
B CSV 資料列數： 19484
B 有異常的檔案數： 1
B 異常資料列數： 1
"""

print()
print("B CSV 檔案數：", len(b_csv_files))
print("B CSV 資料列數：", b_total_rows)
print("B 有異常的檔案數：", b_files_with_bad_rows)
print("B 異常資料列數：", b_bad_field_rows)

print()
print("DATA QUALITY CHECK COMPLETE.")