# real-estate-hpi-cpi-pipeline

## Real Estate Web Crawler, Taiwan Real Estate Transaction Data Engineering & Analytics Pipeline

A Python-based data engineering pipeline for collecting, validating, transforming, and analyzing Taiwan real-estate transaction data from official government open data sources.

The project integrates:

- Taiwan Real Estate Transaction Price Registration data
- Housing Price Index (HPI)
- Consumer Price Index (CPI)
- Inflation-adjusted Real Housing Price Index (Real HPI)

The pipeline is designed as a modular ETL workflow with data validation, schema handling, quarterly transformation, and Parquet-based analytical storage.

---

## 1. Project Overview

This project focuses on building a reproducible data pipeline for Taiwan real-estate market analysis.

The pipeline ingests quarterly transaction data from Taiwan's Ministry of the Interior and combines it with official Housing Price Index and Consumer Price Index data.

The resulting datasets can be used for downstream financial and housing-market analysis, including a team project that investigates:

> How much can be accumulated through passive Taiwan ETF investment over 10 years, and whether the resulting portfolio value can cover a housing down payment in Taiwan's six major municipalities.

The real-estate pipeline provides the housing-market side of that analysis.

### Main objectives

1. Automate the collection of quarterly real-estate transaction data.
2. Validate downloaded ZIP files before extraction.
3. Selectively extract relevant transaction datasets.
4. Handle schema inconsistencies in government open data.
5. Transform transaction-level data into quarterly analytical datasets.
6. Integrate official HPI and CPI data.
7. Calculate inflation-adjusted Real HPI.
8. Store processed datasets in Parquet format for downstream analysis.

---

## 2. Data Sources

### Ministry of the Interior — Real Estate Transaction Price Registration

Official source:

- Ministry of the Interior Real Estate Transaction Price Registration System
- Real Estate Transaction Information Data Supply System

The crawler downloads quarterly datasets using the Ministry of the Interior's seasonal download endpoint.

The pipeline currently processes:

- **A:** 不動產買賣 — Real estate sales
- **B:** 預售屋買賣 — Pre-sale housing transactions

The pipeline does not currently process:

- **C:** 不動產租賃 — Real estate rental transactions

The source data is updated periodically by the government, making it suitable for a repeatable ingestion pipeline.

Raw transaction data is not stored in this repository due to dataset size.
The data can be reproduced by running:
**python src/real_estate_webcrawler.py**

### Housing Price Index (HPI)

Source:

- Ministry of the Interior — Housing Price Index

The HPI dataset provides quarterly housing price indices for:

- 全國
- 臺北市
- 新北市
- 桃園市
- 臺中市
- 臺南市
- 高雄市

The HPI uses **2016 annual average = 100** as its base.

### Consumer Price Index (CPI)

Source:

- Directorate-General of Budget, Accounting and Statistics (DGBAS)
- National Statistics Database
- Dataset: `A030101015` — Consumer Price Index by Basic Classification

Monthly CPI observations are transformed into quarterly CPI values.

The CPI data is used to adjust the Housing Price Index for general inflation.

---

## 3. Pipeline Architecture

```text
                         Official Government Data
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
              ▼                   ▼                   ▼
      Real Estate Data           HPI                 CPI
        (A / B CSV)           Quarterly            Monthly
              │                   │                   │
              ▼                   │                   ▼
   RealEstateCrawler.py           │            CPI.py
              │                   │                   │
              ▼                   │                   ▼
   RealEstateAnalysis.py          │       Quarterly CPI
              │                   │                   │
              │                   │                   │
              ▼                   ▼                   │
       real_estate.parquet   housing_price_index     │
                                  .parquet             │
                                      │                │
                                      └───────┬────────┘
                                              ▼
                                         RealHPI.py
                                              │
                                              ▼
                                      real_hpi.parquet
```

---

## 4. Project Structure

```text
RealEstate_ETF_Project/
│
├── README.md
│
├── RealEstateCrawler.py
├── RealEstateAnalysis.py
├── HPI.py
├── CPI.py
├── RealHPI.py
├── main.py
│
├── data/
│   ├── raw/
│   │   └── *.zip
│   │
│   ├── extracted/
│   │   └── <season>/
│   │       ├── manifest.csv
│   │       ├── *_lvr_land_a.csv
│   │       └── *_lvr_land_b.csv
│   │
│   └── processed/
│       ├── real_estate.parquet
│       ├── housing_price_index.parquet
│       ├── cpi.parquet
│       └── real_hpi.parquet
│
└── .gitignore
```
> Raw and extracted government datasets should generally not be committed to GitHub. The .gitignore file should exclude large raw data files and generated datasets where appropriate.

---

# 5. Python Modules

## 5.1 RealEstateCrawler.py

Responsible for the ingestion layer of the real-estate pipeline.

### Main functions

* Download quarterly ZIP files
* Validate ZIP integrity
* Extract only required files
* Identify A/B transaction datasets
* Validate required files
* Prevent unnecessary re-downloads
* Avoid processing future quarters

### Workflow

```text
Download
   ↓
ZIP validation
   ↓
Selective extraction
   ↓
File identification
   ↓
File validation
```
### ZIP validation

Downloaded content is checked using:

python
zipfile.is_zipfile()

This prevents invalid responses, unavailable future periods, or unexpected server responses from being treated as valid datasets.

### Selective extraction

The pipeline extracts only:

* manifest.csv
* schema files
* *_lvr_land_a.csv
* *_lvr_land_b.csv

Rental transaction data (`*_lvr_land_c.csv`) is currently excluded.

---

## 5.2 RealEstateAnalysis.py

Responsible for transaction-level data cleaning, transformation, and quarterly analysis.

### Planned workflow

```text
Raw A/B CSV
     ↓
Schema inspection
     ↓
Schema normalization
     ↓
Remove metadata rows
     ↓
Data type conversion
     ↓
Data quality validation
     ↓
Transaction-level transformation
     ↓
Quarterly aggregation
     ↓
Price QoQ / YoY
     ↓
Parquet output
```
### Key analytical fields

The processed dataset is designed to contain fields such as:

```text
period
avg_unit_price
transaction_amt
price_QoQ
price_YoY
```
Additional fields may be added as the analysis requirements evolve.

### Handling government data quality issues

Government open data may contain inconsistent rows or schema irregularities.

For example, one B transaction record was identified with 32 comma-separated fields while the official header contained 31 fields.

Rather than silently using:

python
on_bad_lines="skip"

the pipeline investigates malformed rows before determining the appropriate normalization rule.

This approach is intended to prevent valid transaction records from being silently discarded.

---

## 5.3 HPI.py

Responsible for retrieving and transforming the official Housing Price Index.

### Workflow

```text
MOI HPI webpage
       ↓
HTML table extraction
       ↓
Identify target HPI table
       ↓
ROC year/quarter conversion
       ↓
Wide → Long transformation
       ↓
Numeric conversion
       ↓
Validation
       ↓
housing_price_index.parquet
```
### Output schema

```text
period
region
housing_price_index
```
Example:

```text
2026Q1 | 臺北市 | ...
2026Q1 | 新北市 | ...
2026Q1 | 桃園市 | ...
```
The HPI is retained as an official benchmark rather than being treated as an actual transaction price per ping.

---

## 5.4 CPI.py

Responsible for retrieving CPI data from the DGBAS API.

### Workflow

```text
DGBAS API
    ↓
JSON response
    ↓
Extract total CPI
    ↓
Monthly data
    ↓
Quarterly aggregation
    ↓
Validation
    ↓
cpi.parquet
```
The pipeline retrieves monthly CPI data and converts it to quarterly values.

Quarterly CPI is calculated using the mean of the three monthly CPI observations in each complete quarter.

Incomplete quarters are excluded from quarterly analysis.

### Output schema

```text
period
cpi_index
```
Example:

```text
2016Q1 | ...
2016Q2 | ...
...
2026Q2 | ...
```
---

## 5.5 RealHPI.py

Combines HPI and CPI to calculate an inflation-adjusted Housing Price Index.

Because the official HPI uses:

```text
2016 annual average = 100
```
the CPI is normalized to the same base.

### Calculation

First calculate the 2016 annual CPI base:

python
CPI_2016_base = mean(2016 quarterly CPI)

Then:

```text
CPI_2016_base100
    = CPI / CPI_2016_base × 100
```
Real HPI:

```text
Real HPI
    = HPI / CPI_2016_base100 × 100
```
### Output schema

```text
period
region
housing_price_index
cpi_index
cpi_2016_base100
real_hpi
```
### Interpretation

* **HPI** measures nominal housing-price movements relative to the HPI base period.
* **CPI** measures general consumer price movements.
* **Real HPI** adjusts housing-price movements for general inflation.

Real HPI is therefore used as a supporting macroeconomic indicator rather than as an actual transaction price.

---

# 6. Data Engineering Design

The project applies several data engineering practices.

## 6.1 Modular ETL

Each data source has a dedicated module:

```text
Source / Responsibility
──────────────────────────────────────
RealEstateCrawler.py   → Ingestion
RealEstateAnalysis.py