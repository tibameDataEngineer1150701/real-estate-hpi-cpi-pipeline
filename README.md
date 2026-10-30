# real-estate-hpi-cpi-pipeline

## Real Estate Web Crawler, Taiwan Real Estate Transaction Data Engineering & Analytics Pipeline

A Python-based data engineering pipeline for collecting, validating, transforming, and analyzing Taiwan real-estate transaction data from official government open data sources.

The project covers:

- Taiwan Real Estate Transaction Price Registration data
- Housing Price Index (HPI)
- Consumer Price Index (CPI)
- Inflation-adjusted Real Housing Price Index (Real HPI)

The pipeline follows a modular ETL workflow with data validation, schema handling, transaction-level filtering, quarterly aggregation, and Parquet-based analytical storage.

---

## 1. Project Overview

This project focuses on building a reproducible data pipeline for Taiwan real-estate market analysis.

The pipeline collects quarterly transaction data from Taiwan's Ministry of the Interior and processes it into analytical datasets. Official Housing Price Index and Consumer Price Index data provide additional indicators for housing-market and inflation analysis.

The resulting datasets can support downstream financial and housing-market analysis, including a team project that investigates:

> How much can be accumulated through passive Taiwan ETF investment over 10 years, and whether the resulting portfolio value can cover a housing down payment in Taiwan's six major municipalities.

The real-estate pipeline provides the housing-market data for that analysis.

### Main objectives

1. Automate the collection of quarterly real-estate transaction data.
2. Validate downloaded ZIP files before extraction.
3. Selectively extract relevant transaction datasets.
4. Handle schema inconsistencies in government open data.
5. Validate and transform transaction-level data.
6. Filter residential transactions and analysis-eligible records.
7. Aggregate transaction data by quarter, municipality, and transaction type.
8. Process official HPI and CPI data for quarterly analysis.
9. Calculate inflation-adjusted Real HPI.
10. Store processed datasets in CSV and Parquet formats for downstream analysis.

---

## 2. Data Sources

### Ministry of the Interior — Real Estate Transaction Price Registration

Official source:

- Ministry of the Interior Real Estate Transaction Price Registration System
- Real Estate Transaction Information Data Supply System

The crawler downloads quarterly datasets using the Ministry of the Interior's seasonal download endpoint.

The pipeline currently processes:

- **A: 不動產買賣** — Real estate sales
- **B: 預售屋買賣** — Pre-sale housing transactions

The pipeline does not currently process:

- **C: 不動產租賃** — Real estate rental transactions

The source data is updated periodically by the government, making it suitable for a repeatable ingestion pipeline.

Raw transaction data is not stored in this repository due to dataset size. The data can be reproduced by running the real-estate crawler module. The exact command should follow the current project entry point.

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

The HPI is used as an official benchmark for housing-price movements rather than as an actual transaction price per ping.

### Consumer Price Index (CPI)

Source:

- Directorate-General of Budget, Accounting and Statistics (DGBAS)
- National Statistics Database
- Dataset: `A030101015` — Consumer Price Index by Basic Classification

Monthly CPI observations are transformed into quarterly CPI values.

Quarterly CPI is calculated using the mean of the three monthly observations in each complete quarter. Incomplete quarters are excluded from quarterly analysis.

The CPI data is used to adjust the Housing Price Index for general inflation.

---

## 3. Pipeline Architecture

```text
                    Official Government Data
                              |
             +----------------+----------------+
             |                |                |
             v                v                v
     Real Estate Data        HPI              CPI
        (A / B CSV)       Quarterly          Monthly
             |                |                |
             v                |                v
   Real Estate Crawler        |          CPI Crawler
             |                |                |
             v                |                v
      Raw / Extracted         |         Quarterly CPI
      Transaction Data        |                |
             |                |                |
             v                |                |
       analysis.py            |                |
             |                |                |
             v                v                |
   Quarterly Transaction   housing_price_index |
        Aggregation          .csv / .parquet    |
             |                |                |
             |                +--------+-------+
             |                         |
             |                         v
             |                      Real_HPI.py
             |                         |
             |                         v
             |                    real_hpi.csv
             |                    real_hpi.parquet
             |
             v
   Transaction-level and
   quarterly analytical data
   in CSV / Parquet format
```

The real-estate transaction analysis and the HPI/CPI processing workflows are modular components. Their outputs can be used together for downstream analysis after the relevant periods, geographic dimensions, and field definitions have been aligned.

---

## 4. Project Structure

```text
real-estate-hpi-cpi-pipeline/
│
├── README.md
│
├── WebCrawler_real_estate.py
├── Real_Estate_analysis.py
├── WebCrawler_hpi.py
├── WebCrawler_cpi.py
├── Real_HPI.py
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
│       ├── real_estate.csv
│       ├── housing_price_index.parquet
│       ├── housing_price_index.csv
│       ├── cpi.parquet
│       ├── cpi.csv
│       ├── real_hpi.parquet
│       └── real_hpi.csv
│
└── .gitignore
```

The directory structure above describes the intended project organization. Actual file locations should follow the paths configured in the current Python modules.

Raw and extracted government datasets should generally not be committed to GitHub. The `.gitignore` file should exclude large raw data files and generated datasets where appropriate.

---

# 5. Python Modules

## 5.1 WebCrawler_real_estate.py

Responsible for the ingestion layer of the real-estate transaction pipeline.

### Main functions

- Download quarterly ZIP files.
- Validate ZIP integrity.
- Extract only required files.
- Identify A/B transaction datasets.
- Validate required files.
- Prevent unnecessary re-downloads.
- Avoid processing future quarters.

### Workflow

```text
Download
   |
ZIP validation
   |
Selective extraction
   |
File identification
   |
File validation
   |
Extracted A/B CSV files
```

### ZIP validation

Downloaded content is checked using:

```python
zipfile.is_zipfile()
```

This prevents invalid responses, unavailable future periods, or unexpected server responses from being treated as valid datasets.

### Selective extraction

The pipeline extracts relevant files, including:

- `manifest.csv`
- Required schema files
- `*_lvr_land_a.csv`
- `*_lvr_land_b.csv`

Rental transaction data (`*_lvr_land_c.csv`) is currently excluded.

---

## 5.2 analysis.py

Responsible for transaction-level data cleaning, validation, transformation, analytical filtering, and quarterly aggregation.

### Workflow

```text
Raw A/B CSV files
       |
       v
Load transaction datasets
       |
       v
Validate schema
       |
       v
Investigate duplicate records
       |
       v
Remove exact duplicates
       |
       v
Convert data types
       |
       v
Check transaction-period coverage
       |
       v
Validate data quality
       |
       v
Filter residential transactions
       |
       v
Check future-dated records
       |
       v
Filter analysis-eligible transactions
       |
       v
Filter six municipalities
       |
       v
Combine A/B analytical datasets
       |
       v
Quarterly aggregation
       |
       v
CSV and Parquet output
```

### Transaction categories

The analysis currently processes:

- **A: 不動產買賣** — Real estate sales
- **B: 預售屋買賣** — Pre-sale housing transactions

### Data validation and cleaning

The analysis module performs the following checks and transformations:

- Validate required columns and transaction types.
- Investigate exact duplicate records.
- Investigate duplicate transaction identifiers.
- Remove exact duplicate records based on the configured deduplication fields.
- Convert transaction dates and numeric fields.
- Preserve raw values for selected numeric fields to support data-quality investigation.
- Check quarterly transaction-period coverage.
- Report missing or invalid dates and prices.
- Inspect future-dated residential transactions.
- Check missing and non-positive residential unit prices.

Duplicate transaction identifiers are investigated separately and are not automatically treated as proof that records should be removed.

### Residential transaction filtering

The analysis filters transactions to the configured residential transaction categories before applying the analysis eligibility criteria.

Analysis eligibility is based on:

- The configured analysis date range.
- Valid transaction dates.
- Positive unit prices.

The current configured analysis period is **2016Q1–2026Q3**, subject to the availability and quality of source records.

### Six-municipality analysis

The analysis filters records for:

- 臺北市
- 新北市
- 桃園市
- 臺中市
- 臺南市
- 高雄市

The municipality is extracted from the transaction address.

### Handling government data-quality issues

Government open data may contain inconsistent rows or schema irregularities.

For example, one B transaction record was identified with 32 comma-separated fields while the official header contained 31 fields.

Rather than silently using:

```python
on_bad_lines="skip"
```

the pipeline investigates malformed rows before applying the configured normalization rule.

This approach is intended to prevent valid transaction records from being silently discarded.

### Quarterly aggregation

The eligible six-municipality transaction data is aggregated by:

- `period`
- `municipality`
- `transaction_type`

The quarterly output includes:

```text
period
municipality
transaction_type
avg_unit_price
median_unit_price
transaction_count
data_status
```

The aggregation provides average unit price, median unit price, and transaction count for each quarterly group.

Quarterly groups without eligible transactions are retained in the output with a transaction count of zero and an appropriate status. Price statistics remain missing when no eligible transactions are available.

### Output

The quarterly analytical dataset is exported in CSV and Parquet formats for downstream analysis.

The expected complete output structure for the current configuration contains 516 combinations:

```text
43 quarters × 6 municipalities × 2 transaction types = 516 rows
```

This is the expected number of quarterly groups, not the number of actual transactions. Actual transaction counts vary by quarter, municipality, and transaction type.

---

## 5.3 WebCrawler_hpi.py

Responsible for retrieving and transforming the official Housing Price Index.

### Workflow

```text
MOI HPI webpage
       |
       v
HTML table extraction
       |
       v
Identify target HPI table
       |
       v
ROC year / quarter conversion
       |
       v
Wide-to-long transformation
       |
       v
Numeric conversion
       |
       v
Validation
       |
       v
housing_price_index.csv / .parquet
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

## 5.4 WebCrawler_cpi.py

Responsible for retrieving CPI data from the DGBAS API.

### Workflow

```text
DGBAS API
    |
    v
JSON response
    |
    v
Extract total CPI
    |
    v
Monthly data
    |
    v
Quarterly aggregation
    |
    v
Validation
    |
    v
cpi.csv / .parquet
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

The latest available quarter depends on source-data availability and completeness.

---

## 5.5 Real_HPI.py

Combines HPI and CPI to calculate an inflation-adjusted Housing Price Index.

Because the official HPI uses:

```text
2016 annual average = 100
```

the CPI is normalized to the same base.

### Calculation

First calculate the 2016 annual CPI base:

```python
CPI_2016_base = mean(2016 quarterly CPI)
```

Then normalize CPI:

```text
CPI_2016_base100 = CPI / CPI_2016_base × 100
```

Calculate Real HPI:

```text
Real HPI = HPI / CPI_2016_base100 × 100
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

- **HPI** measures nominal housing-price movements relative to the HPI base period.
- **CPI** measures general consumer price movements.
- **Real HPI** adjusts housing-price movements for general inflation.

Real HPI is used as a supporting macroeconomic indicator rather than as an actual transaction price.

---

# 6. Data Engineering Design

The project applies several data engineering practices.

## 6.1 Modular ETL

Each data source has a dedicated module.

| Module | Responsibility |
|---|---|
| `WebCrawler_real_estate.py` | Real-estate transaction data acquisition and extraction |
| `analysis.py` | Transaction data cleaning, validation, filtering, transformation, and quarterly aggregation |
| `WebCrawler_hpi.py` | Housing Price Index acquisition and transformation |
| `WebCrawler_cpi.py` | Consumer Price Index acquisition and quarterly aggregation |
| `Real_HPI.py` | HPI/CPI integration and Real HPI calculation |
| `main.py` | Project entry point, subject to the current implementation |

## 6.2 Data Quality and Validation

The pipeline includes checks for:

- Required columns and schema consistency.
- Exact duplicate transaction records.
- Duplicate transaction identifiers.
- Invalid or missing transaction dates.
- Invalid or missing numeric values.
- Quarterly data coverage.
- Future-dated residential transactions.
- Missing and non-positive unit prices.
- Quarterly aggregation output.

These checks improve transparency and help identify data-quality issues before downstream analysis.

## 6.3 Analytical Storage

Processed analytical datasets are exported in CSV and Parquet formats.

CSV supports inspection and interoperability, while Parquet provides a columnar format suitable for downstream analytical workloads.

## 6.4 Reproducibility

The project separates source-data acquisition from data cleaning and analytical transformation. This modular structure allows each processing stage to be rerun and inspected independently.

---

# 7. Python Environment Setup

## 7.1 Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 7.2 Install Python 3.11

```bash
uv python install 3.11
```

## 7.3 Set Up the Virtual Environment

```bash
uv venv --python 3.11
```

## 7.4 Install Project Dependencies

```bash
uv sync
```

The dependencies should be defined in `pyproject.toml` and maintained using the project's dependency management workflow.

## 7.5 Run the Pipeline

Run the relevant Python modules using the entry points currently implemented in the project.

For example:

```bash
python WebCrawler_real_estate.py
python analysis.py
python WebCrawler_hpi.py
python WebCrawler_cpi.py
python Real_HPI.py
```

These commands assume that the module filenames and execution entry points match the current project. Run the modules in the appropriate order and ensure their required input files are available.

## 7.6 Environment Variables

If the project requires environment variables for local, Docker, or production execution, configure them according to the project's actual deployment setup.

For example, if `genenv.py` is part of the current project:

```bash
ENV=DEV python genenv.py
ENV=DOCKER python genenv.py
ENV=PRODUCTION python genenv.py
```

These commands are only applicable when `genenv.py` and the corresponding environment configurations are present.