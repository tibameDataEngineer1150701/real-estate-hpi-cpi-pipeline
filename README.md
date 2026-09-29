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

### Housing Price Index

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

### Consumer Price Index

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