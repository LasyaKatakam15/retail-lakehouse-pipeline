# 📦 Retail Data Lakehouse Pipeline

## 📖 Overview

This project simulates a real-world retail data engineering pipeline using a modern **Lakehouse architecture**.

Raw transactional data stored in **Amazon S3** is ingested, cleaned, and transformed in **Databricks (PySpark)** to produce analytics-ready datasets. The pipeline enables insights into **sales performance, customer behavior, and product-level KPIs**.

---

## 🏗 Architecture
'''
S3 (Raw Data Source)
↓
Bronze Layer (Raw Ingestion - Databricks PySpark)
↓
Silver Layer (Cleaned & Transformed - PySpark)
↓
Gold Layer (KPIs, Dimensions, Data Marts - Delta)
'''
---

## ⚙️ Tech Stack

- **PySpark (Databricks)** – Distributed data processing & transformations
- **Delta Lake** – Storage layer with ACID guarantees
- **Amazon S3** – Raw data storage (Bronze source)
- **Boto3** – Secure S3 access using environment-based credentials
- **Pandas** – Intermediate data handling before Spark conversion
- **Git & GitHub** – Version control

---

## 🔄 Data Pipeline

### 🥉 Bronze Layer

- Raw CSV files ingested from S3
- Data loaded using **boto3 with environment-based AWS credential management**
- Converted to Spark DataFrames via Pandas
- Metadata enrichment:
  - `ingestion_timestamp`
  - `source_file_name`
- Stored as **Delta tables** in Databricks
- Serves as the source for downstream processing

---

### 🥈 Silver Layer

- Data cleaning and standardization
- Column name normalization
- Handling null values and duplicates
- Business rule transformations
- Produces structured, reliable intermediate datasets

---

### 🥇 Gold Layer

Analytics-ready datasets designed for reporting and business insights:

#### Fact / KPI Tables

- `sales_summary`
- `revenue_by_product`
- `daily_sales_metrics`

#### Dimension Tables

- `customer_dim`
- `product_dim`
- `date_dim`

#### Data Marts

- `customer_360_view`
- `product_performance_mart`
- `store_sales_mart`

---

## ⭐ Key Features

- Layered **Lakehouse architecture** (Bronze → Silver → Gold)
- Scalable **PySpark-based ETL pipeline**
- Clear separation of **storage (S3)** and **compute (Databricks)**
- Secure credential handling using **environment variables**
- Delta Lake with **ACID transactions**
- Modular and maintainable transformations
- Analytics-ready **Gold layer design**

---

## 💡 Design Decisions

- Designed a clear separation between **data storage (S3)** and **data processing (Databricks)**
- Implemented a **layered Lakehouse architecture** using Delta tables for reliability and scalability
- Used **boto3-based ingestion** for flexible and controlled access to S3 data
- Focused on **clean, modular transformations** across Bronze, Silver, and Gold layers
- Structured the pipeline to reflect **production-style data workflows and best practices**

---

## 📁 Project Structure
'''
retail-lakehouse-pipeline/
│
├── databricks/
│ ├── bronze/
│ │ └── bronze_ingestion.py
│ │
│ ├── silver/
│ │ └── silver_transformation.py
│ │
│ └── gold/
│ └── gold_marts.py
│
├── config/
│ └── config.py
│
├── .gitignore
└── README.md
'''
---

## 🚀 How to Run

### 1. Databricks

Run scripts in order:

1. `bronze_ingestion.py` → Ingest raw data from S3
2. `silver_transformation.py` → Clean and transform data
3. `gold_kpi_pipeline.py` → Generate KPIs, dimensions, and marts

---

## 📊 Use Cases

- Retail sales analysis
- Customer behavior insights
- Product performance tracking
- Revenue trend analysis

---

## 🔮 Future Improvements

- Add orchestration (**Databricks Workflows / Airflow**)
- Add **CI/CD pipeline integration**
- Connect **Power BI / Tableau dashboards**
- Optimize **Delta Lake performance & partitioning**

---

## 👩‍💻 Author

**Lasya Katakam**
