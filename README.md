# Project Summary

## 🎯 Data Lakehouse - FAOSTAT Food Security Analysis

### Business Problem
**Question**: Which countries are at risk of food insecurity based on production and trade patterns?

**Solution**: Build an end-to-end data lakehouse analyzing global food production and trade data from FAOSTAT.

---

## 📁 Project Structure

```
local-data-lakehouse/
├── docker-compose.yml          # Full infrastructure
├── Makefile                    # Quick commands
├── setup.sh                    # Automated setup
├── init_minio.py              # MinIO initialization
├── requirements.txt
│
├── spark/
│   └── Dockerfile             # Spark with Iceberg
│
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── faostat_pipeline.py  # Main orchestration DAG
│
├── jobs/
│   ├── ingest_faostat.py        # Bronze: Data ingestion
│   ├── transform_production.py  # Silver: Clean production
│   ├── transform_trade.py       # Silver: Clean trade
│   └── analyze_food_security.py # Gold: Business metrics
│
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                   # Streamlit dashboard
│
├── SECTIONS_CHEATSHEET.md       # Quick reference
└── README.md
```

---

## 🏗️ Architecture

### Tech Stack
- **Storage**: MinIO (S3-compatible object storage)
- **Compute**: Apache Spark 3.5
- **Table Format**: Apache Iceberg
- **Catalog**: PostgreSQL (JDBC)
- **Orchestration**: Apache Airflow
- **Dashboard**: Streamlit

### Data Flow (Medallion Architecture)
```
FAOSTAT API
    ↓
[Bronze Layer] Raw data in Iceberg tables
    ↓
[Silver Layer] Cleaned, structured data
    ↓
[Gold Layer] Business metrics & aggregations
    ↓
Streamlit Dashboard
```

---

## 🚀 Quick Start

```bash
# Start all services
make start

# Run the pipeline manually
make run-pipeline

# Access services
# MinIO:    http://localhost:9001 (admin/password)
# Spark:    http://localhost:8080
# Airflow:  http://localhost:8081 (admin/admin)
# Dashboard: http://localhost:8501
```

---

## 📊 Data Pipeline

### 1. Bronze Layer (Raw Data)
- **Input**: FAOSTAT bulk downloads
  - QCL: Crops and livestock products
  - TCL: Trade data (imports/exports)
- **Output**: 
  - `lakehouse.bronze.faostat_qcl_raw`
  - `lakehouse.bronze.faostat_tcl_raw`

### 2. Silver Layer (Cleaned Data)
- **Transformations**:
  - Filter relevant elements (Production, Exports, Imports)
  - Standardize column names
  - Clean data types
  - Remove nulls
- **Output**:
  - `lakehouse.silver.crop_production`
  - `lakehouse.silver.trade_data`

### 3. Gold Layer (Business Metrics)
- **Analytics**:
  - Yearly production by crop
  - Top crops globally
  - Trade balance (exports - imports)
  - Food security indicators
- **Output**:
  - `lakehouse.gold.yearly_crop_production`
  - `lakehouse.gold.top_crops`
  - `lakehouse.gold.trade_balance`
  - `lakehouse.gold.food_security_indicators`

### Key Metrics Calculated
- **Self-Sufficiency Ratio** = Production / (Production + Imports)
- **Production Growth Rate** = (Current Year - Previous Year) / Previous Year
- **Trade Balance** = Exports - Imports
- **Net Position** = Net Exporter | Net Importer | Balanced

---

## 👨‍🎓 5-Section Teaching Guide

### Section 1: Infrastructure Setup (2-3 hours)
**Focus**: Docker, MinIO, Spark, PostgreSQL
- Start services with docker-compose
- Initialize object storage
- Understand lakehouse architecture

### Section 2: Data Ingestion - Bronze (2-3 hours)
**Focus**: Ingest raw FAOSTAT data
- Download data from FAOSTAT API
- Load into Iceberg tables
- Understand table formats and catalogs

### Section 3: Data Transformation - Silver (3-4 hours)
**Focus**: Clean and structure data
- Transform production data
- Transform trade data
- Learn medallion architecture

### Section 4: Analytics & Orchestration - Gold (3-4 hours)
**Focus**: Business metrics and automation
- Calculate food security indicators
- Create Airflow DAG
- Automate pipeline

### Section 5: Visualization (2-3 hours)
**Focus**: Build dashboard
- Create Streamlit app
- Connect to Iceberg tables
- Generate insights

**Total Duration**: 12-15 hours (4 weeks, 3-4 hours per week)

---


## 📈 Business Insights Generated

1. **Production Trends**: Which crops are growing/declining?
2. **Trade Dependencies**: Which countries rely on imports?
3. **Food Security Risk**: Countries with low self-sufficiency
4. **Regional Analysis**: Production patterns by geography
5. **Crop Priorities**: Most important crops globally

### Sample Findings
- Countries with <50% self-sufficiency are high risk
- Declining production growth indicates capacity issues
- Net importers are vulnerable to supply disruptions

---

## 🔧 Manual Pipeline Execution

```bash
# 1. Ingest production data
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest_faostat.py QCL

# 2. Transform production data
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/transform_production.py

# 3. Ingest trade data
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest_faostat.py TCL

# 4. Transform trade data
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/transform_trade.py

# 5. Analyze food security
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/analyze_food_security.py
```

---

## 🎨 Dashboard Features

### Tab 1: Production
- Top 10 crops by production
- Production trends over time
- Interactive crop selection

### Tab 2: Trade
- Top exporters and importers
- Trade balance visualization
- Country comparison

### Tab 3: Food Security
- Self-sufficiency ratios
- Countries at risk
- Trend analysis

### Tab 4: Insights
- Risk distribution pie chart
- Countries with declining production
- Summary statistics

---

## 🛠️ Troubleshooting

```bash
# View logs
make logs

# Stop all services
make stop

# Clean everything and restart
make clean
make start

# Check service status
docker ps

# Restart specific service
docker-compose restart spark-master
```

---

## 📚 Additional Resources

### Datasets
- FAOSTAT QCL: Crops and livestock production
- FAOSTAT TCL: Trade (crops and livestock)
- FAOSTAT FBS: Food balance sheets (optional extension)

### Documentation
- Apache Iceberg: https://iceberg.apache.org/
- Apache Spark: https://spark.apache.org/
- Apache Airflow: https://airflow.apache.org/
- Streamlit: https://streamlit.io/

---

## 📝 Key Takeaways

1. **Lakehouse = Best of Both Worlds**
   - Data lake flexibility + warehouse performance

2. **Medallion Architecture = Data Quality**
   - Bronze → Silver → Gold ensures clean, reliable data

3. **Iceberg = Modern Table Format**
   - ACID transactions, schema evolution, time travel

4. **Orchestration = Automation**
   - Airflow manages dependencies and scheduling

5. **Business Value = The Goal**
   - Technology serves business insights, not vice versa

---

## 🎯 Project Goal Achieved

**Question**: Which countries are at risk of food insecurity?

**Answer**: Countries with:
- Self-sufficiency ratio <50%
- Negative production growth
- High import dependency
- Low cereal production

**Evidence**: Interactive dashboard showing risk indicators with historical trends and real-time data updates.
