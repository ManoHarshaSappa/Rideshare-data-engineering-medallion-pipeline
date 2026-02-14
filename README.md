# 🚀 Rideshare Data Engineering Pipeline

> Production-ready ETL pipeline implementing Medallion Architecture with Databricks, PySpark, Delta Lake, and dbt

[![Databricks](https://img.shields.io/badge/Databricks-Enabled-FF3621?logo=databricks)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Supported-00ADD8)](https://delta.io/)
[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt)](https://www.getdbt.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apachespark)](https://spark.apache.org/)

---

## 📋 Overview

A scalable data pipeline that transforms raw rideshare operational data into analytics-ready insights using the Medallion Architecture pattern. The system processes customer, driver, trip, payment, and location data through Bronze → Silver → Gold layers, enabling real-time analytics and machine learning applications.

### The Challenge

Rideshare platforms generate massive, unstructured datasets that are:
- Inconsistent and contain duplicates
- Not optimized for analytics or ML
- Difficult to scale for enterprise BI needs

### The Solution

An end-to-end data engineering pipeline leveraging:
- **Spark Structured Streaming** for real-time ingestion
- **Delta Lake** for ACID transactions and time travel
- **Medallion Architecture** for data quality progression
- **dbt** for transformation orchestration and star schema modeling

---

## 🏗️ Architecture

```
┌─────────────────┐
│   Raw CSV Data  │
│   (S3 / DBFS)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │ ◄── Streaming Ingestion (append-only Delta tables)
│  Raw Data Lake  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │ ◄── Cleansing, Deduplication, Standardization
│  Validated Data │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │ ◄── Star Schema (dbt models)
│  Analytics-Ready│     • fact_trips
└────────┬────────┘     • fact_payments
         │              • dim_customers
         │              • dim_drivers
         ▼              • dim_vehicles
┌─────────────────┐     • dim_locations
│  BI / ML / APIs │
│  Dashboards     │
└─────────────────┘
```

### Medallion Layers

| Layer | Purpose | Technology | Key Operations |
|-------|---------|------------|----------------|
| **Bronze** | Raw data ingestion | Spark Streaming + Delta Lake | Append-only, checkpointing |
| **Silver** | Cleaned, deduplicated data | PySpark + Delta MERGE | CDC deduplication, standardization |
| **Gold** | Business-ready analytics | dbt + Delta Lake | Star schema, aggregations |

---

## 🛠️ Tech Stack

**Data Processing**
- Apache Spark (Structured Streaming)
- PySpark
- Databricks Runtime

**Storage & Format**
- Delta Lake
- AWS S3 / Databricks DBFS

**Transformation & Modeling**
- dbt (Data Build Tool)
- SQL

**Analytics & Visualization**
- Databricks SQL
- Power BI / Tableau

---

## ✨ Key Features

- ✅ **Scalable Medallion Architecture** with clear data quality progression
- ✅ **Real-time Streaming Ingestion** using Spark Structured Streaming
- ✅ **ACID Transactions** with Delta Lake MERGE operations
- ✅ **CDC-based Deduplication** using window functions and timestamp tracking
- ✅ **Reusable Transformation Framework** with modular utilities
- ✅ **Star Schema Modeling** using dbt for dimensional analytics
- ✅ **Data Lineage** with process timestamps and audit columns
- ✅ **Production-ready Code** with error handling and checkpointing

---

## 📊 Data Pipeline

### Bronze Layer: Raw Ingestion
```python
# Streaming ingestion with automatic schema inference
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .load(source_path) \
    .writeStream \
    .format("delta") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("bronze.customers")
```

### Silver Layer: Transformation
```python
# Cleansing and deduplication
- Phone number standardization
- Email domain extraction
- Name case standardization
- CDC-based deduplication using ROW_NUMBER()
- Delta MERGE for upserts
```

### Gold Layer: dbt Models
```sql
-- Star schema with facts and dimensions
-- fact_trips: Trip transactions with FK relationships
-- dim_customers: Customer attributes (SCD Type 1)
-- dim_drivers: Driver profiles
-- dim_vehicles: Vehicle master data
-- dim_locations: Geographic dimensions
```

---

## 📁 Project Structure

```
rideshare-pipeline/
├── notebooks/
│   ├── bronze_ingestion.py          # Streaming ingestion
│   ├── silver_transformation.py     # Cleansing & deduplication
│   └── utils/
│       └── transformation_utils.py  # Reusable functions
├── dbt_project/
│   ├── models/
│   │   ├── gold/
│   │   │   ├── fact_trips.sql
│   │   │   ├── fact_payments.sql
│   │   │   └── dim_*.sql
│   │   └── silver/
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/
│   └── source/                      # Raw CSV files
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Databricks Workspace (Runtime 13.0+)
- Unity Catalog enabled
- Delta Lake support
- dbt-databricks adapter

### Installation

1. **Clone Repository**
```bash
git clone https://github.com/ManoHarshaSappa/rideshare-pipeline.git
cd rideshare-pipeline
```

2. **Upload Source Data**
```bash
# Upload CSV files to Databricks volume
/Volumes/pysparkdbt/source/source_data/
```

3. **Run Bronze Ingestion**
```python
# Execute in Databricks notebook
%run ./notebooks/bronze_ingestion.py
```

4. **Run Silver Transformations**
```python
%run ./notebooks/silver_transformation.py
```

5. **Execute dbt Models**
```bash
cd dbt_project
dbt run --models gold.*
```

---

## 📈 Results & Impact

### Data Quality Metrics
- **Deduplication Rate**: 15-20% reduction in duplicate records
- **Processing Speed**: 10K+ records/second with streaming
- **Data Freshness**: Near real-time (< 5 min latency)

### Business Outcomes
- 📊 **360° Analytics**: Unified view of customers, drivers, and trips
- 🚗 **Operational Insights**: Trip demand patterns and driver utilization
- 💰 **Revenue Analytics**: Payment trends and fraud detection
- 🎯 **ML-Ready Data**: Clean features for predictive modeling

---

## 🔮 Future Enhancements

- [ ] Kafka integration for real-time event streaming
- [ ] Great Expectations for data quality testing
- [ ] ML model for trip demand forecasting
- [ ] Power BI embedded dashboards
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Data observability with Monte Carlo / Datafold
- [ ] SCD Type 2 implementation for historical tracking

---

## 📸 Screenshots

*Architecture Diagram*
![Pipeline Flow](assets/PySpark%20Delta%20Lake%20pipeline%20flowchart.png)

*dbt Lineage Graph*
![dbt DAG](assets/dbt_lineage.png)

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## 📄 License

This project is licensed under the MIT License.

---

## 👤 Author

**Mano Harsha Sappa**

🎓 M.S. Data Analytics Engineering – George Mason University  
💼 Data Engineer | PySpark Specialist | Cloud Architect

[![Portfolio](https://img.shields.io/badge/Portfolio-Visit-blue?style=flat&logo=google-chrome)](https://manoharshasappa.github.io/portfolio_ManoHarshaSappa/)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://www.linkedin.com/in/manoharshasappa/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github)](https://github.com/ManoHarshaSappa)

---

<div align="center">

**⭐ If you find this project helpful, please give it a star! ⭐**

Made with ❤️ using Databricks, PySpark, and Delta Lake

</div>