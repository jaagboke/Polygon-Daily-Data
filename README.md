# 📊 Polygon Daily Data Pipeline

This project is an end-to-end data engineering solution designed to extract, store, process, and visualize financial data from the [Polygon.io](https://polygon.io) API. It demonstrates cloud-native data pipeline orchestration using Apache Airflow, scalable processing via Azure Databricks, secure storage in Azure Data Lake Gen2, and reporting with Power BI.

---

## 🧱 Architecture

![image](https://github.com/user-attachments/assets/1d8009d3-3b82-4e9b-b788-68f7d6b9c874)


---

## 🚀 Technologies Used

| Component        | Technology                              |
|------------------|------------------------------------------|
| **Source**        | Polygon.io API                          |
| **Orchestration** | Apache Airflow (via Docker)             |
| **Storage**       | Azure Data Lake Storage Gen2            |
| **Processing**    | Azure Databricks (PySpark)              |
| **Data Sink**     | Azure SQL Database                      |
| **Visualization** | Microsoft Power BI                      |

---

## 🔄 Data Flow Description

1. **📥 Data Ingestion**
   - A custom Python script queries the Polygon API daily to fetch market data.
   - Apache Airflow (running in Docker) orchestrates the daily ETL process.

2. **💾 Raw Storage**
   - The data is stored as Parquet files in Azure Data Lake Gen2 for cost-efficient, scalable storage.

3. **⚙️ Transformation**
   - A Databricks notebook mounts the ADLS Gen2 storage.
   - PySpark transformations are applied: schema enforcement, null checks, formatting, and enrichment.

4. **📤 Data Loading**
   - Cleaned and transformed data is written to Azure SQL Database using JDBC.

5. **📈 Reporting**
   - Power BI connects directly to the SQL DB for interactive dashboarding and insights.

---

## 🎯 Objectives

- Automate ingestion of daily financial data.
- Build a robust data pipeline using modern cloud tools.
- Apply scalable data transformations in Databricks.
- Centralize cleansed data in SQL DB for downstream reporting.
- Visualize key metrics in Power BI dashboards.

## 🧪 How to Run (Dev Setup)

1. **Clone the repo**  
   ```bash
   git clone https://github.com/yourusername/Polygon-Daily-Data.git
