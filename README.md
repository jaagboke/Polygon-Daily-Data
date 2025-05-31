# 📊 Polygon Daily Data Pipeline

This project is an end-to-end data engineering solution designed to extract, store, process, and visualize financial data from the [Polygon.io](https://polygon.io) API. It demonstrates cloud-native data pipeline orchestration using Apache Airflow, scalable processing via Azure Databricks, secure storage in Azure Data Lake Gen2, and reporting with Power BI.

---

## 🧱 Architecture

![Architecture Diagram](./4f13297c-ed59-4795-a6ab-dc6ededcab29.png)

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

## 📂 Project Structure

