# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://bronze@hvhistoricaldata.blob.core.windows.net",
    mount_point= "/mnt/bronze",
    extra_configs = {"fs.azure.account.key.hvhistoricaldata.blob.core.windows.net":dbutils.secrets.get(scope="databricksScope",key="hvhistoricalKey")}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

# COMMAND ----------

from datetime import timedelta, date, datetime
date = str((date.today() - timedelta(days=1)).strftime("%Y-%m-%d"))
df = spark.read.parquet(f"/mnt/bronze/{date}.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
df = df.withColumn(
    "timestamp",
    to_timestamp((col("timestamp") / 1_000_000_000).cast("long"))
)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumnRenamed("timestamp", "t_date")

# COMMAND ----------

display(df)

# COMMAND ----------

jdbcHostNname = dbutils.secrets.get(scope="databricksScope",key="hvHostName")
jdbcDatabase = dbutils.secrets.get(scope="databricksScope",key="hvDB")
jdbcPort = 1433
jdbcUrl = f"jdbc:sqlserver://{jdbcHostNname}:{jdbcPort};database={jdbcDatabase}"
connectionProperties = {
    "user": dbutils.secrets.get(scope="databricksScope",key="hvUser"),
    "password": dbutils.secrets.get(scope="databricksScope",key="hvPass"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

df.write.jdbc(url=jdbcUrl, table="hvhistoricaldb.dbo.historical", mode="overwrite", properties=connectionProperties)


# COMMAND ----------

