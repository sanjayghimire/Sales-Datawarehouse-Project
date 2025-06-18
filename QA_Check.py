# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_factSales = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales_data_v2.csv")

# COMMAND ----------

df_factSales_clean = df_factSales.select(
    when(col("UnitsSold").isNull(), 0).otherwise(col("UnitsSold").cast("int")).alias("UnitsSold"),
    when(col("UnitPrice") < 0, 0).otherwise(col("UnitPrice").cast("double")).alias("UnitPrice"),
    when(col("Discount").isNull(), 0).otherwise(col("Discount").cast("double")).alias("Discount"),
    col("SaleDate")
)

# COMMAND ----------

df_factFinal = spark.read.format("delta").load("/FileStore/tables/fact_sales")

# COMMAND ----------

display(df_factFinal)

# COMMAND ----------

df_final_check = df_factFinal.select("UnitsSold", "UnitPrice", "Discount", "SaleDate")

# COMMAND ----------

df_original_check = df_factSales_clean.select("UnitsSold", "UnitPrice", "Discount", "SaleDate")

# COMMAND ----------

df_missing = df_original_check.exceptAll(df_final_check)

# COMMAND ----------

display(df_missing)