# Databricks notebook source
from pyspark.sql import SparkSession    
from pyspark.sql.functions import(col, when, hash as hashforkey, upper, concat)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_factSales = spark.read.option("header", True).option("inferschema", True).csv("/FileStore/tables/fact_sales_data_v2.csv")

# COMMAND ----------

df_Store = df_factSales.select([
    col("StoreRegion"),
    col("StoreName"),
    col("StoreType")
]).distinct()

# COMMAND ----------

df_Store_clean = df_Store.select([
    when(col("StoreRegion").isNotNull(), col("StoreRegion")).otherwise("N/A").alias("StoreRegion"),
    when(col("StoreName").isNotNull(), col("StoreName")).otherwise("N/A").alias("StoreName"),
    when(col("StoreType").isNotNull(), col("StoreType")).otherwise("N/A").alias("StoreType")
])

# COMMAND ----------

df_Store1 = df_Store_clean.withColumn(
    "DIM_StoreId",
    hashforkey(
        upper(concat(col("StoreRegion"), col("StoreName"), col("StoreType")))
    ).cast("bigInt")
) 

# COMMAND ----------

df_base = spark.createDataFrame([
    ("N/A", "N/A", "N/A", -1)
], ["StoreRegion", "StoreName", "StoreType", "Dim_ProductId"])

# COMMAND ----------

df_final = df_Store1.union(df_base)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/FileStore/tables/dim_store")

# COMMAND ----------

df_result = spark.read.format("delta").load("/FileStore/tables/dim_store")

# COMMAND ----------

display(df_result)