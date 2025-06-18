# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import(col, hash as hashforkey, upper,when, concat)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_factSales = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales_data_v2.csv")

# COMMAND ----------

df_Product = df_factSales.select([
    col("ProductCategory"),
    col("ProductName"),
    col("Brand")
]).distinct()

# COMMAND ----------

df_Product_clean = df_Product.select(
    when(col("ProductCategory").isNotNull(), col("ProductCategory")).otherwise("N/A").alias("ProductCategory"),
    when(col("ProductName").isNotNull(), col("ProductName")).otherwise("N/A").alias("ProductName"),
    when(col("Brand").isNotNull(), col("Brand")).otherwise("N/A").alias("Brand")
)

# COMMAND ----------

df_Product1 = df_Product_clean.withColumn(
    "DIM_ProductId",
    hashforkey(
        upper(concat(col("ProductCategory"), col("ProductName"), col("Brand")))
    ).cast("bigint")
)

# COMMAND ----------

df_base = spark.createDataFrame([
    ("N/A", "N/A", "N/A", -1)
], ["ProductCategory", "ProductName", "Brand", "DIM_ProductId"])

# COMMAND ----------

df_final = df_Product1.union(df_base)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/FileStore/tables/dim_product")

# COMMAND ----------

df_result = spark.read.format("delta").load(("/FileStore/tables/dim_product"))

# COMMAND ----------

display(df_result)