# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, upper, concat, hash as hashforkey, when)

# COMMAND ----------

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_factSales = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales_data_v2.csv")


# COMMAND ----------

dim_product = spark.read.format("delta").load("/FileStore/tables/dim_product")
dim_employee = spark.read.format("delta").load("/FileStore/tables/dim_employee")
dim_store = spark.read.format("delta").load("/FileStore/tables/dim_store")

# COMMAND ----------

df_factSales = df_factSales.withColumn("ProductName", when(col("ProductName").isNull(), "N/A").otherwise(col("ProductName")))

# COMMAND ----------

df_factSales = df_factSales.withColumn("ProductKey",
    hashforkey(upper(concat(
        col("ProductCategory"), col("ProductName"), col("Brand")
    ))).cast("bigint"))

# COMMAND ----------

df_factSales = df_factSales.withColumn("EmployeeKey",
    hashforkey(upper(concat(
        col("SalesRep"), col("Department"), col("EmployeeRole")
    ))).cast("bigint"))

# COMMAND ----------

df_factSales = df_factSales.withColumn("StoreKey",
    hashforkey(upper(concat(
        col("StoreRegion"), col("StoreName"), col("StoreType")
    ))).cast("bigint"))

# COMMAND ----------

fact_sales_clean = df_factSales \
    .join(dim_product,(df_factSales.ProductKey == dim_product.DIM_ProductId), how = "left") \
    .join(dim_employee,(df_factSales.EmployeeKey == dim_employee.DIM_EmployeeId), how = "left") \
    .join(dim_store,(df_factSales.StoreKey == dim_store.DIM_StoreId), how = "left")

# COMMAND ----------

display(fact_sales_clean)

# COMMAND ----------

df_factFinal = fact_sales_clean.select(
    "DIM_ProductId", "DIM_EmployeeId", "DIM_StoreId",
    when(col("UnitsSold").isNull(), 0).otherwise(col("UnitsSold").cast("int")).alias("UnitsSold"),
    when(col("UnitPrice") < 0, 0).otherwise(col("UnitPrice").cast("double")).alias("UnitPrice"),
    when(col("Discount").isNull(), 0).otherwise(col("Discount").cast("double")).alias("Discount"),
    col("SaleDate"),
    (
        when(col("UnitsSold").isNull(), 0).otherwise(col("UnitsSold").cast("int")) *
        when((col("UnitPrice").isNull()) | (col("UnitPrice") < 0), 0).otherwise(col("UnitPrice").cast("double")) *
        (1 - when(col("Discount").isNull(), 0).otherwise(col("Discount").cast("double")))
    ).alias("NetRevenue")
)

# COMMAND ----------

display(df_factFinal)

# COMMAND ----------

df_factFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/fact_sales")

# COMMAND ----------

df_result = spark.read.format("delta").load("/FileStore/tables/fact_sales")

# COMMAND ----------

display(df_result)

# COMMAND ----------

missing_Product_Data = df_factFinal.join(dim_product, df_factFinal.DIM_ProductId == dim_product.DIM_ProductId, how = "left_anti")
missing_Product_Data1 = missing_Product_Data.select("DIM_ProductId").distinct()
display(missing_Product_Data1)


missing_Employee_Data = df_factFinal.join(dim_employee, df_factFinal.DIM_EmployeeId == dim_employee.DIM_EmployeeId, how = "left_anti")
missing_Employee_Data1 = missing_Employee_Data.select("DIM_EmployeeId").distinct()
display(missing_Employee_Data1)

missing_Store_Data = df_factFinal.join(dim_store, df_factFinal.DIM_StoreId == dim_store.DIM_StoreId, how = "left_anti")
missing_Store_Data1 = missing_Store_Data.select("DIM_StoreId").distinct()
display(missing_Store_Data1)