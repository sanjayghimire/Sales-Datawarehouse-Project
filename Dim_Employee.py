# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import(col, hash as hashforkey, upper,when, concat)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_factSales = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales_data_v2.csv")

# COMMAND ----------

df_Employee = df_factSales.select([
    col("SalesRep"),
    col("Department"),
    col("EmployeeRole")
]).distinct()


# COMMAND ----------

df_Employee_clean = df_Employee.select(
    when(col("SalesRep").isNotNull(), col("SalesRep")).otherwise("N/A").alias("SalesRep"),
    when(col("Department").isNotNull(), col("Department")).otherwise("N/A").alias("Department"),
    when(col("EmployeeRole").isNotNull(), col("EmployeeRole")).otherwise("N/A").alias("EmployeeRole")
)

# COMMAND ----------

df_Employee1 = df_Employee_clean.withColumn(
    "DIM_EmployeeId",
    hashforkey(
        upper(concat(col("SalesRep"), col("Department"), col("EmployeeRole")))
    ).cast("bigint")
)

# COMMAND ----------

df_base = spark.createDataFrame([
    ("N/A", "N/A", "N/A", -1)
], ["SalesRep", "Department", "EmployeeRole", "DIM_EmployeeId"])

# COMMAND ----------

df_final = df_base.union(df_Employee1)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/FileStore/tables/dim_employee")

# COMMAND ----------

df_result = spark.read.format("delta").load("/FileStore/tables/dim_employee")

# COMMAND ----------

display(df_result)