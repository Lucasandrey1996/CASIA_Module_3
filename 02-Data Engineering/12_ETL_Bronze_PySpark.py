# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion in the Bronze layer
# MAGIC
# MAGIC ## Connecting to the bronze layer (Target)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG levkiwi_lakehouse;
# MAGIC USE DATABASE bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the JDBC connection to the Azure SQL Database (Source)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("loading bronze layer").getOrCreate()
jdbcHostname = "sql-datasource-dev-001.database.windows.net"
jdbcDatabase = "sqldb-adventureworks-dev-001"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
    "user" : "levkiwi-admin",
    "password" : "cas-ia2024",
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of SalesOrderDetail

# COMMAND ----------

SalesOrderDetail = spark.read.jdbc(url=jdbcUrl, table="SalesLT.SalesOrderDetail", properties=connectionProperties)
display(SalesOrderDetail)

# COMMAND ----------

SalesOrderDetail.write.mode("overwrite").saveAsTable("bronze.SalesOrderDetail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of SalesOrderHeader

# COMMAND ----------

SalesOrderHeader = spark.read.jdbc(url=jdbcUrl, table="SalesLT.SalesOrderHeader", properties=connectionProperties)
display(SalesOrderHeader)

SalesOrderHeader.write.mode("overwrite").saveAsTable("bronze.SalesOrderHeader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of Product

# COMMAND ----------

Product = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Product", properties=connectionProperties)
display(Product)

Product.write.mode("overwrite").saveAsTable("bronze.Product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of ProductCategory

# COMMAND ----------

ProductCategory = spark.read.jdbc(url=jdbcUrl, table="SalesLT.ProductCategory", properties=connectionProperties)
display(ProductCategory)

ProductCategory.write.mode("overwrite").saveAsTable("bronze.ProductCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of Address
# MAGIC
# MAGIC

# COMMAND ----------

Address = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Address", properties=connectionProperties)
display(Address)

Address.write.mode("overwrite").saveAsTable("bronze.Address")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of Customer

# COMMAND ----------

Customer = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Customer", properties=connectionProperties)
display(Customer)

Customer.write.mode("overwrite").saveAsTable("bronze.Customer")
