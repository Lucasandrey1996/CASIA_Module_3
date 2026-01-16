# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG levkiwi_lakehouse;
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS levkiwi_lakehouse.ml_sandbox;

# COMMAND ----------

# MAGIC %md
# MAGIC Upload now the data files in the ML Sandbox

# COMMAND ----------

from pyspark.sql.functions import col

file_path = f"/Volumes/levkiwi_lakehouse/ml_sandbox/data/train.csv"
train_df = spark.read.csv(file_path, header="true", inferSchema="true")
train_df = train_df.withColumn("PassengerId", col("PassengerId").cast("string")) \
                   .withColumn("VIP", col("VIP").cast("int")) \
                   .withColumn("CryoSleep", col("CryoSleep").cast("int")) \
                   .withColumn("Transported", col("Transported").cast("int")) 
                   
