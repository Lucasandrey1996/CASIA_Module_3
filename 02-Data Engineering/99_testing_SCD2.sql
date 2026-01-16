-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Code to test the SCD2 in Silver

-- COMMAND ----------

USE CATALOG levkiwi_lakehouse;
USE SCHEMA bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simulate an UPDATE in source

-- COMMAND ----------

SELECT * FROM address WHERE City = 'Bothell';

-- COMMAND ----------

UPDATE address SET PostalCode = '12345', ModifiedDate = current_timestamp() WHERE City = 'Bothell';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simulate a DELETE in source

-- COMMAND ----------

SELECT * FROM address WHERE City = 'Surrey';

-- COMMAND ----------

DELETE FROM address WHERE City = 'Surrey';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simulate an INSERT in source

-- COMMAND ----------

SELECT * FROM bronze.Address ORDER BY AddressID DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Due to laziness we shall cheat and just modify the primary key, i.e. it's actually an INSERT and a DELETE.

-- COMMAND ----------

UPDATE bronze.Address SET AddressID = 11383 WHERE AddressID = 1105;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## !!! Run the ETL and come back here to test

-- COMMAND ----------

USE CATALOG levkiwi_lakehouse;
USE SCHEMA silver;

SELECT * FROM address WHERE city = 'Bothell' ORDER BY address_id, _tf_valid_from

-- COMMAND ----------

SELECT * FROM address WHERE city = 'Surrey' ORDER BY address_id, _tf_valid_from

-- COMMAND ----------

SELECT * FROM address WHERE address_id IN (11383, 1105);
