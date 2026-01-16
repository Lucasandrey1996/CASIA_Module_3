-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Loading the Fact tables in the Gold layer 
-- MAGIC ## Connecting to the Gold layer (Target)

-- COMMAND ----------

USE CATALOG levkiwi_lakehouse;
USE SCHEMA gold;

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _tmp_fact_sales AS
SELECT
    CAST(soh.sales_order_id AS INT) AS sales_order_id,
    CAST(sod.sales_order_detail_id AS INT) AS sales_order_detail_id,

    --
    10000 * YEAR(soh.order_date) + 100 * MONTH(soh.order_date) + DAY(soh.order_date) AS _tf_dim_calendar_id,
    COALESCE(cust._tf_dim_customer_id, -9) AS _tf_dim_customer_id,
    COALESCE(geo._tf_dim_geography_id, -9) AS _tf_dim_geography_id,

    --
    COALESCE(TRY_CAST(sod.order_qty AS SMALLINT), 0) AS sales_order_qty,
    COALESCE(TRY_CAST(sod.unit_price AS DECIMAL(19,4)), 0) AS sales_unit_price,
    COALESCE(TRY_CAST(sod.unit_price_discount AS DECIMAL(19,4)), 0) AS sales_unit_price_discount,
    COALESCE(TRY_CAST(sod.line_total AS DECIMAL(38, 6)), 0) AS sales_line_total

  FROM silver.sales_order_detail sod
    LEFT OUTER JOIN silver.sales_order_header soh 
      ON sod.sales_order_id = soh.sales_order_id AND soh._tf_valid_to IS NULL
      LEFT OUTER JOIN silver.customer c 
        ON soh.customer_id = c.customer_id AND c._tf_valid_to IS NULL
        LEFT OUTER JOIN gold.dim_customer cust
          ON c.customer_id = cust.cust_customer_id
      LEFT OUTER JOIN silver.address a 
        ON soh.bill_to_address_id = a.address_id AND a._tf_valid_to IS NULL
        LEFT OUTER JOIN gold.dim_geography geo 
          ON a.address_id = geo.geo_address_id
  WHERE sod._tf_valid_to IS NULL;

SELECT * FROM _tmp_fact_sales;

-- COMMAND ----------

MERGE INTO gold.fact_sales AS tgt
USING _tmp_fact_sales AS src
ON tgt.sales_order_detail_id = src.sales_order_detail_id 
  AND tgt.sales_order_id = src.sales_order_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt._tf_dim_calendar_id != src._tf_dim_calendar_id OR
    tgt._tf_dim_customer_id != src._tf_dim_customer_id OR
    tgt._tf_dim_geography_id != src._tf_dim_geography_id OR
    tgt.sales_order_qty != src.sales_order_qty OR
    tgt.sales_unit_price != src.sales_unit_price OR
    tgt.sales_unit_price_discount != src.sales_unit_price_discount OR
    tgt.sales_line_total != src.sales_line_total
) THEN 
  
  UPDATE SET 
    _tf_dim_calendar_id = src._tf_dim_calendar_id,
    _tf_dim_customer_id = src._tf_dim_customer_id,
    _tf_dim_geography_id = src._tf_dim_geography_id,
    tgt.sales_order_qty = src.sales_order_qty,
    tgt.sales_unit_price = src.sales_unit_price,
    tgt.sales_unit_price_discount = src.sales_unit_price_discount,
    tgt.sales_line_total = src.sales_line_total,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    tgt.sales_order_id,
    tgt.sales_order_detail_id,
    tgt._tf_dim_calendar_id,
    tgt._tf_dim_customer_id,
    tgt._tf_dim_geography_id,
    tgt.sales_order_qty,
    tgt.sales_unit_price,
    tgt.sales_unit_price_discount,
    tgt.sales_line_total,
    tgt._tf_create_date,
    tgt._tf_update_date
  )
  VALUES (
    src.sales_order_id,
    src.sales_order_detail_id,
    src._tf_dim_calendar_id,
    src._tf_dim_customer_id,
    src._tf_dim_geography_id,
    src.sales_order_qty,
    src.sales_unit_price,
    src.sales_unit_price_discount,
    src.sales_line_total,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  );
