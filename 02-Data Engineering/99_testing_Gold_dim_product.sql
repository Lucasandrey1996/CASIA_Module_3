-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Test des dimensions Gold : dim_product_category et dim_product

-- COMMAND ----------

USE CATALOG lua_lakehouse;
USE SCHEMA gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test dim_product_category

-- COMMAND ----------

-- Consultation de la structure et du contenu
SELECT 
    _tf_dim_product_category_id,
    prod_cat_category_id,
    prod_cat_name,
    prod_cat_parent_category_id,
    prod_cat_level_1_id,
    prod_cat_level_1_name,
    prod_cat_level_2_id,
    prod_cat_level_2_name
FROM dim_product_category
ORDER BY prod_cat_category_id
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test dim_product

-- COMMAND ----------

-- Consultation de la structure et du contenu
SELECT 
    _tf_dim_product_id,
    prod_product_id,
    prod_name,
    prod_product_number,
    prod_color,
    prod_list_price,
    _tf_dim_product_category_id,
    prod_product_model_name
FROM dim_product
ORDER BY prod_product_id
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test de jointure dim_product ↔ dim_product_category

-- COMMAND ----------

-- Vérification de la relation entre les deux dimensions
SELECT 
    p.prod_product_id,
    p.prod_name,
    p.prod_list_price,
    pc.prod_cat_name AS category_name,
    pc.prod_cat_level_1_name AS level_1,
    pc.prod_cat_level_2_name AS level_2
FROM dim_product p
LEFT JOIN dim_product_category pc
  ON p._tf_dim_product_category_id = pc._tf_dim_product_category_id
ORDER BY p.prod_product_id
LIMIT 20;
