-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Loading the Dim tables in the Gold layer 
-- MAGIC ## Connecting to the Gold layer (Target)

-- COMMAND ----------

USE CATALOG lua_lakehouse;
USE SCHEMA gold;

-- Timestamp de référence unique pour la session de chargement
DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

-- DIM_GEOGRAPHY
-- Source: `silver.address` (uniquement les enregistrements actifs) → normalisation + valeurs par défaut
MERGE INTO gold.dim_geography AS tgt
USING (
    SELECT
        -- Clé naturelle de la source (réutilisée comme clé de correspondance)
        CAST(address_id AS INT) AS geo_address_id,
        -- Nettoyage: cast + valeur 'N/A' par défaut si NULL
        COALESCE(TRY_CAST(address_line1 AS STRING), 'N/A') AS geo_address_line_1,
        COALESCE(TRY_CAST(address_line2 AS STRING), 'N/A') AS geo_address_line_2,
        COALESCE(TRY_CAST(city AS STRING), 'N/A') AS geo_city,
        COALESCE(TRY_CAST(state_province AS STRING), 'N/A') AS geo_state_province,
        COALESCE(TRY_CAST(country_region AS STRING), 'N/A') AS geo_country_region,
        COALESCE(TRY_CAST(postal_code AS STRING), 'N/A') AS geo_postal_code
    FROM silver.address
    -- SCD2: on ne charge en Gold que la version active
    WHERE _tf_valid_to IS NULL
) AS src
-- Alignement: mise à jour/insertion par clé adresse
ON tgt.geo_address_id = src.geo_address_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.geo_address_line_1 != src.geo_address_line_1 OR 
    tgt.geo_address_line_2 != src.geo_address_line_2 OR 
    tgt.geo_city != src.geo_city OR
    tgt.geo_state_province != src.geo_state_province OR
    tgt.geo_country_region != src.geo_country_region OR
    tgt.geo_postal_code != src.geo_postal_code
) THEN 
  
  UPDATE SET 
    tgt.geo_address_line_1 = src.geo_address_line_1,
    tgt.geo_address_line_2 = src.geo_address_line_2,
    tgt.geo_city = src.geo_city,
    tgt.geo_state_province = src.geo_state_province,
    tgt.geo_country_region = src.geo_country_region,
    tgt.geo_postal_code = src.geo_postal_code,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    geo_address_id,
    geo_address_line_1,
    geo_address_line_2,
    geo_city,
    geo_state_province,
    geo_country_region,
    geo_postal_code,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.geo_address_id,
    src.geo_address_line_1,
    src.geo_address_line_2,
    src.geo_city,
    src.geo_state_province,
    src.geo_country_region,
    src.geo_postal_code,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

-- DIM_CUSTOMER
-- Source: `silver.customer` (enregistrements actifs) → attributs “analytics-ready” (sans champs sensibles)
MERGE INTO gold.dim_customer AS tgt
USING (
    SELECT
        -- Clé naturelle de la source (réutilisée comme clé de correspondance)
        CAST(customer_id AS INT) AS cust_customer_id,
        -- Nettoyage: cast + valeur 'N/A' par défaut si NULL
        COALESCE(TRY_CAST(title AS STRING), 'N/A') AS cust_title,
        COALESCE(TRY_CAST(first_name AS STRING), 'N/A') AS cust_first_name,
        COALESCE(TRY_CAST(middle_name AS STRING), 'N/A') AS cust_middle_name,
        COALESCE(TRY_CAST(last_name AS STRING), 'N/A') AS cust_last_name,
        COALESCE(TRY_CAST(suffix AS STRING), 'N/A') AS cust_suffix,
        COALESCE(TRY_CAST(company_name AS STRING), 'N/A') AS cust_company_name,
        COALESCE(TRY_CAST(sales_person AS STRING), 'N/A') AS cust_sales_person,
        COALESCE(TRY_CAST(email_address AS STRING), 'N/A') AS cust_email_address,
        COALESCE(TRY_CAST(phone AS STRING), 'N/A') AS cust_phone
    FROM silver.customer
    -- SCD2: on ne charge en Gold que la version active
    WHERE _tf_valid_to IS NULL
) AS src
-- Alignement: mise à jour/insertion par clé client
ON tgt.cust_customer_id = src.cust_customer_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.cust_title != src.cust_title OR
    tgt.cust_first_name != src.cust_first_name OR
    tgt.cust_middle_name != src.cust_middle_name OR
    tgt.cust_last_name != src.cust_last_name OR
    tgt.cust_suffix != src.cust_suffix OR
    tgt.cust_company_name != src.cust_company_name OR
    tgt.cust_sales_person != src.cust_sales_person OR
    tgt.cust_email_address != src.cust_email_address OR
    tgt.cust_phone != src.cust_phone
) THEN 
  
  UPDATE SET 
    tgt.cust_title = src.cust_title,
    tgt.cust_first_name = src.cust_first_name,
    tgt.cust_middle_name = src.cust_middle_name,
    tgt.cust_last_name = src.cust_last_name,
    tgt.cust_suffix = src.cust_suffix,
    tgt.cust_company_name = src.cust_company_name,
    tgt.cust_sales_person = src.cust_sales_person,
    tgt.cust_email_address = src.cust_email_address,
    tgt.cust_phone = src.cust_phone,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    cust_customer_id,
    cust_title,
    cust_first_name,
    cust_middle_name,
    cust_last_name,
    cust_suffix,
    cust_company_name,
    cust_sales_person,
    cust_email_address,
    cust_phone,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.cust_customer_id,
    src.cust_title,
    src.cust_first_name,
    src.cust_middle_name,
    src.cust_last_name,
    src.cust_suffix,
    src.cust_company_name,
    src.cust_sales_person,
    src.cust_email_address,
    src.cust_phone,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

-- DIM_PRODUCT_CATEGORY
-- Source: `silver.product_category` (enregistrements actifs) → hiérarchie parent/enfant avec niveaux
MERGE INTO gold.dim_product_category AS tgt
USING (
    SELECT
        CAST(pc.product_category_id AS INT) AS prod_cat_category_id,
        COALESCE(TRY_CAST(pc.name AS STRING), 'N/A') AS prod_cat_name,
        COALESCE(TRY_CAST(pc.parent_product_category_id AS INT), 0) AS prod_cat_parent_category_id,
        -- Niveau 1: catégorie racine (parent IS NULL ou parent = 0)
        CASE 
            WHEN pc.parent_product_category_id IS NULL OR pc.parent_product_category_id = 0 
            THEN CAST(pc.product_category_id AS INT)
            ELSE CAST(parent_pc.product_category_id AS INT)
        END AS prod_cat_level_1_id,
        CASE 
            WHEN pc.parent_product_category_id IS NULL OR pc.parent_product_category_id = 0 
            THEN COALESCE(TRY_CAST(pc.name AS STRING), 'N/A')
            ELSE COALESCE(TRY_CAST(parent_pc.name AS STRING), 'N/A')
        END AS prod_cat_level_1_name,
        -- Niveau 2: sous-catégorie (si parent existe)
        CASE 
            WHEN pc.parent_product_category_id IS NOT NULL AND pc.parent_product_category_id != 0 
            THEN CAST(pc.product_category_id AS INT)
            ELSE 0
        END AS prod_cat_level_2_id,
        CASE 
            WHEN pc.parent_product_category_id IS NOT NULL AND pc.parent_product_category_id != 0 
            THEN COALESCE(TRY_CAST(pc.name AS STRING), 'N/A')
            ELSE 'N/A'
        END AS prod_cat_level_2_name
    FROM silver.product_category pc
    LEFT OUTER JOIN silver.product_category parent_pc
      ON pc.parent_product_category_id = parent_pc.product_category_id 
      AND parent_pc._tf_valid_to IS NULL
    WHERE pc._tf_valid_to IS NULL
) AS src
ON tgt.prod_cat_category_id = src.prod_cat_category_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.prod_cat_name != src.prod_cat_name OR
    tgt.prod_cat_parent_category_id != src.prod_cat_parent_category_id OR
    tgt.prod_cat_level_1_id != src.prod_cat_level_1_id OR
    tgt.prod_cat_level_1_name != src.prod_cat_level_1_name OR
    tgt.prod_cat_level_2_id != src.prod_cat_level_2_id OR
    tgt.prod_cat_level_2_name != src.prod_cat_level_2_name
) THEN
  UPDATE SET
    tgt.prod_cat_name = src.prod_cat_name,
    tgt.prod_cat_parent_category_id = src.prod_cat_parent_category_id,
    tgt.prod_cat_level_1_id = src.prod_cat_level_1_id,
    tgt.prod_cat_level_1_name = src.prod_cat_level_1_name,
    tgt.prod_cat_level_2_id = src.prod_cat_level_2_id,
    tgt.prod_cat_level_2_name = src.prod_cat_level_2_name,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  INSERT (
    prod_cat_category_id,
    prod_cat_name,
    prod_cat_parent_category_id,
    prod_cat_level_1_id,
    prod_cat_level_1_name,
    prod_cat_level_2_id,
    prod_cat_level_2_name,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.prod_cat_category_id,
    src.prod_cat_name,
    src.prod_cat_parent_category_id,
    src.prod_cat_level_1_id,
    src.prod_cat_level_1_name,
    src.prod_cat_level_2_id,
    src.prod_cat_level_2_name,
    load_date,
    load_date
  )

-- COMMAND ----------

-- DIM_PRODUCT
-- Source: `silver.product` + `silver.product_model` + `gold.dim_product_category` (enregistrements actifs) → enrichissement avec les infos de modèle et catégorie
MERGE INTO gold.dim_product AS tgt
USING (
    SELECT
        CAST(p.product_id AS INT) AS prod_product_id,
        COALESCE(TRY_CAST(p.name AS STRING), 'N/A') AS prod_name,
        COALESCE(TRY_CAST(p.product_number AS STRING), 'N/A') AS prod_product_number,
        COALESCE(TRY_CAST(p.color AS STRING), 'N/A') AS prod_color,
        COALESCE(TRY_CAST(p.standard_cost AS DECIMAL(19, 4)), 0) AS prod_standard_cost,
        COALESCE(TRY_CAST(p.list_price AS DECIMAL(19, 4)), 0) AS prod_list_price,
        COALESCE(TRY_CAST(p.size AS STRING), 'N/A') AS prod_size,
        COALESCE(TRY_CAST(p.weight AS DECIMAL(19, 4)), 0) AS prod_weight,
        COALESCE(pcat._tf_dim_product_category_id, -9) AS _tf_dim_product_category_id,
        COALESCE(TRY_CAST(p.product_model_id AS INT), 0) AS prod_product_model_id,
        COALESCE(TRY_CAST(pm.name AS STRING), 'N/A') AS prod_product_model_name,
        COALESCE(TRY_CAST(pm.catalog_description AS STRING), 'N/A') AS prod_product_model_catalog_description
    FROM silver.product p
    LEFT OUTER JOIN silver.product_model pm
      ON p.product_model_id = pm.product_model_id AND pm._tf_valid_to IS NULL
    LEFT OUTER JOIN gold.dim_product_category pcat
      ON p.product_category_id = pcat.prod_cat_category_id
    WHERE p._tf_valid_to IS NULL
) AS src
ON tgt.prod_product_id = src.prod_product_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.prod_name != src.prod_name OR
    tgt.prod_product_number != src.prod_product_number OR
    tgt.prod_color != src.prod_color OR
    tgt.prod_standard_cost != src.prod_standard_cost OR
    tgt.prod_list_price != src.prod_list_price OR
    tgt.prod_size != src.prod_size OR
    tgt.prod_weight != src.prod_weight OR
    tgt._tf_dim_product_category_id != src._tf_dim_product_category_id OR
    tgt.prod_product_model_id != src.prod_product_model_id OR
    tgt.prod_product_model_name != src.prod_product_model_name OR
    tgt.prod_product_model_catalog_description != src.prod_product_model_catalog_description
) THEN
  UPDATE SET
    tgt.prod_name = src.prod_name,
    tgt.prod_product_number = src.prod_product_number,
    tgt.prod_color = src.prod_color,
    tgt.prod_standard_cost = src.prod_standard_cost,
    tgt.prod_list_price = src.prod_list_price,
    tgt.prod_size = src.prod_size,
    tgt.prod_weight = src.prod_weight,
    tgt._tf_dim_product_category_id = src._tf_dim_product_category_id,
    tgt.prod_product_model_id = src.prod_product_model_id,
    tgt.prod_product_model_name = src.prod_product_model_name,
    tgt.prod_product_model_catalog_description = src.prod_product_model_catalog_description,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  INSERT (
    prod_product_id,
    prod_name,
    prod_product_number,
    prod_color,
    prod_standard_cost,
    prod_list_price,
    prod_size,
    prod_weight,
    _tf_dim_product_category_id,
    prod_product_model_id,
    prod_product_model_name,
    prod_product_model_catalog_description,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.prod_product_id,
    src.prod_name,
    src.prod_product_number,
    src.prod_color,
    src.prod_standard_cost,
    src.prod_list_price,
    src.prod_size,
    src.prod_weight,
    src._tf_dim_product_category_id,
    src.prod_product_model_id,
    src.prod_product_model_name,
    src.prod_product_model_catalog_description,
    load_date,
    load_date
  )
