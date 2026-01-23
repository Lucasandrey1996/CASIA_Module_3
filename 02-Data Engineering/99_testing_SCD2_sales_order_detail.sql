-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Code to test the SCD2 in Silver for sales_order_detail
-- MAGIC
-- MAGIC Ce notebook permet de tester manuellement l'historisation SCD2 de la table `sales_order_detail` dans la couche Silver.

-- COMMAND ----------

USE CATALOG lua_lakehouse;
USE SCHEMA bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scénario 1 : Test de mise à jour (UPDATE)
-- MAGIC
-- MAGIC Modification d'une ligne de commande existante pour vérifier que l'historisation crée une nouvelle version.

-- COMMAND ----------

-- Consultation initiale : sélectionner quelques lignes de commande pour le test
SELECT 
    SalesOrderID,
    SalesOrderDetailID,
    OrderQty,
    UnitPrice,
    UnitPriceDiscount,
    LineTotal,
    ModifiedDate
FROM SalesOrderDetail
WHERE SalesOrderID = (SELECT MIN(SalesOrderID) FROM SalesOrderDetail)
ORDER BY SalesOrderDetailID
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Modification d'une quantité et d'un prix unitaire
-- MAGIC
-- MAGIC On modifie la quantité commandée et le prix unitaire pour déclencher l'historisation.

-- COMMAND ----------

-- Sauvegarde de l'ID pour le test (à adapter selon vos données)
-- Exemple : modification de la première ligne de détail de la première commande
UPDATE SalesOrderDetail 
SET 
    OrderQty = OrderQty + 1,
    UnitPrice = UnitPrice * 1.1,
    ModifiedDate = current_timestamp()
WHERE SalesOrderID = (SELECT MIN(SalesOrderID) FROM SalesOrderDetail)
  AND SalesOrderDetailID = (
      SELECT MIN(SalesOrderDetailID) 
      FROM SalesOrderDetail 
      WHERE SalesOrderID = (SELECT MIN(SalesOrderID) FROM SalesOrderDetail)
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scénario 2 : Test de suppression (DELETE)
-- MAGIC
-- MAGIC Suppression d'une ligne de commande pour vérifier que l'enregistrement est fermé (fermeture SCD2).

-- COMMAND ----------

-- Consultation initiale : sélectionner quelques lignes pour le test de suppression
SELECT 
    SalesOrderID,
    SalesOrderDetailID,
    OrderQty,
    ProductID,
    ModifiedDate
FROM SalesOrderDetail
WHERE SalesOrderID = (SELECT MAX(SalesOrderID) FROM SalesOrderDetail)
ORDER BY SalesOrderDetailID
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Suppression d'une ligne de détail
-- MAGIC
-- MAGIC On supprime une ligne de commande pour tester la fermeture SCD2.

-- COMMAND ----------

-- Suppression de la dernière ligne de détail de la dernière commande
DELETE FROM SalesOrderDetail
WHERE SalesOrderID = (SELECT MAX(SalesOrderID) FROM SalesOrderDetail)
  AND SalesOrderDetailID = (
      SELECT MAX(SalesOrderDetailID) 
      FROM SalesOrderDetail 
      WHERE SalesOrderID = (SELECT MAX(SalesOrderID) FROM SalesOrderDetail)
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scénario 3 : Test d'insertion (INSERT)
-- MAGIC
-- MAGIC Simulation d'une nouvelle ligne de commande (ou modification de clé composite pour simuler un INSERT).

-- COMMAND ----------

-- Consultation des dernières lignes de détail
SELECT 
    SalesOrderID,
    SalesOrderDetailID,
    OrderQty,
    ProductID,
    UnitPrice,
    ModifiedDate
FROM SalesOrderDetail
ORDER BY SalesOrderID DESC, SalesOrderDetailID DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Simulation d'insertion par modification de clé composite
-- MAGIC
-- MAGIC Pour simuler un INSERT, on modifie la clé composite (SalesOrderID + SalesOrderDetailID) d'un enregistrement existant.
-- MAGIC Cela crée un nouvel enregistrement et ferme l'ancien.

-- COMMAND ----------

-- Exemple : modification de la clé composite pour simuler un INSERT
-- ATTENTION : adapter les IDs selon vos données réelles
-- On prend un enregistrement existant et on modifie son SalesOrderDetailID
UPDATE SalesOrderDetail 
SET SalesOrderDetailID = 999999
WHERE SalesOrderID = (SELECT MIN(SalesOrderID) FROM SalesOrderDetail)
  AND SalesOrderDetailID = (
      SELECT MAX(SalesOrderDetailID) 
      FROM SalesOrderDetail 
      WHERE SalesOrderID = (SELECT MIN(SalesOrderID) FROM SalesOrderDetail)
  )
  AND SalesOrderDetailID < 999999; -- Éviter les conflits si l'ID existe déjà

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⚠️ IMPORTANT : Exécuter l'ETL Silver avant de continuer
-- MAGIC
-- MAGIC **Vous devez maintenant exécuter le script `21_ETL_Silver_SQL.sql` pour que les modifications soient historisées dans Silver.**

-- COMMAND ----------

USE CATALOG lua_lakehouse;
USE SCHEMA silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vérification des résultats après exécution de l'ETL Silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vérification du scénario UPDATE
-- MAGIC
-- MAGIC On vérifie que l'ancien enregistrement a `_tf_valid_to` défini et qu'un nouvel enregistrement existe avec `_tf_valid_to IS NULL`.

-- COMMAND ----------

-- Vérification des mises à jour (plusieurs versions pour la même clé composite)
SELECT 
    sales_order_id,
    sales_order_detail_id,
    order_qty,
    unit_price,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date,
    _tf_update_date
FROM sales_order_detail
WHERE sales_order_id = (SELECT MIN(sales_order_id) FROM sales_order_detail)
  AND sales_order_detail_id = (
      SELECT MIN(sales_order_detail_id) 
      FROM sales_order_detail 
      WHERE sales_order_id = (SELECT MIN(sales_order_id) FROM sales_order_detail)
  )
ORDER BY _tf_valid_from;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vérification du scénario DELETE
-- MAGIC
-- MAGIC On vérifie que les enregistrements supprimés ont `_tf_valid_to` défini (fermeture SCD2).

-- COMMAND ----------

-- Vérification des suppressions (enregistrements fermés)
SELECT 
    sales_order_id,
    sales_order_detail_id,
    order_qty,
    product_id,
    _tf_valid_from,
    _tf_valid_to,
    _tf_update_date
FROM sales_order_detail
WHERE sales_order_id = (SELECT MAX(sales_order_id) FROM sales_order_detail)
  AND _tf_valid_to IS NOT NULL  -- Enregistrements fermés
ORDER BY sales_order_detail_id, _tf_valid_from;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vérification du scénario INSERT
-- MAGIC
-- MAGIC On vérifie que le nouvel enregistrement (avec le SalesOrderDetailID modifié) est présent et que l'ancien est fermé.

-- COMMAND ----------

-- Vérification des insertions (nouveau SalesOrderDetailID)
SELECT 
    sales_order_id,
    sales_order_detail_id,
    order_qty,
    product_id,
    unit_price,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date
FROM sales_order_detail
WHERE sales_order_id = (SELECT MIN(sales_order_id) FROM sales_order_detail)
  AND (sales_order_detail_id = 999999 
       OR sales_order_detail_id = (
           SELECT MAX(sales_order_detail_id) 
           FROM sales_order_detail 
           WHERE sales_order_id = (SELECT MIN(sales_order_id) FROM sales_order_detail)
             AND sales_order_detail_id < 999999
       ))
ORDER BY sales_order_detail_id, _tf_valid_from;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Requêtes de validation complémentaires

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Statistiques d'historisation
-- MAGIC
-- MAGIC Vue d'ensemble du nombre de versions par ligne de commande.

-- COMMAND ----------

-- Compter le nombre de versions par clé composite
SELECT 
    sales_order_id,
    sales_order_detail_id,
    COUNT(*) AS nb_versions,
    MIN(_tf_valid_from) AS premiere_version,
    MAX(CASE WHEN _tf_valid_to IS NULL THEN _tf_valid_from ELSE NULL END) AS version_active,
    MAX(_tf_valid_to) AS derniere_fermeture
FROM sales_order_detail
GROUP BY sales_order_id, sales_order_detail_id
HAVING COUNT(*) > 1  -- Uniquement les lignes avec plusieurs versions
ORDER BY nb_versions DESC, sales_order_id, sales_order_detail_id
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vérification de la cohérence SCD2
-- MAGIC
-- MAGIC Vérification qu'il n'y a qu'une seule version active par clé composite.

-- COMMAND ----------

-- Vérifier qu'il n'y a qu'une seule version active par clé composite
SELECT 
    sales_order_id,
    sales_order_detail_id,
    COUNT(*) AS nb_versions_actives
FROM sales_order_detail
WHERE _tf_valid_to IS NULL
GROUP BY sales_order_id, sales_order_detail_id
HAVING COUNT(*) > 1;  -- Si des résultats apparaissent, il y a un problème !

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Liste des enregistrements actifs
-- MAGIC
-- MAGIC Consultation des enregistrements actifs (version courante).

-- COMMAND ----------

-- Enregistrements actifs uniquement
SELECT 
    sales_order_id,
    sales_order_detail_id,
    order_qty,
    product_id,
    unit_price,
    unit_price_discount,
    line_total,
    _tf_valid_from,
    _tf_create_date
FROM sales_order_detail
WHERE _tf_valid_to IS NULL
ORDER BY sales_order_id, sales_order_detail_id
LIMIT 50;
