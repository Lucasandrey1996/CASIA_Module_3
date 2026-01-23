# Documentation du Processus de Chargement des Données

## Vue d'ensemble

Ce document décrit le processus complet de chargement des données dans l'architecture Medallion (Bronze, Silver, Gold) pour le projet Databricks. Le processus suit une approche ETL (Extract, Transform, Load) en plusieurs étapes, avec gestion de l'historisation des données via SCD2 (Slowly Changing Dimension Type 2).

---

## Architecture Medallion

L'architecture Medallion est organisée en trois couches :

- **Bronze** : Données brutes, copie exacte de la source
- **Silver** : Données nettoyées et enrichies avec historisation (SCD2)
- **Gold** : Données agrégées et structurées pour l'analyse (modèle en étoile)

---

## Étape 1 : Initialisation (01_Init.py)

### Objectif
Créer l'infrastructure de base de données et définir la structure des tables pour chaque couche de l'architecture Medallion.

### Actions réalisées

#### 1.1 Nettoyage initial
- Suppression des bases de données existantes (bronze, silver, gold) si elles existent
- Permet de repartir sur une base propre lors de l'initialisation

#### 1.2 Création de l'architecture Medallion
- Création du catalogue `lua_lakehouse`
- Création des trois bases de données :
  - `lua_lakehouse.bronze`
  - `lua_lakehouse.silver`
  - `lua_lakehouse.gold`

#### 1.3 Création des tables Silver
Les tables Silver sont créées avec :
- **Clé primaire surrogée** : `_tf_id` (auto-incrémentée)
- **Colonnes métier** : Colonnes issues de la source
- **Colonnes techniques pour SCD2** :
  - `_tf_valid_from` : Date de début de validité de l'enregistrement
  - `_tf_valid_to` : Date de fin de validité (NULL = enregistrement actif)
  - `_tf_create_date` : Date de création
  - `_tf_update_date` : Date de dernière mise à jour

**Tables créées :**
- `silver.address`
- `silver.customer`
- `silver.customer_address`
- `silver.product`
- `silver.product_category`
- `silver.product_description`
- `silver.product_model`
- `silver.product_model_product_description`
- `silver.sales_order_header`
- `silver.sales_order_detail`

#### 1.4 Création des tables Gold

**Table de dimension : `dim_calendar`**
- Table de calendrier générée pour la période 2000-2030
- Contient des attributs temporels (année, mois, jour, semaine, trimestre fiscal, etc.)
- Clé primaire : `_tf_dim_calendar_id` (format : YYYYMMDD)

**Table de dimension : `dim_geography`**
- Clé primaire surrogée : `_tf_dim_geography_id`
- Attributs géographiques préfixés par `geo_`
- Enregistrement "N/A" initialisé avec ID = -9

**Table de dimension : `dim_customer`**
- Clé primaire surrogée : `_tf_dim_customer_id`
- Attributs clients préfixés par `cust_`
- Enregistrement "N/A" initialisé avec ID = -9

**Table de faits : `fact_sales`**
- Clé primaire surrogée : `_tf_fact_sales_id`
- Clés étrangères vers les dimensions :
  - `_tf_dim_calendar_id` → `dim_calendar`
  - `_tf_dim_customer_id` → `dim_customer`
  - `_tf_dim_geography_id` → `dim_geography`
- Mesures : quantités, prix unitaires, remises, totaux

---

## Étape 2 : Chargement Bronze (11_ETL_Bronze_SQL.sql)

### Objectif
Ingérer les données brutes depuis la source (AdventureWorks) vers la couche Bronze sans transformation.

### Principe
Copie directe des données source vers Bronze. Aucune transformation n'est appliquée.

### Tables chargées

1. **SalesOrderDetail** : Détails des commandes
   - Source : `lua_adventureworks.saleslt.SalesOrderDetail`

2. **SalesOrderHeader** : En-têtes des commandes
   - Source : `lua_adventureworks.saleslt.SalesOrderHeader`

3. **Product** : Produits
   - Source : `lua_adventureworks.saleslt.Product`

4. **ProductCategory** : Catégories de produits
   - Source : `lua_adventureworks.saleslt.ProductCategory`

5. **Address** : Adresses
   - Source : `lua_adventureworks.saleslt.Address`

6. **Customer** : Clients
   - Source : `lua_adventureworks.saleslt.Customer`

7. **CustomerAddress** : Association client ↔ adresse
   - Source : `lua_adventureworks.saleslt.CustomerAddress`

8. **ProductDescription** : Descriptions de produits
   - Source : `lua_adventureworks.saleslt.ProductDescription`

9. **ProductModel** : Modèles de produits
   - Source : `lua_adventureworks.saleslt.ProductModel`

10. **ProductModelProductDescription** : Association modèle ↔ description (culture)
   - Source : `lua_adventureworks.saleslt.ProductModelProductDescription`

### Exclusions (metadata)
Les tables de metadata ne sont **pas** chargées dans Bronze (ex. `dbo.ErrorLog`, `dbo.BuildVersion`) : on ingère uniquement les tables du schéma `SalesLT`.

### Méthode
Utilisation de `CREATE OR REPLACE TABLE ... AS SELECT *` pour une copie complète à chaque exécution.

---

## Étape 3 : Chargement Silver (21_ETL_Silver_SQL.sql)

### Objectif
Transformer et charger les données de Bronze vers Silver avec gestion de l'historisation (SCD2).

### Principe SCD2
Slowly Changing Dimension Type 2 : conservation de l'historique des changements en créant de nouveaux enregistrements pour chaque modification.

### Processus de chargement incrémental

Pour chaque table, le processus utilise deux opérations `MERGE` successives :

#### Phase 1 : Fermeture des enregistrements modifiés/supprimés

1. **Détection des modifications** :
   - Comparaison des valeurs entre Bronze (source) et Silver (cible)
   - Si différence détectée sur un enregistrement actif (`_tf_valid_to IS NULL`)

2. **Fermeture de l'ancien enregistrement** :
   - Mise à jour de `_tf_valid_to` avec la date de chargement
   - Mise à jour de `_tf_update_date`

3. **Gestion des suppressions** :
   - Si un enregistrement existe en Silver mais plus en Bronze (`WHEN NOT MATCHED BY SOURCE`)
   - Fermeture de l'enregistrement en définissant `_tf_valid_to`

#### Phase 2 : Insertion des nouveaux enregistrements

1. **Insertion des nouveaux enregistrements** :
   - Nouveaux enregistrements de Bronze
   - Nouvelles versions d'enregistrements modifiés (après fermeture de l'ancien)

2. **Initialisation des colonnes techniques** :
   - `_tf_valid_from` = date de chargement
   - `_tf_valid_to` = NULL (enregistrement actif)
   - `_tf_create_date` = date de chargement
   - `_tf_update_date` = date de chargement

### Tables traitées

#### 3.1 Table `address`
- Transformation des noms de colonnes (PascalCase → snake_case)
- Suivi des modifications sur : address_line1, address_line2, city, state_province, country_region, postal_code, rowguid, modified_date

#### 3.2 Table `customer`
- Transformation des noms de colonnes
- Suivi des modifications sur tous les attributs clients (name_style, title, first_name, middle_name, last_name, suffix, company_name, sales_person, email_address, phone, password_hash, password_salt, rowguid, modified_date)

#### 3.3 Table `sales_order_detail`
- Clé composite : `sales_order_id` + `sales_order_detail_id`
- Suivi des modifications sur : order_qty, product_id, unit_price, unit_price_discount, line_total, rowguid, modified_date

#### 3.4 Table `sales_order_header`
- Clé : `sales_order_id`
- Suivi des modifications sur tous les attributs de commande (revision_number, order_date, due_date, ship_date, status, online_order_flag, sales_order_number, purchase_order_number, account_number, customer_id, ship_to_address_id, bill_to_address_id, ship_method, credit_card_approval_code, sub_total, tax_amt, freight, total_due, comment, rowguid, modified_date)

#### 3.5 Table `customer_address`
- Clé composite : `customer_id` + `address_id`
- Suivi des modifications sur : address_type, rowguid, modified_date

#### 3.6 Table `product_category`
- Clé : `product_category_id`
- Suivi des modifications sur : parent_product_category_id, name, rowguid, modified_date

#### 3.7 Table `product_description`
- Clé : `product_description_id`
- Suivi des modifications sur : description, rowguid, modified_date

#### 3.8 Table `product_model`
- Clé : `product_model_id`
- Suivi des modifications sur : name, catalog_description, rowguid, modified_date

#### 3.9 Table `product_model_product_description`
- Clé composite : `product_model_id` + `product_description_id` + `culture`
- Suivi des modifications sur : rowguid, modified_date

#### 3.10 Table `product`
- Clé : `product_id`
- Suivi des modifications sur : l’ensemble des attributs produit (dont product_number, color, standard_cost, list_price, size, weight, product_category_id, product_model_id, sell_start_date, sell_end_date, discontinued_date, thumbnail_photo, thumbnail_photo_file_name, rowguid, modified_date)

### Note (scripts Bronze SQL vs PySpark)
Si vous utilisez `12_ETL_Bronze_PySpark.py` au lieu de `11_ETL_Bronze_SQL.sql`, vérifiez qu’il charge bien aussi les tables ajoutées (CustomerAddress, ProductDescription, ProductModel, ProductModelProductDescription), sinon l’ETL Silver échouera sur des tables Bronze manquantes.

### Variable de chargement
- `load_date` : Timestamp unique pour chaque exécution, utilisé pour `_tf_valid_from` et `_tf_valid_to`

---

## Étape 4 : Chargement Gold - Dimensions (31_ETL_Gold_Dim_SQL.sql)

### Objectif
Charger les tables de dimension dans la couche Gold à partir des données Silver nettoyées.

### Principe
- Utilisation uniquement des enregistrements actifs de Silver (`WHERE _tf_valid_to IS NULL`)
- Gestion des valeurs NULL avec `COALESCE` et valeurs par défaut 'N/A'
- Opération `MERGE` pour mise à jour incrémentale

### Tables de dimension chargées

#### 4.1 `dim_geography`
- **Source** : `silver.address` (enregistrements actifs uniquement)
- **Transformation** :
  - Mapping des colonnes avec préfixe `geo_`
  - Gestion des NULL avec 'N/A'
  - Clé de correspondance : `geo_address_id`
- **Logique MERGE** :
  - **WHEN MATCHED** : Mise à jour si différence détectée sur les attributs géographiques
  - **WHEN NOT MATCHED** : Insertion des nouvelles adresses

#### 4.2 `dim_customer`
- **Source** : `silver.customer` (enregistrements actifs uniquement)
- **Transformation** :
  - Mapping des colonnes avec préfixe `cust_`
  - Exclusion des colonnes sensibles (password_hash, password_salt)
  - Gestion des NULL avec 'N/A'
  - Clé de correspondance : `cust_customer_id`
- **Logique MERGE** :
  - **WHEN MATCHED** : Mise à jour si différence détectée sur les attributs clients
  - **WHEN NOT MATCHED** : Insertion des nouveaux clients

### Note importante
La table `dim_calendar` est créée lors de l'initialisation (01_Init.py) et ne nécessite pas de chargement incrémental car elle est générée statiquement.

---

## Étape 5 : Chargement Gold - Faits (32_ETL_Gold_Fact_SQL.sql)

### Objectif
Charger la table de faits `fact_sales` dans la couche Gold en joignant les données Silver et les dimensions Gold.

### Processus

#### 5.1 Création d'une vue temporaire `_tmp_fact_sales`
La vue assemble les données nécessaires :

**Sources :**
- `silver.sales_order_detail` (table principale)
- `silver.sales_order_header` (JOIN sur sales_order_id)
- `silver.customer` (JOIN via sales_order_header)
- `silver.address` (JOIN via bill_to_address_id)
- `gold.dim_customer` (JOIN pour obtenir la clé surrogée)
- `gold.dim_geography` (JOIN pour obtenir la clé surrogée)

**Transformations :**
- **Clé calendrier** : Calcul de `_tf_dim_calendar_id` à partir de `order_date` (format YYYYMMDD)
- **Clé client** : Récupération de `_tf_dim_customer_id` depuis `dim_customer`
- **Clé géographie** : Récupération de `_tf_dim_geography_id` depuis `dim_geography`
- **Gestion des valeurs manquantes** :
  - Clés étrangères : Utilisation de -9 (enregistrement "N/A") si non trouvé
  - Mesures : Utilisation de 0 si NULL
- **Filtrage** : Uniquement les enregistrements actifs (`_tf_valid_to IS NULL`)

**Colonnes générées :**
- `sales_order_id`, `sales_order_detail_id`
- `_tf_dim_calendar_id`, `_tf_dim_customer_id`, `_tf_dim_geography_id`
- `sales_order_qty`, `sales_unit_price`, `sales_unit_price_discount`, `sales_line_total`

#### 5.2 Chargement dans `fact_sales`
- **Clé composite** : `sales_order_id` + `sales_order_detail_id`
- **Logique MERGE** :
  - **WHEN MATCHED** : Mise à jour si différence détectée sur les clés étrangères ou les mesures
  - **WHEN NOT MATCHED** : Insertion des nouvelles lignes de commande

### Relations dimensionnelles
La table de faits est liée aux dimensions via :
- `_tf_dim_calendar_id` → `dim_calendar(_tf_dim_calendar_id)`
- `_tf_dim_customer_id` → `dim_customer(_tf_dim_customer_id)`
- `_tf_dim_geography_id` → `dim_geography(_tf_dim_geography_id)`

---

## Étape 6 : Tests SCD2 (99_testing_SCD2.sql)

### Objectif
Valider le bon fonctionnement de l'historisation SCD2 dans la couche Silver.

### Scénarios de test

#### 6.1 Test de mise à jour (UPDATE)
1. **Sélection initiale** : Consultation des adresses de la ville "Bothell" en Bronze
2. **Modification** : Mise à jour du code postal et de la date de modification
   ```sql
   UPDATE address SET PostalCode = '12345', ModifiedDate = current_timestamp() 
   WHERE City = 'Bothell';
   ```
3. **Vérification** : Après exécution de l'ETL Silver, vérifier que :
   - L'ancien enregistrement a `_tf_valid_to` défini
   - Un nouvel enregistrement existe avec `_tf_valid_to IS NULL`
   - Les deux enregistrements ont le même `address_id`

#### 6.2 Test de suppression (DELETE)
1. **Sélection initiale** : Consultation des adresses de la ville "Surrey"
2. **Suppression** : Suppression des enregistrements
   ```sql
   DELETE FROM address WHERE City = 'Surrey';
   ```
3. **Vérification** : Après exécution de l'ETL Silver, vérifier que :
   - Les enregistrements ont `_tf_valid_to` défini (fermeture)

#### 6.3 Test d'insertion (INSERT)
1. **Sélection** : Consultation des dernières adresses
2. **Simulation d'insertion** : Modification d'un ID existant (simulation)
   ```sql
   UPDATE bronze.Address SET AddressID = 11383 WHERE AddressID = 1105;
   ```
3. **Vérification** : Après exécution de l'ETL Silver, vérifier que :
   - L'ancien ID (1105) est fermé
   - Le nouvel ID (11383) est présent

### Requêtes de validation
```sql
-- Vérification des mises à jour (plusieurs versions pour un même ID)
SELECT * FROM address 
WHERE city = 'Bothell' 
ORDER BY address_id, _tf_valid_from;

-- Vérification des suppressions (enregistrements fermés)
SELECT * FROM address 
WHERE city = 'Surrey' 
ORDER BY address_id, _tf_valid_from;

-- Vérification des insertions
SELECT * FROM address 
WHERE address_id IN (11383, 1105);
```

---

## Flux de données complet

```
┌─────────────────┐
│ Source System   │
│ AdventureWorks  │
└────────┬────────┘
         │
         │ Extraction
         ▼
┌─────────────────┐
│   BRONZE Layer  │  ← 11_ETL_Bronze_SQL.sql
│  (Données brutes)│
└────────┬────────┘
         │
         │ Transformation + Historisation (SCD2)
         ▼
┌─────────────────┐
│  SILVER Layer   │  ← 21_ETL_Silver_SQL.sql
│ (Données nettoyées│
│  + Historique)  │
└────────┬────────┘
         │
         │ Agrégation + Modèle en étoile
         ▼
┌─────────────────┐
│   GOLD Layer    │
│  (Analytics)    │
│                 │
│  Dimensions:    │  ← 31_ETL_Gold_Dim_SQL.sql
│  - dim_calendar │
│  - dim_geography│
│  - dim_customer │
│                 │
│  Facts:         │  ← 32_ETL_Gold_Fact_SQL.sql
│  - fact_sales   │
└─────────────────┘
```

---

## Ordre d'exécution recommandé

1. **01_Init.py** : Initialisation de l'infrastructure (une seule fois)
2. **11_ETL_Bronze_SQL.sql** : Chargement des données brutes
3. **21_ETL_Silver_SQL.sql** : Transformation et historisation
4. **31_ETL_Gold_Dim_SQL.sql** : Chargement des dimensions
5. **32_ETL_Gold_Fact_SQL.sql** : Chargement des faits
6. **99_testing_SCD2.sql** : Tests de validation (optionnel)

---

## Colonnes techniques standardisées

Toutes les tables utilisent un préfixe `_tf_` (technical fields) pour les colonnes techniques :

- `_tf_id` : Clé primaire surrogée (Silver)
- `_tf_dim_*_id` : Clés primaires des dimensions (Gold)
- `_tf_fact_*_id` : Clés primaires des faits (Gold)
- `_tf_valid_from` : Date de début de validité (SCD2)
- `_tf_valid_to` : Date de fin de validité (SCD2, NULL = actif)
- `_tf_create_date` : Date de création de l'enregistrement
- `_tf_update_date` : Date de dernière mise à jour

---

## Bonnes pratiques observées

1. **Gestion des valeurs NULL** : Utilisation de `COALESCE` avec valeurs par défaut ('N/A' pour les dimensions, 0 pour les mesures)
2. **Clés surrogées** : Utilisation systématique de clés auto-incrémentées pour l'indépendance vis-à-vis de la source
3. **Historisation** : Implémentation SCD2 pour traçabilité complète des changements
4. **Chargement incrémental** : Utilisation de `MERGE` pour éviter les doublons et optimiser les performances
5. **Séparation des préoccupations** : Chaque couche a un rôle bien défini (raw, cleaned, aggregated)
6. **Tests** : Scripts de validation pour garantir la qualité des données

---

## Notes importantes

- ⚠️ **Attention** : Le script `01_Init.py` supprime toutes les données existantes. À utiliser uniquement lors de l'initialisation.
- 🔄 **Chargement incrémental** : Les scripts Silver et Gold utilisent `MERGE` pour un chargement incrémental efficace.
- 📊 **Modèle en étoile** : La couche Gold suit un modèle en étoile classique (dimensions + faits) optimisé pour l'analyse.
- 🔍 **Traçabilité** : L'historisation SCD2 permet de reconstituer l'état des données à n'importe quel moment dans le temps.
