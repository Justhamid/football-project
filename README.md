# Football Market Value — Data Engineering Project

> **Problématique** : Quels facteurs influencent la valeur marchande d'un joueur de football, et comment évolue-t-elle selon l'âge, les performances et le championnat ?

---

## Table des matières

- [Contexte et besoin métier](#contexte-et-besoin-métier)
- [Architecture Médaillon / ELT](#architecture-médaillon--elt)
- [Sources de données](#sources-de-données)
- [Stack technique](#stack-technique)
- [Structure du projet](#structure-du-projet)
- [Installation et lancement](#installation-et-lancement)
- [Pipeline de données](#pipeline-de-données)
- [Modèle de données](#modèle-de-données)
- [Traitements de données détaillés](#traitements-de-données-détaillés)
- [Contraintes d'intégrité](#contraintes-dintégrité)
- [Optimisations](#optimisations)
- [Sécurité](#sécurité)
- [Visualisations Metabase](#visualisations-metabase)
- [Difficultés rencontrées](#difficultés-rencontrées)

---

## Contexte et besoin métier

Ce projet a été réalisé dans le cadre du module **Data Engineering & Architectures Distribuées**.

Dans le monde du football professionnel, les transferts impliquent des centaines de millions d'euros. Pourtant, la valeur d'un joueur reste souvent floue — elle dépend de critères multiples : l'âge, le poste, le championnat, les performances, la perception FIFA.

**L'objectif** est de construire une infrastructure de données robuste et automatisée capable de traiter des millions de lignes pour répondre à la question : quels facteurs influencent réellement la valeur marchande d'un joueur ?

Sans cette infrastructure, les analyses resteraient manuelles, non reproductibles et impossibles à mettre à jour automatiquement.

---

## Architecture Médaillon / ELT

Le projet suit une architecture **Médaillon (ELT)** en 3 zones de qualité croissante.

```
CSV (Kaggle)
    |
    v upload_to_bronze.py (boto3)
    |
+---+----------------------------+
|   BRONZE (MinIO)               |  <- Données brutes CSV, non modifiées
|   Règle : intouchable          |
+---+----------------------------+
    |
    v Spark Job 1 : bronze_to_silver.py
    |
+---+----------------------------+
|   SILVER (MinIO)               |  <- Données nettoyées en Parquet
|   Partitionné par année        |
+---+----------------------------+
    |
    v Spark Job 2 : silver_to_gold.py
    |
+---+----------------------------+
|   GOLD (PostgreSQL)            |  <- 9 tables analytiques
|   Schéma en étoile             |
+---+----------------------------+
    |
    v SQL SELECT (role_metabase_read)
    |
+---+----------------------------+
|   METABASE                     |  <- Dashboards & visualisations
+--------------------------------+

Apache Airflow orchestre tout le pipeline (schedule : 0 6 * * *)
```

### Pourquoi ELT et pas ETL ?

En ETL classique, on transforme avant de charger. Si la transformation échoue, les données brutes sont perdues et il faut tout retélécharger.

En ELT, on charge d'abord en Bronze (brut), puis on transforme avec Spark. Si le Job 1 échoue, les CSV sont toujours en Bronze — on relance sans perte. C'est plus sûr, plus flexible, et c'est le paradigme des plateformes modernes comme Databricks.

### Pourquoi l'architecture Médaillon ?

- **Bronze** : traçabilité complète — les données brutes sont toujours disponibles
- **Silver** : performance — Parquet est 5x plus léger et 10x plus rapide que CSV
- **Gold** : accessibilité — tables pré-agrégées pour Metabase, résultats instantanés

---

## Sources de données

| Dataset | Source | Format | Taille | Description |
|---------|--------|--------|--------|-------------|
| Transfermarkt | Kaggle | CSV | ~700 MB | Valeurs marchandes réelles, clubs, joueurs, championnats |
| FIFA 23 Players | Kaggle | CSV converti Parquet | ~5.3 GB (limité à 50 000 lignes) | Notes FIFA, statistiques techniques |

### Pourquoi ces deux sources ?

Transfermarkt donne la **valeur réelle** du marché — celle que les clubs utilisent vraiment pour les transferts. FIFA 23 donne la **valeur perçue** — basée sur les algorithmes du jeu vidéo. Croiser les deux permet de valider si la perception FIFA reflète fidèlement la réalité du marché.

### Pourquoi seulement male_players de FIFA 23 ?

Le dataset FIFA 23 contient plusieurs fichiers (male_players, female_players, male_teams...). On utilise uniquement `male_players` pour deux raisons :
1. Transfermarkt couvre exclusivement le football masculin — il faut que les joueurs correspondent pour pouvoir les joindre
2. Le fichier fait 5.3 GB original — largement supérieur aux 2 GB de RAM du worker Spark. On limite à 50 000 lignes avec `.limit()`, ce qui reste statistiquement représentatif (tous les postes et nationalités sont couverts)

### Fichiers utilisés

**Transfermarkt :**
- `players.csv` — profil joueur (âge, poste, nationalité, pied)
- `player_valuations.csv` — historique des 507 814 valorisations
- `clubs.csv` — informations des 796 clubs
- `competitions.csv` — 67 championnats
- `appearances.csv` — 1 877 839 statistiques de match

**FIFA 23 :**
- `male_players.csv` — overall_rating, potential_rating, wage_eur

---

## Stack technique

| Composant | Outil | Version | Rôle | Justification |
|-----------|-------|---------|------|---------------|
| Stockage objet | MinIO | latest | Bronze et Silver | Compatible AWS S3 — même code en production cloud |
| Base de données | PostgreSQL | 15 | Zone Gold | SQL natif pour Metabase, optimal pour petites tables |
| Traitement | Apache Spark | 3.5.1 | Jobs ELT | Standard distribué — parallélisme sur 1.8M lignes |
| Orchestration | Apache Airflow | 2.9.3 | DAG et scheduling | Dépendances, retries, monitoring — cron ne suffit pas |
| Visualisation | Metabase | latest | Dashboards | Dashboards auto-rafraîchis connectés à PostgreSQL |
| Conteneurisation | Docker | - | Infrastructure | Reproductibilité totale — même environnement partout |

### Pourquoi MinIO et pas HDFS ?

MinIO est compatible avec l'API Amazon S3. Le même code Spark (`s3a://bronze/...`) fonctionne en production sur AWS S3 sans modifier une seule ligne. HDFS est plus adapté aux clusters Hadoop on-premise.

### Pourquoi Spark et pas Pandas ?

Pandas traite les données sur une seule machine en mémoire. Il aurait planté sur FIFA 23 (5.3 GB) et les 1.8M lignes d'appearances. Spark distribue le traitement en partitions sur un cluster, c'est le standard du Data Engineering en entreprise.

### Cluster Spark

- 1 Master (coordination, planification)
- 1 Worker (2 cores, 2 GB RAM)
- 2 partitions traitées en parallèle simultanément
- Contrainte RAM locale : 11 GB partagés entre 8 services Docker

---

## Structure du projet

```
football-project/
|
+-- data/
|   +-- raw/                              # Données brutes Kaggle (non versionnées)
|       +-- Transfermarkt/
|       +-- FIFA 23 Players/
|
+-- spark/
|   +-- jobs/
|       +-- bronze_to_silver.py           # Job Spark 1 : CSV -> Parquet (nettoyage)
|       +-- silver_to_gold.py             # Job Spark 2 : Parquet -> PostgreSQL
|
+-- airflow/
|   +-- dags/
|       +-- football_pipeline_dag.py      # DAG Airflow (orchestration)
|
+-- upload_to_bronze.py                   # Script upload CSV -> MinIO Bronze
+-- init_db.sql                           # Init PostgreSQL : base, tables, rôles
+-- docker-compose.yml                    # Environnement Docker complet (8 services)
+-- README.ipynb                          # Notebook de démarrage et vérification
+-- requirements.txt                      # Dépendances Python
+-- README.md                             # Documentation complète
```

---

## Installation et lancement

### Prérequis

- Docker Desktop installé et lancé
- Python 3.8+
- 11 GB de RAM disponibles
- 15 GB d'espace disque libre

### 1. Cloner le dépôt

```bash
git clone https://github.com/Justhamid/football-project.git
cd football-project
```

### 2. Créer l'environnement virtuel

```bash
python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Télécharger les données Kaggle

- [Transfermarkt Dataset](https://www.kaggle.com/datasets/davidcariboo/player-scores)
- [FIFA 23 Players Dataset](https://www.kaggle.com/datasets/stefanoleone992/fifa-23-complete-player-dataset)

Placer les fichiers dans :
```
data/raw/Transfermarkt/        <- players.csv, player_valuations.csv, etc.
data/raw/FIFA 23 Players/      <- male_players.csv
```

### 4. Lancer l'infrastructure Docker

```bash
docker-compose up -d
```

Attendre 3-4 minutes. Vérifier que tout tourne :
```bash
docker-compose ps
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO Console | http://localhost:9003 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |
| Metabase | http://localhost:3000 | à configurer |
| PostgreSQL | localhost:5432 | airflow / airflow |

### 5. Option A — Lancer via le notebook (recommandé)

```bash
jupyter notebook README.ipynb
```

Exécuter toutes les cellules dans l'ordre — le notebook fait tout automatiquement.

### 6. Option B — Lancement manuel étape par étape

```bash
# Uploader les données
python upload_to_bronze.py

# Déclencher le pipeline dans Airflow
# http://localhost:8081 -> Trigger DAG football_market_value_pipeline
```

### Pourquoi l'upload est séparé du DAG ?

L'upload vers Bronze est volontairement séparé du DAG car les données Kaggle sont statiques — elles ne changent pas quotidiennement. Intégrer l'upload dans le DAG signifierait re-uploader 6.8 GB à chaque run quotidien, ce qui est inutile et coûteux.

Le DAG vérifie la présence des fichiers via `check_data_available` avant de lancer Spark. Si les fichiers sont absents, le pipeline s'arrête avec un message clair.

En production avec des données dynamiques (API Transfermarkt temps réel), cette tâche serait une étape du DAG.

---

## Pipeline de données

### DAG Airflow

```
check_minio_buckets -> check_data_available -> check_postgres -> bronze_to_silver -> silver_to_gold -> pipeline_success
```

| Tâche | Type | Description |
|-------|------|-------------|
| `check_minio_buckets` | PythonOperator | Vérifie/crée les buckets MinIO |
| `check_data_available` | PythonOperator | Vérifie présence et non-vacuité des CSV |
| `check_postgres` | PythonOperator | Vérifie que PostgreSQL est accessible avant Spark |
| `bronze_to_silver` | PythonOperator | Lance le job Spark de nettoyage (subprocess + docker exec) |
| `silver_to_gold` | PythonOperator | Lance le job Spark d'agrégation |
| `pipeline_success` | PythonOperator | Vérifie les counts finaux et confirme la fin |

**Schedule** : `0 6 * * *` — tous les jours à 6h du matin, avant l'arrivée des analystes.

**Retries** : 1 retry automatique après 5 minutes en cas d'échec.

### Pourquoi pipeline_success ?

Sans cette tâche, le DAG se terminerait sur `silver_to_gold` sans confirmation explicite. `pipeline_success` vérifie les counts finaux dans PostgreSQL et confirme que toutes les tables sont bien écrites. En production, elle déclencherait des notifications email/Slack et d'autres pipelines en aval.

### Pourquoi subprocess + docker exec ?

Le provider Spark officiel d'Airflow (`SparkSubmitOperator`) forçait une mise à jour vers Airflow 3.x incompatible avec notre installation. On utilise `subprocess.run()` + `docker exec football_spark_master spark-submit` directement. Cela nécessite de monter le socket Docker dans le conteneur Airflow : `/var/run/docker.sock:/var/run/docker.sock`.

### Job 1 : Bronze -> Silver (`bronze_to_silver.py`)

**Rôle** : nettoyer les CSV bruts et les convertir en Parquet optimisé.

| Transformation | Méthode Spark | Justification |
|---------------|---------------|---------------|
| Suppression doublons | `dropDuplicates(["player_id"])` | Un doublon fausserait les calculs de moyenne |
| Filtrage nulls | `filter(col("player_id").isNotNull())` | Sans ID, jointure impossible |
| Correction types | `cast(IntegerType())`, `cast(FloatType())` | Spark lit tout en String depuis CSV |
| Remplacement nulls | `na.fill(0, ["goals", "assists"])` | null dans les buts casse les agrégations |
| Nettoyage chaînes | `trim(lower(col("foot")))` | " Left " et "left" seraient 2 groupes différents |
| Calcul âge | `year(current_date()) - year(col("date_of_birth"))` | Calculé dynamiquement depuis la date de naissance |
| Limitation FIFA | `.limit(50000)` | Fichier 5.3 GB > 2 GB RAM du worker |
| Partitionnement | `partitionBy("valuation_year")` | Predicate Pushdown — 8x plus rapide sur filtres annuels |
| Format Parquet | `.write.parquet()` | 5x compression, 10x plus rapide, types natifs |

### Job 2 : Silver -> Gold (`silver_to_gold.py`)

**Rôle** : construire le schéma analytique et écrire dans PostgreSQL.

Étapes :
1. Vérification PostgreSQL accessible (avant toute insertion)
2. Vérification des Parquet Silver disponibles
3. Construction des 3 dimensions (dim_player, dim_club, dim_competition)
4. Agrégation des stats de performance (1 877 839 lignes -> 1 ligne par joueur)
5. Window Function pour la dernière valorisation
6. Jointures pour construire fact_player_value
7. Calcul des 5 tables agrégées
8. Écriture dans PostgreSQL via JDBC avec role_spark_etl

---

## Modèle de données

Le projet utilise un **schéma en étoile** (Star Schema), standard du Data Warehousing, optimisé pour les requêtes analytiques.

```
         dim_player
         (47 637 lignes)
         QUI est le joueur ?
              ^
              | player_id
              |
dim_club <----+----> fact_player_value <----> dim_competition
(796)         |      (31 507 lignes)          (67 lignes)
QUI club ?    |      COMBIEN vaut-il ?        QUELLE compétition ?
              |
              v
    5 tables agrégées (5 à 50 lignes)
    pré-calculées pour Metabase
```

### Tables de dimensions — répondent à "Qui ? Quoi ?"

Les dimensions décrivent les entités. Elles ne contiennent pas de mesures à analyser.

**dim_player** (47 637 lignes) : profil joueur enrichi
- Depuis Transfermarkt : player_id, player_name, position, nationality, age, foot
- Depuis FIFA 23 : overall_rating, potential_rating, wage_eur

**dim_club** (796 lignes) : informations du club
- club_id, club_name, domestic_competition_id, squad_size, average_age

**dim_competition** (67 lignes) : informations du championnat
- competition_id, competition_name, country_name, type

### Table de faits — répond à "Combien ?"

**fact_player_value** (31 507 lignes) — TABLE CENTRALE
- player_id (lien vers dim_player)
- latest_market_value (mesure clé — valeur marchande la plus récente)
- total_goals, total_assists, total_appearances, goals_per_game
- overall_rating, potential_rating (depuis FIFA 23)
- competition_name, league_country (depuis dim_competition)

Pourquoi 31 507 et pas 47 637 ? L'INNER JOIN entre latest_valuation et dim_player élimine les joueurs sans valorisation Transfermarkt — ils ne sont pas pertinents pour l'analyse.

### Tables agrégées — pré-calculées pour Metabase

| Table | Lignes | Répond à |
|-------|--------|----------|
| `agg_value_by_position` | 5 | Les attaquants valent-ils plus que les défenseurs ? |
| `agg_value_by_league` | 32 | Quel championnat a les joueurs les plus valorisés ? |
| `agg_value_by_age` | 24 | À quel âge un joueur atteint-il son pic de valeur ? |
| `agg_top_players` | 50 | Qui sont les 50 joueurs les plus valorisés ? |
| `agg_value_by_nationality` | 30 | Quelle nationalité produit les joueurs les plus chers ? |

Metabase lit 5 lignes au lieu de 31 507 — dashboards instantanés.

---

## Traitements de données détaillés

### Pourquoi Left Join et pas Inner Join ?

| Jointure | Type | Justification |
|----------|------|---------------|
| players + FIFA 23 | LEFT JOIN | Certains joueurs peu connus n'ont pas de note FIFA — on les garde avec overall_rating = null |
| fact + player_stats | LEFT JOIN | Certains joueurs n'ont pas de matchs dans le dataset (blessés, transférés) — on les garde avec total_goals = 0 |
| fact + competitions | LEFT JOIN | Certains joueurs sans championnat lié — on les garde avec competition_name = null |
| latest_valuation + players | INNER JOIN | Un joueur DOIT avoir un profil ET une valorisation pour être dans fact_player_value |

### Pourquoi Window Function et pas groupBy + max ?

Pour extraire la dernière valorisation de chaque joueur parmi son historique :

```python
# Notre solution — Window Function : 1 seule passe sur 507 814 lignes
window = Window.partitionBy("player_id").orderBy(desc("date"))
latest = valuations \
    .withColumn("rn", rank().over(window)) \
    .filter(col("rn") == 1)

# Alternative sans Window Function : 2 scans + 1 jointure (plus lent)
max_dates = valuations.groupBy("player_id").agg(max("date").alias("max_date"))
latest = valuations.join(max_dates, (valuations["player_id"] == max_dates["player_id"]) &
                                     (valuations["date"] == max_dates["max_date"]))
```

La Window Function est plus efficace (1 seule passe) et plus robuste (gère les ex-aequo proprement).

### Pourquoi Range Partitioning et pas Hash ?

```
Range Partitioning (notre choix) :
  partitionBy("valuation_year")
  -> valuation_year=2019/ -> toutes les lignes de 2019
  -> valuation_year=2023/ -> toutes les lignes de 2023
  -> Predicate Pushdown automatique : filtre year==2023
     -> Spark lit UNIQUEMENT le dossier 2023
  -> 8x plus rapide sur les requêtes filtrées par année

Hash Partitioning (non utilisé) :
  repartition(8, col("player_id"))
  -> données éparpillées aléatoirement dans 8 fichiers
  -> Spark doit lire tous les fichiers pour un filtre par année
  -> Pas de Predicate Pushdown possible
  -> Meilleur équilibre des partitions mais inadapté à nos requêtes
```

Valuation_year a été choisie comme colonne de partition car :
- 8 valeurs distinctes équilibrées (~63 000 lignes/an)
- C'est la dimension la plus filtrée dans nos requêtes analytiques

### Pourquoi mode overwrite ?

```python
df.write.mode("overwrite").jdbc(...)
```

Nos données Kaggle sont statiques — on recalcule tout à chaque run. Le mode `append` doublerait les données à chaque exécution quotidienne d'Airflow (31 507 -> 63 014 -> 94 521...).

### Pourquoi l'API DataFrame et pas Spark SQL ?

Les deux approches produisent le même plan d'exécution Spark. On a choisi l'API DataFrame (PySpark) car elle est plus naturelle en Python, permet de chaîner les transformations de façon lisible, et facilite la réutilisation du code.

---

## Contraintes d'intégrité

### Approche choisie : validation en amont dans Spark

On n'a pas défini de contraintes PostgreSQL formelles (PRIMARY KEY, FOREIGN KEY) sur les tables créées dynamiquement par Spark via JDBC. La raison : les contraintes ralentiraient chaque INSERT individuel lors de l'écriture massive.

L'intégrité est garantie en amont dans le Job 1 :

| Contrainte | Équivalent Spark | Code |
|------------|-----------------|------|
| PRIMARY KEY | dropDuplicates | `dropDuplicates(["player_id"])` |
| NOT NULL | filter isNotNull | `filter(col("player_id").isNotNull())` |
| CHECK age | filter between | `filter(col("age").between(14, 50))` |
| CHECK market_value | filter > 0 | `filter(col("market_value_eur") > 0)` |
| UNIQUE sur combinaison | dropDuplicates | `dropDuplicates(["player_id", "date"])` |

### Contraintes CHECK dans init_db.sql

Les tables Gold créées par init_db.sql ont des contraintes CHECK PostgreSQL :

```sql
age INT CHECK (age >= 14 AND age <= 50)
latest_market_value FLOAT NOT NULL CHECK (latest_market_value > 0)
overall_rating INT CHECK (overall_rating >= 1 AND overall_rating <= 99)
squad_size INT CHECK (squad_size >= 0)
```

Ces contraintes servent de filet de sécurité supplémentaire : si une donnée incorrecte passait malgré le nettoyage Spark, PostgreSQL la rejetterait.

### Gestion des échecs d'insertion

En cas d'échec d'insertion :
1. Spark lève une exception
2. Airflow détecte via `returncode != 0`
3. Airflow attend 5 minutes et réessaie automatiquement (1 retry)
4. Silver et Bronze restent intacts — on peut relancer le Job 2 sans reprendre depuis le début

En production, on utiliserait une table temporaire avec swap atomique pour garantir qu'aucune table n'est jamais dans un état intermédiaire.

---

## Optimisations

### Format Parquet (Silver)

| Critère | CSV (Bronze) | Parquet (Silver) |
|---------|-------------|-----------------|
| Stockage | 140 MB (appearances) | ~28 MB |
| Lecture | Ligne entière | Colonne seule |
| Compression | Aucune | ~5x automatique |
| Types | Tout en String | Int, Float, Date natifs |
| Partitionnement | Impossible | Par valeur de colonne |

### Partitionnement Range par année

```
silver/player_valuations/
  valuation_year=2016/   <- ~45 000 lignes
  valuation_year=2017/   <- ~55 000 lignes
  ...
  valuation_year=2023/   <- ~52 000 lignes

Requête : filter(col("valuation_year") == 2023)
  -> Spark lit UNIQUEMENT valuation_year=2023/
  -> 52 000 lignes au lieu de 507 814
  -> 8x plus rapide (Predicate Pushdown)
```

### Tables pré-agrégées Gold

```
Sans pré-agrégation :
  Metabase -> SELECT position, AVG(market_value) FROM fact_player_value GROUP BY position
  -> 31 507 lignes scannées à chaque affichage du dashboard

Avec pré-agrégation :
  Metabase -> SELECT * FROM agg_value_by_position
  -> 5 lignes retournées instantanément
  -> Spark calcule une fois, Metabase lit toujours
```

### Window Function

1 seule passe sur 507 814 lignes au lieu de 2 scans + 1 jointure coûteuse avec groupBy + max.

### Paramètres spark-submit optimisés

```
--executor-memory 1g       -> mémoire allouée à chaque executor
--driver-memory 1g         -> mémoire du driver Spark
--conf spark.sql.shuffle.partitions=4  -> adapté à notre cluster 2 cores
```

---

## Sécurité

### Principe du moindre privilège

Deux rôles PostgreSQL distincts créés automatiquement dans `init_db.sql` :

| Rôle | Droits | Utilisé par | Justification |
|------|--------|-------------|---------------|
| `role_spark_etl` | SELECT + INSERT + UPDATE + DELETE | Apache Spark | Spark doit écrire en Gold — pas besoin de DROP TABLE |
| `role_metabase_read` | SELECT uniquement | Metabase | Metabase lit pour afficher — jamais besoin d'écrire |

Si Metabase est compromis, un attaquant peut lire les données mais ne peut pas les modifier, les supprimer ou détruire les tables.

### Isolation réseau Docker

Tous les services communiquent sur le réseau privé `football_net`. Aucun service n'est exposé directement sur Internet. MinIO écoute sur le port 9000 en interne, remappé sur 9002 en local uniquement.

### Sécurité de l'accès S3

Spark accède à MinIO avec des credentials explicites :
```python
.config("spark.hadoop.fs.s3a.access.key", "minioadmin")
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
```

Sans ces credentials, l'accès est refusé. En production, on utiliserait HashiCorp Vault pour ne pas avoir les credentials en clair dans le code.

---

## Visualisations Metabase

Connexion : http://localhost:3000

Les 3 dashboards répondent directement à la problématique :

| Dashboard | Table utilisée | Insight |
|-----------|---------------|---------|
| Top 20 joueurs par valeur | `agg_top_players` | Identification des joueurs les plus valorisés |
| Valeur moyenne par poste | `agg_value_by_position` | Attaquants valent ~2x plus que gardiens |
| Évolution valeur par âge | `agg_value_by_age` | Pic entre 24 et 26 ans, déclin après 30 |

**Insight métier clé** : corrélation confirmée entre note FIFA (overall_rating) et valeur marchande Transfermarkt. Un joueur Elite (85+) vaut en moyenne beaucoup plus qu'un joueur Faible (<65), ce qui valide l'utilisation de FIFA comme proxy pour identifier des joueurs sous-valorisés.

---

## Difficultés rencontrées

| Problème | Diagnostic | Solution |
|----------|------------|---------|
| Image `bitnami/spark` indisponible | Image retirée de Docker Hub | Migration vers `apache/spark:3.5.1-python3` |
| Port 9000 occupé (Zscaler VPN) | `netstat -ano` -> ZSATunnel.exe | Remappage MinIO sur ports 9002/9003 |
| Bug `flask-session` Airflow 2.8.1 | TypeError sur comparaison datetime | Upgrade vers Airflow 2.9.3 |
| `docker exec` inaccessible depuis Airflow | Pas accès au daemon Docker | Montage `/var/run/docker.sock:/var/run/docker.sock` |
| `ClassCastException` jointures Spark | Colonnes `player_id` ambiguës dans plan Spark | Renommage explicite : `val_player_id`, `p_player_id`, `s_player_id` |
| Fichier FIFA 23 de 5.3 GB | Worker Spark limité à 2 GB RAM | `.limit(50000)` — 50k lignes représentatives |
| Dossier Ivy2 manquant dans Spark | Image officielle Spark ne crée pas `/home/spark/` | `mkdir -p /home/spark/.ivy2` dans l'entrypoint Docker |
| `role_spark_etl` ne pouvait pas créer de tables | Droits insuffisants sur le schéma | `GRANT CREATE ON SCHEMA public TO role_spark_etl` dans `init_db.sql` |
| Worker Spark limité à 1 GiB malgré `SPARK_WORKER_MEMORY: 3g` | Variable non prise en compte par spark-submit | Passage explicite de `--executor-memory 1g` dans la commande spark-submit |

---

## Auteur

**Hamid Belhadj Kacem**
Projet réalisé dans le cadre du module Data Engineering & Architectures Distribuées — 2026
