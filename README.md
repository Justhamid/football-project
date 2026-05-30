# ⚽ Football Market Value — Data Engineering Project

> **Problématique** : Quels facteurs influencent la valeur marchande d'un joueur de football, et comment évolue-t-elle selon l'âge, les performances et le championnat ?

---

## 📋 Table des matières

- [Contexte](#contexte)
- [Architecture](#architecture)
- [Sources de données](#sources-de-données)
- [Stack technique](#stack-technique)
- [Structure du projet](#structure-du-projet)
- [Installation et lancement](#installation-et-lancement)
- [Pipeline de données](#pipeline-de-données)
- [Optimisations](#optimisations)
- [Visualisations](#visualisations)
- [Difficultés rencontrées](#difficultés-rencontrées)

---

## Contexte

Ce projet a été réalisé dans le cadre du module **Data Engineering & Architectures Distribuées**.

L'objectif est de construire une chaîne complète de traitement de données autour du football, permettant d'analyser les facteurs qui influencent la **valeur marchande des joueurs professionnels**.

---

## Architecture

Le projet suit une architecture **Médaillon (ELT)** en 3 zones :

```
┌─────────────────────────────────────────────────────────┐
│                    ARCHITECTURE MÉDAILLON                │
│                                                         │
│  CSV (Kaggle)                                           │
│      ↓                                                  │
│  🥉 BRONZE (MinIO)     ← Données brutes, non modifiées  │
│      ↓  Spark Job 1                                     │
│  🥈 SILVER (MinIO)     ← Données nettoyées en Parquet   │
│      ↓  Spark Job 2                                     │
│  🥇 GOLD (PostgreSQL)  ← Tables analytiques             │
│      ↓                                                  │
│  📊 Metabase           ← Dashboards & visualisations    │
└─────────────────────────────────────────────────────────┘
```

### Pourquoi l'architecture Médaillon ?

- **Bronze** : conservation des données brutes pour la traçabilité et la reprise en cas d'erreur
- **Silver** : données nettoyées et optimisées en Parquet pour des lectures rapides
- **Gold** : tables pré-agrégées pour des requêtes analytiques performantes dans Metabase

---

## Sources de données

| Dataset | Source | Format | Taille | Description |
|---------|--------|--------|--------|-------------|
| Transfermarkt | Kaggle | CSV | ~700 MB | Valeurs marchandes, clubs, joueurs, championnats |
| FIFA 23 Players | Kaggle | CSV → Parquet | ~5 GB | Notes FIFA, statistiques techniques des joueurs |

### Fichiers utilisés

**Transfermarkt :**
- `players.csv` — informations des joueurs (âge, poste, nationalité)
- `player_valuations.csv` — historique des valeurs marchandes
- `clubs.csv` — informations des clubs
- `competitions.csv` — championnats
- `appearances.csv` — statistiques de match (buts, passes, minutes)

**FIFA 23 :**
- `male_players.csv` — notes FIFA (overall, potential, wage)

---

## Stack technique

| Composant | Outil | Version | Rôle |
|-----------|-------|---------|------|
| Stockage objet | MinIO | latest | Zone Bronze et Silver (S3-compatible) |
| Base de données | PostgreSQL | 15 | Zone Gold (tables analytiques) |
| Traitement | Apache Spark | 3.5.1 | Jobs de transformation ELT |
| Orchestration | Apache Airflow | 2.9.3 | DAG et scheduling du pipeline |
| Visualisation | Metabase | latest | Dashboards et graphiques |
| Conteneurisation | Docker | - | Environnement reproductible |

---

## Structure du projet

```
football-project/
│
├── data/
│   └── raw/                          # Données brutes Kaggle
│       ├── Transfermarkt/
│       └── FIFA 23 Players/
│
├── spark/
│   └── jobs/
│       ├── bronze_to_silver.py       # Job Spark : CSV → Parquet (nettoyage)
│       └── silver_to_gold.py         # Job Spark : Parquet → PostgreSQL (agrégations)
│
├── airflow/
│   └── dags/
│       └── football_pipeline_dag.py  # DAG Airflow (orchestration)
│
├── upload_to_bronze.py               # Script upload CSV → MinIO
├── docker-compose.yml                # Environnement Docker complet
├── requirements.txt                  # Dépendances Python
└── README.md                         # Documentation
```

---

## Installation et lancement

### Prérequis

- Docker Desktop installé et lancé
- Python 3.8+
- Compte Kaggle pour télécharger les datasets

### 1. Cloner le dépôt

```bash
git clone https://github.com/<ton-username>/football-project.git
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

Placer les fichiers dans `data/raw/Transfermarkt/` et `data/raw/FIFA 23 Players/`

### 4. Lancer l'environnement Docker

```bash
docker-compose up -d
```

Attendre 3-4 minutes que tous les services démarrent.

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO | http://localhost:9003 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |
| Metabase | http://localhost:3000 | à configurer |

### 5. Uploader les données dans MinIO

```bash
python upload_to_bronze.py
```

### 6. Lancer le pipeline

Aller sur http://localhost:8081, activer le DAG `football_market_value_pipeline` et cliquer sur **Trigger DAG ▶**.

---
## Ingestion des données

### Pourquoi l'upload est séparé du DAG ?

L'upload vers la zone Bronze (`upload_to_bronze.py`) est 
volontairement séparé du DAG Airflow pour une raison simple :
les données sources (Transfermarkt + FIFA 23) proviennent de 
fichiers statiques Kaggle qui ne changent pas quotidiennement.

Intégrer l'upload dans le DAG signifierait re-uploader 
les mêmes 6.8 GB de CSV à chaque exécution quotidienne — 
inutile et coûteux en ressources.

Le DAG vérifie simplement que les fichiers sont présents 
dans Bronze via `check_data_available` avant de lancer 
les jobs Spark. Si les fichiers sont absents, le pipeline 
s'arrête avec un message d'erreur clair.

**En production avec des données dynamiques** (API 
Transfermarkt temps réel, nouvelles valorisations 
hebdomadaires), cette tâche serait intégrée dans le DAG 
comme première étape d'ingestion automatique.

---

### Pourquoi la tâche `pipeline_success` ?

La tâche `pipeline_success` est un point de contrôle final 
explicite. Elle confirme que toutes les étapes précédentes 
se sont bien exécutées de bout en bout.

Sans elle, le DAG se terminerait sur `silver_to_gold` sans 
confirmation explicite du succès global. Avec elle, on a 
une validation claire dans l'interface Airflow — visible 
en vert uniquement si tout s'est bien passé.

**En production** elle déclencherait :
- Une notification email/Slack de confirmation
- Un log dans un système d'audit
- Le déclenchement d'autres pipelines en aval

---

## Pipeline de données

### DAG Airflow

```
check_minio_buckets → check_data_available → bronze_to_silver → silver_to_gold → pipeline_success
```

| Tâche | Type | Description |
|-------|------|-------------|
| `check_minio_buckets` | PythonOperator | Vérifie/crée les buckets MinIO |
| `check_data_available` | PythonOperator | Vérifie la présence des CSV |
| `bronze_to_silver` | PythonOperator | Lance le job Spark de nettoyage |
| `silver_to_gold` | PythonOperator | Lance le job Spark d'agrégation |
| `pipeline_success` | PythonOperator | Notification de fin |

### Job 1 : Bronze → Silver (`bronze_to_silver.py`)

Transformations appliquées :
- **Déduplication** : `dropDuplicates()` sur les clés primaires
- **Filtrage** : suppression des lignes nulles sur les colonnes critiques
- **Typage** : cast des colonnes vers les bons types (FloatType, IntegerType)
- **Nettoyage** : `trim()` sur les chaînes, `na.fill(0)` sur les métriques
- **Calcul** : âge calculé depuis `date_of_birth`
- **Format** : sauvegarde en **Parquet** partitionné par année pour les valuations

### Job 2 : Silver → Gold (`silver_to_gold.py`)

Transformations appliquées :
- **Jointures** : players ↔ FIFA ↔ valuations ↔ competitions ↔ appearances
- **Window Functions** : `rank()` pour extraire la dernière valeur marchande
- **Agrégations** : `avg()`, `max()`, `count()` par poste, championnat, âge, nationalité
- **Tables de faits** : `fact_player_value` avec 31,507 lignes
- **Dimensions** : `dim_player`, `dim_club`, `dim_competition`

### Tables produites dans PostgreSQL

| Table | Lignes | Description |
|-------|--------|-------------|
| `dim_player` | 47,637 | Dimension joueurs enrichie FIFA |
| `dim_club` | 796 | Dimension clubs |
| `dim_competition` | 67 | Dimension championnats |
| `fact_player_value` | 31,507 | Table de faits centrale |
| `agg_value_by_position` | 5 | Agrégation par poste |
| `agg_value_by_league` | 32 | Agrégation par championnat |
| `agg_value_by_age` | 24 | Agrégation par âge |
| `agg_top_players` | 50 | Top 50 joueurs |
| `agg_value_by_nationality` | 30 | Agrégation par nationalité |

---

## Optimisations

### Formats de fichiers

| Zone | Format | Justification |
|------|--------|---------------|
| Bronze | CSV | Format brut universel, lecture directe depuis Kaggle |
| Silver | Parquet | Format columnar : compression ~5x, lectures 10x plus rapides |
| Gold | PostgreSQL | Requêtes SQL optimisées pour Metabase |

### Partitionnement

Les `player_valuations` sont partitionnées par **année** (`valuation_year`) :
```python
df.write.partitionBy("valuation_year").parquet(path)
```
→ Si on filtre sur une année, Spark ne lit que le sous-dossier correspondant.

### Optimisations Spark

- **`inferSchema`** : détection automatique des types à la lecture
- **`limit(50000)`** sur FIFA 23 : évite de charger 5 GB en mémoire
- **Window Functions** : calcul de la dernière valeur sans groupBy coûteux
- **Tables pré-agrégées** en Gold : Metabase ne recalcule pas à chaque requête

---

## Visualisations

Les dashboards Metabase répondent à la problématique métier :

1. **Top joueurs par valeur marchande** — Bar chart des 50 joueurs les plus valorisés
2. **Valeur moyenne par poste** — Comparaison Attaquant vs Milieu vs Défenseur vs Gardien
3. **Évolution valeur par âge** — Courbe montrant le pic de valeur (~25 ans)

---

## Difficultés rencontrées

| Problème | Solution |
|----------|----------|
| Image `bitnami/spark` indisponible | Migration vers `apache/spark:3.5.1-python3` |
| Port 9000 occupé par Zscaler VPN | Remappage MinIO sur ports 9002/9003 |
| Bug `flask-session` Airflow 2.8.1 | Upgrade vers Airflow 2.9.3 |
| `docker exec` inaccessible depuis Airflow | Montage du socket Docker `/var/run/docker.sock` |
| `ClassCastException` jointures Spark | Renommage explicite des colonnes avant jointure |
| Fichier FIFA 23 de 5.3 GB | Limitation à 50,000 lignes avec `.limit()` |
| Dossier Ivy2 manquant dans Spark | Création manuelle avec permissions root |

---

## Auteur

Projet réalisé dans le cadre du module Data Engineering & Architectures Distribuées.