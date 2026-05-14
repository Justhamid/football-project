"""
JOB SPARK 1 : BRONZE → SILVER
==============================
Ce job lit les CSV bruts depuis MinIO (zone Bronze),
nettoie les données et les sauvegarde en Parquet (zone Silver).

Architecture Médaillon :
  Bronze : données brutes, non modifiées
  Silver : données nettoyées, typées, sans doublons
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, current_date, trim, lower
from pyspark.sql.types import IntegerType, FloatType, DateType
import logging

# ─────────────────────────────────────────
# CONFIGURATION SPARK + MINIO
# ─────────────────────────────────────────
# On crée la session Spark avec les connecteurs nécessaires :
#   - hadoop-aws : pour lire/écrire sur S3/MinIO
#   - aws-java-sdk : SDK AWS utilisé par hadoop-aws
spark = SparkSession.builder \
    .appName("FootballBronzeToSilver") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CHEMINS BRONZE (lecture CSV)
# ─────────────────────────────────────────
BRONZE = "s3a://bronze"
SILVER = "s3a://silver"

# ─────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────

def read_csv(path: str):
    """Lit un CSV depuis MinIO avec détection automatique du schéma."""
    logger.info(f"Lecture : {path}")
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(path)

def write_parquet(df, path: str, partition_by: str = None):
    """
    Écrit un DataFrame en Parquet dans MinIO (Silver).
    Le format Parquet est columnar : plus rapide à lire,
    meilleure compression que CSV, idéal pour Spark.
    """
    logger.info(f"Écriture Parquet : {path}")
    writer = df.write.mode("overwrite").parquet
    if partition_by:
        # Partitionnement : divise les données en sous-dossiers
        # Ex: partition par année = 1 fichier par année → lectures plus rapides
        df.write.mode("overwrite").partitionBy(partition_by).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
    logger.info(f"✅ Écrit : {path} ({df.count()} lignes)")


# ═══════════════════════════════════════════════════════
# TRAITEMENT 1 : PLAYERS (Transfermarkt)
# ═══════════════════════════════════════════════════════
logger.info("=== TRAITEMENT : players.csv ===")

players = read_csv(f"{BRONZE}/Transfermarkt/players.csv")

# Nettoyage :
# 1. On supprime les doublons sur player_id
# 2. On filtre les lignes sans player_id (inutilisables)
# 3. On nettoie les chaînes (espaces, casse)
# 4. On calcule l'âge courant depuis date_of_birth
players_clean = players \
    .dropDuplicates(["player_id"]) \
    .filter(col("player_id").isNotNull()) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("position", trim(col("position"))) \
    .withColumn("nationality", trim(col("country_of_citizenship"))) \
    .withColumn("age",
        (year(current_date()) - year(col("date_of_birth"))).cast(IntegerType())
    ) \
    .withColumn("height_cm", col("height_in_cm").cast(FloatType())) \
    .withColumn("foot", trim(lower(col("foot")))) \
    .select(
        col("player_id"),
        col("name"),
        col("position"),
        col("nationality"),
        col("age"),
        col("height_cm"),
        col("foot"),
        col("current_club_id"),
        col("last_season")
    )

write_parquet(players_clean, f"{SILVER}/players")
logger.info(f"Players nettoyés : {players_clean.count()} lignes")


# ═══════════════════════════════════════════════════════
# TRAITEMENT 2 : PLAYER_VALUATIONS (Transfermarkt)
# ═══════════════════════════════════════════════════════
logger.info("=== TRAITEMENT : player_valuations.csv ===")

valuations = read_csv(f"{BRONZE}/Transfermarkt/player_valuations.csv")

# Nettoyage :
# - On filtre les valeurs marchandes nulles ou négatives (données invalides)
# - On caste market_value_in_eur en Float
# - On extrait l'année pour le partitionnement
valuations_clean = valuations \
    .dropDuplicates(["player_id", "date"]) \
    .filter(col("player_id").isNotNull()) \
    .filter(col("market_value_in_eur").isNotNull()) \
    .filter(col("market_value_in_eur").cast(FloatType()) > 0) \
    .withColumn("market_value_eur", col("market_value_in_eur").cast(FloatType())) \
    .withColumn("valuation_year", year(col("date").cast(DateType()))) \
    .select(
        col("player_id"),
        col("date"),
        col("valuation_year"),
        col("market_value_eur"),
        col("current_club_id"),
        col("player_club_domestic_competition_id").alias("competition_id")
    )

# Partitionné par année : on lira souvent "donne-moi les valeurs de 2023"
# → Spark ne lira que le sous-dossier valuation_year=2023, pas tout le dataset
write_parquet(valuations_clean, f"{SILVER}/player_valuations", partition_by="valuation_year")
logger.info(f"Valuations nettoyées : {valuations_clean.count()} lignes")


# ═══════════════════════════════════════════════════════
# TRAITEMENT 3 : CLUBS (Transfermarkt)
# ═══════════════════════════════════════════════════════
logger.info("=== TRAITEMENT : clubs.csv ===")

clubs = read_csv(f"{BRONZE}/Transfermarkt/clubs.csv")

clubs_clean = clubs \
    .dropDuplicates(["club_id"]) \
    .filter(col("club_id").isNotNull()) \
    .withColumn("club_name", trim(col("name"))) \
    .withColumn("domestic_competition_id", trim(col("domestic_competition_id"))) \
    .select(
        col("club_id"),
        col("club_name"),
        col("domestic_competition_id"),
        col("squad_size"),
        col("average_age"),
        col("total_market_value")
    )

write_parquet(clubs_clean, f"{SILVER}/clubs")
logger.info(f"Clubs nettoyés : {clubs_clean.count()} lignes")


# ═══════════════════════════════════════════════════════
# TRAITEMENT 4 : COMPETITIONS (Transfermarkt)
# ═══════════════════════════════════════════════════════
logger.info("=== TRAITEMENT : competitions.csv ===")

competitions = read_csv(f"{BRONZE}/Transfermarkt/competitions.csv")

competitions_clean = competitions \
    .dropDuplicates(["competition_id"]) \
    .filter(col("competition_id").isNotNull()) \
    .withColumn("competition_name", trim(col("name"))) \
    .withColumn("country_name", trim(col("country_name"))) \
    .select(
        col("competition_id"),
        col("competition_name"),
        col("country_name"),
        col("type")
    )

write_parquet(competitions_clean, f"{SILVER}/competitions")
logger.info(f"Compétitions nettoyées : {competitions_clean.count()} lignes")


# ═══════════════════════════════════════════════════════
# TRAITEMENT 5 : APPEARANCES (Transfermarkt)
# ═══════════════════════════════════════════════════════
logger.info("=== TRAITEMENT : appearances.csv ===")

appearances = read_csv(f"{BRONZE}/Transfermarkt/appearances.csv")

appearances_clean = appearances \
    .dropDuplicates(["appearance_id"]) \
    .filter(col("player_id").isNotNull()) \
    .withColumn("goals", col("goals").cast(IntegerType())) \
    .withColumn("assists", col("assists").cast(IntegerType())) \
    .withColumn("minutes_played", col("minutes_played").cast(IntegerType())) \
    .withColumn("yellow_cards", col("yellow_cards").cast(IntegerType())) \
    .withColumn("red_cards", col("red_cards").cast(IntegerType())) \
    .na.fill(0, ["goals", "assists", "minutes_played", "yellow_cards", "red_cards"]) \
    .select(
        col("appearance_id"),
        col("player_id"),
        col("game_id"),
        col("competition_id"),
        col("date"),
        col("goals"),
        col("assists"),
        col("minutes_played"),
        col("yellow_cards"),
        col("red_cards")
    )

write_parquet(appearances_clean, f"{SILVER}/appearances")
logger.info(f"Appearances nettoyées : {appearances_clean.count()} lignes")


# ═══════════════════════════════════════════════════════
# TRAITEMENT 6 : FIFA 23 MALE PLAYERS
# ═══════════════════════════════════════════════════════
logger.info("=== TRAITEMENT : male_players.csv (FIFA 23) ===")

fifa = read_csv(f"{BRONZE}/FIFA 23 Players/male_players.csv").limit(50000)

# On garde les colonnes utiles pour notre analyse
# fifa_update_date : permet de garder uniquement la dernière version si doublons
fifa_clean = fifa \
    .dropDuplicates(["player_id", "fifa_update_date"]) \
    .filter(col("player_id").isNotNull()) \
    .withColumn("overall_rating", col("overall").cast(IntegerType())) \
    .withColumn("potential_rating", col("potential").cast(IntegerType())) \
    .withColumn("wage_eur", col("wage_eur").cast(FloatType())) \
    .withColumn("value_eur", col("value_eur").cast(FloatType())) \
    .select(
        col("player_id"),
        col("short_name"),
        col("long_name"),
        col("overall_rating"),
        col("potential_rating"),
        col("wage_eur"),
        col("value_eur"),
        col("age"),
        col("club_name"),
        col("nationality_name"),
        col("preferred_foot"),
        col("height_cm").cast(FloatType()),
        col("weight_kg").cast(FloatType()),
        col("player_positions"),
        col("fifa_update_date")
    )

write_parquet(fifa_clean, f"{SILVER}/fifa_players")
logger.info(f"FIFA players nettoyés : {fifa_clean.count()} lignes")


# ─────────────────────────────────────────
# RÉSUMÉ FINAL
# ─────────────────────────────────────────
logger.info("=" * 50)
logger.info("✅ JOB BRONZE → SILVER TERMINÉ")
logger.info("Fichiers Parquet écrits dans MinIO/silver/")
logger.info("=" * 50)

spark.stop()