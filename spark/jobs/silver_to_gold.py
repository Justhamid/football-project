"""
JOB SPARK 2 : SILVER -> GOLD
============================
Ce job lit les Parquet nettoyes depuis MinIO (zone Silver),
realise les jointures et agregations, puis ecrit les tables
analytiques dans PostgreSQL (zone Gold).


"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, count, sum, round,
    when, year, desc, rank
)
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.window import Window
import logging
import psycopg2
import sys

# ─────────────────────────────────────────
# CONFIGURATION SPARK + MINIO + POSTGRESQL
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("FootballSilverToGold") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER = "s3a://silver"
PG_URL = "jdbc:postgresql://postgres:5432/football"
PG_PROPS = {
    "user": "role_spark_etl",
    "password": "spark_etl_2024",
    "driver": "org.postgresql.Driver"
}

# ─────────────────────────────────────────
# BUG FIX 1 : Verification PostgreSQL
# avant toute insertion
# ─────────────────────────────────────────
def check_postgres_connection():
    """
    Verifie que PostgreSQL est accessible avant de lancer
    les insertions. Evite que Spark plante de facon cryptique.
    """
    logger.info("Verification connexion PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="football",
            user="role_spark_etl",
            password="spark_etl_2024",
            connect_timeout=10
        )
        conn.close()
        logger.info("PostgreSQL accessible")
    except Exception as e:
        logger.error(f"PostgreSQL inaccessible : {e}")
        raise Exception(
            "Impossible de se connecter a PostgreSQL. "
            "Verifie que le service postgres est bien demarre."
        )

# ─────────────────────────────────────────
# BUG FIX 2 : Ecriture securisee
# Table temporaire + swap atomique
# ─────────────────────────────────────────
def write_postgres_safe(df, table: str):
    """
    Ecrit un DataFrame dans PostgreSQL de facon securisee :
    1. Verifie que le DataFrame n'est pas vide
    2. Ecrit dans une table temporaire
    3. Si succes -> swap atomique (renomme temp -> table finale)
    4. Verifie le count apres insertion
    5. Si echec -> table originale reste intacte
    """
    temp_table = f"{table}_temp"

    # Etape 1 : verification DataFrame non vide
    count_df = df.count()
    if count_df == 0:
        raise Exception(
            f"DataFrame vide pour la table {table}. "
            "Insertion annulee."
        )
    logger.info(f"Insertion {table} : {count_df} lignes a ecrire")

    # Etape 2 : ecriture dans table temporaire
    logger.info(f"Ecriture dans table temporaire {temp_table}...")
    try:
        df.write \
            .mode("overwrite") \
            .jdbc(PG_URL, temp_table, properties=PG_PROPS)
        logger.info(f"Table temporaire {temp_table} ecrite")
    except Exception as e:
        logger.error(f"Echec ecriture table temp {temp_table} : {e}")
        raise Exception(f"Echec insertion dans {temp_table} : {e}")

    # Etape 3 : swap atomique
    logger.info(f"Swap atomique {temp_table} -> {table}...")
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="football",
            user="role_spark_etl",
            password="spark_etl_2024"
        )
        conn.autocommit = False
        cursor = conn.cursor()

        # Supprimer l'ancienne table et renommer la temp
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table};")
        conn.commit()
        logger.info(f"Swap reussi : {table} est maintenant a jour")

    except Exception as e:
        conn.rollback()
        logger.error(f"Echec swap atomique : {e}")
        raise Exception(f"Echec swap {temp_table} -> {table} : {e}")
    finally:
        conn.close()

    # Etape 4 : verification count apres insertion
    try:
        conn = psycopg2.connect(
            host="postgres", port=5432,
            database="football",
            user="role_spark_etl",
            password="spark_etl_2024"
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count_pg = cursor.fetchone()[0]
        conn.close()

        if count_pg != count_df:
            raise Exception(
                f"Verification echouee pour {table} : "
                f"{count_df} lignes attendues, {count_pg} dans PostgreSQL"
            )
        logger.info(f"Table {table} : {count_pg} lignes verifiees")

    except psycopg2.Error as e:
        logger.warning(f"Impossible de verifier le count : {e}")

# ─────────────────────────────────────────
# BUG FIX 3 : Verification MinIO Silver
# avant de lire les Parquet
# ─────────────────────────────────────────
def check_silver_available():
    """
    Verifie que les fichiers Parquet Silver existent
    dans MinIO avant de les lire.
    """
    import boto3
    from botocore.client import Config

    logger.info("Verification des Parquet dans MinIO/silver...")
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    required = [
        "players/",
        "player_valuations/",
        "clubs/",
        "competitions/",
        "appearances/",
        "fifa_players/"
    ]

    response = s3.list_objects_v2(Bucket="silver", Delimiter="/")
    existing = [p["Prefix"] for p in response.get("CommonPrefixes", [])]

    missing = [r for r in required if r not in existing]
    if missing:
        raise Exception(
            f"Parquet manquants dans MinIO/silver : {missing}. "
            "Lance d'abord le Job 1 bronze_to_silver.py"
        )
    logger.info("Tous les Parquet Silver sont presents")

# ─────────────────────────────────────────
# DEBUT DU JOB
# ─────────────────────────────────────────

# Verification 1 : PostgreSQL accessible ?
check_postgres_connection()

# Verification 2 : Silver disponible ?
check_silver_available()

# ─────────────────────────────────────────
# LECTURE DES DONNEES SILVER
# ─────────────────────────────────────────
logger.info("Lecture des Parquet depuis MinIO/silver...")

players      = spark.read.parquet(f"{SILVER}/players")
valuations   = spark.read.parquet(f"{SILVER}/player_valuations")
clubs        = spark.read.parquet(f"{SILVER}/clubs")
competitions = spark.read.parquet(f"{SILVER}/competitions")
appearances  = spark.read.parquet(f"{SILVER}/appearances")
fifa         = spark.read.parquet(f"{SILVER}/fifa_players")

logger.info("Tous les Parquet charges")

# ═══════════════════════════════════════════════════════
# TABLES DIMENSIONS
# ═══════════════════════════════════════════════════════

logger.info("=== Construction : dim_player ===")
dim_player = players \
    .join(fifa, players["player_id"] == fifa["player_id"], how="left") \
    .select(
        players["player_id"],
        players["name"].alias("player_name"),
        players["position"],
        players["nationality"],
        players["age"],
        players["height_cm"],
        players["foot"],
        fifa["overall_rating"],
        fifa["potential_rating"],
        fifa["wage_eur"]
    ) \
    .dropDuplicates(["player_id"])
write_postgres_safe(dim_player, "dim_player")

logger.info("=== Construction : dim_club ===")
dim_club = clubs.select(
    col("club_id"),
    col("club_name"),
    col("domestic_competition_id"),
    col("squad_size"),
    col("average_age"),
    col("total_market_value")
)
write_postgres_safe(dim_club, "dim_club")

logger.info("=== Construction : dim_competition ===")
dim_competition = competitions.select(
    col("competition_id"),
    col("competition_name"),
    col("country_name"),
    col("type")
)
write_postgres_safe(dim_competition, "dim_competition")

# ═══════════════════════════════════════════════════════
# TABLE DE FAITS
# ═══════════════════════════════════════════════════════
logger.info("=== Construction : fact_player_value ===")

player_stats = appearances \
    .groupBy("player_id") \
    .agg(
        sum("goals").alias("total_goals"),
        sum("assists").alias("total_assists"),
        sum("minutes_played").alias("total_minutes"),
        count("appearance_id").alias("total_appearances"),
        sum("yellow_cards").alias("total_yellows"),
        sum("red_cards").alias("total_reds")
    ) \
    .withColumn(
        "goals_per_game",
        round(
            (col("total_goals") + col("total_assists")) / col("total_appearances"), 2
        )
    )

window_latest = Window.partitionBy("player_id").orderBy(desc("date"))
latest_valuation = valuations \
    .withColumn("rn", rank().over(window_latest)) \
    .filter(col("rn") == 1) \
    .select(
        col("player_id").alias("val_player_id"),
        col("market_value_eur").alias("latest_market_value"),
        col("date").alias("valuation_date"),
        col("competition_id").alias("val_competition_id")
    )

comp_renamed = competitions.select(
    col("competition_id").alias("comp_id"),
    col("competition_name"),
    col("country_name").alias("league_country")
)

players_clean = dim_player.select(
    col("player_id").alias("p_player_id"),
    col("player_name"),
    col("position"),
    col("nationality"),
    col("age"),
    col("overall_rating"),
    col("potential_rating"),
    col("foot")
)

stats_clean = player_stats.select(
    col("player_id").alias("s_player_id"),
    col("total_goals"),
    col("total_assists"),
    col("total_appearances"),
    col("total_minutes"),
    col("goals_per_game")
)

fact_player_value = latest_valuation \
    .join(players_clean,
          latest_valuation["val_player_id"] == players_clean["p_player_id"],
          how="inner") \
    .join(stats_clean,
          latest_valuation["val_player_id"] == stats_clean["s_player_id"],
          how="left") \
    .join(comp_renamed,
          latest_valuation["val_competition_id"] == comp_renamed["comp_id"],
          how="left") \
    .select(
        col("val_player_id").alias("player_id"),
        col("player_name"),
        col("position"),
        col("nationality"),
        col("age"),
        col("overall_rating"),
        col("potential_rating"),
        col("latest_market_value"),
        col("valuation_date"),
        col("competition_name"),
        col("league_country"),
        col("total_goals"),
        col("total_assists"),
        col("total_appearances"),
        col("total_minutes"),
        col("goals_per_game"),
        col("foot")
    ) \
    .filter(col("latest_market_value").isNotNull()) \
    .filter(col("latest_market_value") > 0) \
    .filter(col("position") != "Missing")

write_postgres_safe(fact_player_value, "fact_player_value")

# ═══════════════════════════════════════════════════════
# TABLES AGREGEES
# ═══════════════════════════════════════════════════════

logger.info("=== Construction : agg_value_by_position ===")
agg_position = fact_player_value \
    .filter(col("position").isNotNull()) \
    .groupBy("position") \
    .agg(
        round(avg("latest_market_value"), 0).alias("avg_market_value"),
        round(max("latest_market_value"), 0).alias("max_market_value"),
        count("player_id").alias("player_count"),
        round(avg("overall_rating"), 1).alias("avg_fifa_rating"),
        round(avg("total_goals"), 1).alias("avg_goals")
    ) \
    .orderBy(desc("avg_market_value"))
write_postgres_safe(agg_position, "agg_value_by_position")

logger.info("=== Construction : agg_value_by_league ===")
agg_league = fact_player_value \
    .filter(col("competition_name").isNotNull()) \
    .groupBy("competition_name", "league_country") \
    .agg(
        round(avg("latest_market_value"), 0).alias("avg_market_value"),
        round(max("latest_market_value"), 0).alias("max_market_value"),
        count("player_id").alias("player_count"),
        round(avg("overall_rating"), 1).alias("avg_fifa_rating")
    ) \
    .orderBy(desc("avg_market_value"))
write_postgres_safe(agg_league, "agg_value_by_league")

logger.info("=== Construction : agg_value_by_age ===")
agg_age = fact_player_value \
    .filter(col("age").isNotNull()) \
    .filter((col("age") >= 16) & (col("age") <= 40)) \
    .groupBy("age") \
    .agg(
        round(avg("latest_market_value"), 0).alias("avg_market_value"),
        count("player_id").alias("player_count"),
        round(avg("overall_rating"), 1).alias("avg_fifa_rating")
    ) \
    .orderBy("age")
write_postgres_safe(agg_age, "agg_value_by_age")

logger.info("=== Construction : agg_top_players ===")
agg_top = fact_player_value \
    .orderBy(desc("latest_market_value")) \
    .limit(50) \
    .select(
        col("player_name"),
        col("position"),
        col("nationality"),
        col("age"),
        col("latest_market_value"),
        col("competition_name"),
        col("overall_rating"),
        col("total_goals"),
        col("total_assists"),
        col("goals_per_game")
    )
write_postgres_safe(agg_top, "agg_top_players")

logger.info("=== Construction : agg_value_by_nationality ===")
agg_nationality = fact_player_value \
    .filter(col("nationality").isNotNull()) \
    .groupBy("nationality") \
    .agg(
        round(avg("latest_market_value"), 0).alias("avg_market_value"),
        count("player_id").alias("player_count"),
        round(avg("overall_rating"), 1).alias("avg_fifa_rating")
    ) \
    .filter(col("player_count") >= 5) \
    .orderBy(desc("avg_market_value")) \
    .limit(30)
write_postgres_safe(agg_nationality, "agg_value_by_nationality")

# ─────────────────────────────────────────
# RESUME FINAL
# ─────────────────────────────────────────
logger.info("=" * 50)
logger.info("JOB SILVER -> GOLD TERMINE")
logger.info("9 tables ecrites et verifiees dans PostgreSQL")
logger.info("Swap atomique applique sur chaque table")
logger.info("=" * 50)

spark.stop()