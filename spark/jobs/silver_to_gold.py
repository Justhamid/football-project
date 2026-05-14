"""
JOB SPARK 2 : SILVER → GOLD
============================
Ce job lit les Parquet nettoyés depuis MinIO (zone Silver),
réalise les jointures et agrégations, puis écrit les tables
analytiques dans PostgreSQL (zone Gold).

Tables produites :
  - dim_player        : dimension joueurs (qui ?)
  - dim_club          : dimension clubs (quel club ?)
  - dim_competition   : dimension championnats (quelle ligue ?)
  - fact_player_value : table de faits (valeur marchande + stats)
  - agg_value_by_position  : valeur moyenne par poste
  - agg_value_by_league    : valeur moyenne par championnat
  - agg_value_by_age       : valeur moyenne par âge
  - agg_top_players        : top 50 joueurs les mieux valorisés
  - agg_value_by_nationality : valeur moyenne par nationalité
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, count, sum, round,
    when, year, desc, rank
)
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.window import Window
import logging

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

# Connexion PostgreSQL
PG_URL = "jdbc:postgresql://postgres:5432/football"
PG_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# ─────────────────────────────────────────
# FONCTION : écriture dans PostgreSQL
# ─────────────────────────────────────────
def write_postgres(df, table: str):
    logger.info(f"Écriture PostgreSQL : {table} ({df.count()} lignes)")
    df.write \
        .mode("overwrite") \
        .jdbc(PG_URL, table, properties=PG_PROPS)
    logger.info(f"✅ Table {table} écrite")


# ─────────────────────────────────────────
# LECTURE DES DONNÉES SILVER (Parquet)
# ─────────────────────────────────────────
logger.info("Lecture des Parquet depuis MinIO/silver...")

players      = spark.read.parquet(f"{SILVER}/players")
valuations   = spark.read.parquet(f"{SILVER}/player_valuations")
clubs        = spark.read.parquet(f"{SILVER}/clubs")
competitions = spark.read.parquet(f"{SILVER}/competitions")
appearances  = spark.read.parquet(f"{SILVER}/appearances")
fifa         = spark.read.parquet(f"{SILVER}/fifa_players")

logger.info("✅ Tous les Parquet chargés")


# ═══════════════════════════════════════════════════════
# TABLES DIMENSIONS
# ═══════════════════════════════════════════════════════

# ── DIMENSION JOUEURS ──
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

write_postgres(dim_player, "dim_player")


# ── DIMENSION CLUBS ──
logger.info("=== Construction : dim_club ===")

dim_club = clubs.select(
    col("club_id"),
    col("club_name"),
    col("domestic_competition_id"),
    col("squad_size"),
    col("average_age"),
    col("total_market_value")
)

write_postgres(dim_club, "dim_club")


# ── DIMENSION COMPETITIONS ──
logger.info("=== Construction : dim_competition ===")

dim_competition = competitions.select(
    col("competition_id"),
    col("competition_name"),
    col("country_name"),
    col("type")
)

write_postgres(dim_competition, "dim_competition")


# ═══════════════════════════════════════════════════════
# TABLE DE FAITS : fact_player_value
# ═══════════════════════════════════════════════════════
logger.info("=== Construction : fact_player_value ===")

# Étape 1 : stats de performance par joueur
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

# Étape 2 : dernière valeur marchande par joueur
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

# Étape 3 : jointure principale — on évite les alias croisés
# en renommant les colonnes avant de joindre
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
    .join(players_clean, latest_valuation["val_player_id"] == players_clean["p_player_id"], how="inner") \
    .join(stats_clean, latest_valuation["val_player_id"] == stats_clean["s_player_id"], how="left") \
    .join(comp_renamed, latest_valuation["val_competition_id"] == comp_renamed["comp_id"], how="left") \
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
    .filter(col("latest_market_value") > 0)

write_postgres(fact_player_value, "fact_player_value")


# ═══════════════════════════════════════════════════════
# TABLES AGRÉGÉES (pour Metabase)
# ═══════════════════════════════════════════════════════

# ── AGG 1 : Valeur par POSTE ──
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

write_postgres(agg_position, "agg_value_by_position")


# ── AGG 2 : Valeur par CHAMPIONNAT ──
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

write_postgres(agg_league, "agg_value_by_league")


# ── AGG 3 : Valeur par ÂGE ──
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

write_postgres(agg_age, "agg_value_by_age")


# ── AGG 4 : TOP 50 joueurs ──
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

write_postgres(agg_top, "agg_top_players")


# ── AGG 5 : Valeur par NATIONALITÉ ──
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

write_postgres(agg_nationality, "agg_value_by_nationality")


# ─────────────────────────────────────────
# RÉSUMÉ FINAL
# ─────────────────────────────────────────
logger.info("=" * 50)
logger.info("✅ JOB SILVER → GOLD TERMINÉ")
logger.info("Tables écrites dans PostgreSQL :")
logger.info("  - dim_player")
logger.info("  - dim_club")
logger.info("  - dim_competition")
logger.info("  - fact_player_value")
logger.info("  - agg_value_by_position")
logger.info("  - agg_value_by_league")
logger.info("  - agg_value_by_age")
logger.info("  - agg_top_players")
logger.info("  - agg_value_by_nationality")
logger.info("=" * 50)

spark.stop()