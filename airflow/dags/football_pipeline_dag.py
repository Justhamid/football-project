"""
DAG AIRFLOW : Football Market Value Pipeline
=============================================
Orchestre le pipeline complet de traitement des donnees football.

Sequence :
  1. check_minio_buckets    -> verifie/cree les buckets MinIO
  2. check_data_available   -> verifie CSV presents et non vides
  3. check_postgres         -> verifie PostgreSQL accessible
  4. bronze_to_silver       -> Spark Job 1 : CSV -> Parquet
  5. silver_to_gold         -> Spark Job 2 : Parquet -> PostgreSQL
  6. pipeline_success       -> confirmation finale

Schedule : tous les jours a 6h du matin
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import subprocess

logger = logging.getLogger(__name__)

default_args = {
    "owner": "football_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="football_market_value_pipeline",
    default_args=default_args,
    description="Pipeline ELT : valeur marchande des joueurs de football",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["football", "spark", "minio", "postgres"],
)

# ─────────────────────────────────────────
# TACHE 1 : Verification buckets MinIO
# ─────────────────────────────────────────
def check_minio_buckets():
    import boto3
    from botocore.client import Config
    from botocore.exceptions import EndpointResolutionError, ClientError

    logger.info("Connexion a MinIO...")

    # BUG FIX : verification que MinIO est accessible
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(
                signature_version="s3v4",
                connect_timeout=10,
                retries={"max_attempts": 3}
            ),
            region_name="us-east-1"
        )
        s3.list_buckets()
        logger.info("MinIO accessible")
    except Exception as e:
        raise Exception(
            f"MinIO inaccessible : {e}. "
            "Verifie que le service minio est bien demarre."
        )

    # Creation des buckets si manquants
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    for bucket in ["bronze", "silver"]:
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            logger.info(f"Bucket cree : {bucket}")
        else:
            logger.info(f"Bucket existant : {bucket}")

check_buckets_task = PythonOperator(
    task_id="check_minio_buckets",
    python_callable=check_minio_buckets,
    dag=dag,
)

# ─────────────────────────────────────────
# TACHE 2 : Verification donnees Bronze
# ─────────────────────────────────────────
def check_data_available():
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    required_files = [
        "Transfermarkt/players.csv",
        "Transfermarkt/player_valuations.csv",
        "FIFA 23 Players/male_players.csv",
    ]

    objects = s3.list_objects_v2(Bucket="bronze")
    contents = objects.get("Contents", [])
    existing_keys = [obj["Key"] for obj in contents]

    # BUG FIX 1 : verification fichiers presents
    missing = []
    for f in required_files:
        if not any(f in key for key in existing_keys):
            missing.append(f)

    if missing:
        raise FileNotFoundError(
            f"Fichiers manquants dans MinIO/bronze : {missing}. "
            "Lance upload_to_bronze.py avant de demarrer le pipeline."
        )

    # BUG FIX 2 : verification fichiers non vides
    empty_files = []
    for obj in contents:
        if obj["Size"] == 0:
            empty_files.append(obj["Key"])

    if empty_files:
        raise Exception(
            f"Fichiers vides detectes dans Bronze : {empty_files}. "
            "Verifie que l'upload s'est bien termine."
        )

    logger.info(f"Tous les fichiers presents et non vides ({len(contents)} fichiers)")

check_data_task = PythonOperator(
    task_id="check_data_available",
    python_callable=check_data_available,
    dag=dag,
)

# ─────────────────────────────────────────
# TACHE 3 : Verification PostgreSQL
# BUG FIX : nouvelle tache manquante
# ─────────────────────────────────────────
def check_postgres_connection():
    """
    Verifie que PostgreSQL est accessible avant de lancer Spark.
    Evite que silver_to_gold plante de facon cryptique.
    """
    import psycopg2

    logger.info("Verification connexion PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="football",
            user="airflow",
            password="airflow",
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL accessible : {version[:50]}")
        conn.close()
    except Exception as e:
        raise Exception(
            f"PostgreSQL inaccessible : {e}. "
            "Verifie que le service postgres est bien demarre."
        )

check_postgres_task = PythonOperator(
    task_id="check_postgres",
    python_callable=check_postgres_connection,
    dag=dag,
)

# ─────────────────────────────────────────
# TACHES 4 & 5 : Jobs Spark
# ─────────────────────────────────────────
def submit_spark_job(script_name: str):
    packages = (
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    if script_name == "silver_to_gold.py":
        packages += ",org.postgresql:postgresql:42.6.0"

    cmd = [
        "docker", "exec", "football_spark_master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        "--conf", "spark.sql.shuffle.partitions=4",
        "--packages", packages,
        "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        f"/opt/spark_jobs/{script_name}"
    ]

    logger.info(f"Lancement spark-submit : {script_name}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=7200
    )

    if result.stdout:
        logger.info(result.stdout[-8000:])

    if result.returncode != 0:
        logger.error(result.stderr[-5000:])
        raise Exception(
            f"Spark job {script_name} a echoue (code {result.returncode}). "
            f"Voir les logs Airflow pour les details."
        )

    logger.info(f"Job {script_name} termine avec succes")

bronze_to_silver_task = PythonOperator(
    task_id="bronze_to_silver",
    python_callable=submit_spark_job,
    op_kwargs={"script_name": "bronze_to_silver.py"},
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

silver_to_gold_task = PythonOperator(
    task_id="silver_to_gold",
    python_callable=submit_spark_job,
    op_kwargs={"script_name": "silver_to_gold.py"},
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

# ─────────────────────────────────────────
# TACHE 6 : Confirmation finale
# ─────────────────────────────────────────
def pipeline_success():
    """
    Verifie les counts finaux dans PostgreSQL
    et logue la confirmation de fin de pipeline.
    """
    import psycopg2

    logger.info("=" * 60)
    logger.info("VERIFICATION FINALE DES TABLES POSTGRESQL")
    logger.info("=" * 60)

    conn = psycopg2.connect(
        host="postgres", port=5432,
        database="football",
        user="airflow", password="airflow"
    )
    cursor = conn.cursor()

    tables = [
        "dim_player", "dim_club", "dim_competition",
        "fact_player_value", "agg_value_by_position",
        "agg_value_by_league", "agg_value_by_age",
        "agg_top_players", "agg_value_by_nationality"
    ]

    total = 0
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        total += count
        logger.info(f"  {table}: {count} lignes")

    conn.close()

    logger.info("=" * 60)
    logger.info(f"TOTAL : {total} lignes dans PostgreSQL")
    logger.info("PIPELINE FOOTBALL TERMINE AVEC SUCCES")
    logger.info(f"Execution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

success_task = PythonOperator(
    task_id="pipeline_success",
    python_callable=pipeline_success,
    dag=dag,
)

# ─────────────────────────────────────────
# ORDRE D'EXECUTION
# ─────────────────────────────────────────
(
    check_buckets_task
    >> check_data_task
    >> check_postgres_task
    >> bronze_to_silver_task
    >> silver_to_gold_task
    >> success_task
)