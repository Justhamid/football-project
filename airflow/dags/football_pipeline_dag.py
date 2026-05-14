"""
DAG AIRFLOW : Football Market Value Pipeline
=============================================
Ce DAG orchestre le pipeline complet de traitement des données football.

Séquence d'exécution :
  1. check_minio_buckets    → vérifie que les buckets MinIO existent
  2. check_data_available   → vérifie que les CSV sont bien dans Bronze
  3. bronze_to_silver       → job Spark nettoyage CSV → Parquet
  4. silver_to_gold         → job Spark agrégations → PostgreSQL
  5. pipeline_success       → notification de fin

Planification : tous les jours à 6h du matin (configurable)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION DU DAG
# ─────────────────────────────────────────
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
    description="Pipeline ETL : valeur marchande des joueurs de football",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["football", "spark", "minio", "postgres"],
)

# ─────────────────────────────────────────
# TÂCHE 1 : Vérification des buckets MinIO
# ─────────────────────────────────────────
def check_minio_buckets():
    """
    Vérifie que les buckets bronze et silver existent dans MinIO.
    Si non, les crée automatiquement.
    """
    import boto3
    from botocore.client import Config

    logger.info("Connexion à MinIO...")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    buckets_needed = ["bronze", "silver"]
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]

    for bucket in buckets_needed:
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            logger.info(f"✅ Bucket créé : {bucket}")
        else:
            logger.info(f"✅ Bucket existant : {bucket}")

    logger.info("Vérification MinIO terminée")

check_buckets_task = PythonOperator(
    task_id="check_minio_buckets",
    python_callable=check_minio_buckets,
    dag=dag,
)

# ─────────────────────────────────────────
# TÂCHE 2 : Vérification des données Bronze
# ─────────────────────────────────────────
def check_data_available():
    """
    Vérifie que les fichiers CSV sont bien présents dans le bucket bronze.
    Lance une erreur si les fichiers sont manquants.
    """
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
    existing_keys = [obj["Key"] for obj in objects.get("Contents", [])]

    missing = []
    for f in required_files:
        if not any(f in key for key in existing_keys):
            missing.append(f)

    if missing:
        raise FileNotFoundError(
            f"Fichiers manquants dans MinIO/bronze : {missing}\n"
            "Vérifie que les CSV ont bien été uploadés dans MinIO."
        )

    logger.info(f"✅ Tous les fichiers requis sont présents ({len(existing_keys)} fichiers au total)")

check_data_task = PythonOperator(
    task_id="check_data_available",
    python_callable=check_data_available,
    dag=dag,
)

# ─────────────────────────────────────────
# TÂCHE 3 & 4 : Jobs Spark
# ─────────────────────────────────────────
def submit_spark_job(script_name: str):
    import subprocess

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
        "--packages", packages,
        "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        f"/opt/spark_jobs/{script_name}"
    ]

    logger.info(f"Lancement : {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"❌ Spark job {script_name} a échoué :\n{result.stderr}")

    logger.info(f"✅ Job {script_name} terminé avec succès")

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
# TÂCHE 5 : Notification de succès
# ─────────────────────────────────────────
def pipeline_success():
    """Logue la fin du pipeline avec un résumé."""
    logger.info("=" * 60)
    logger.info("🎉 PIPELINE FOOTBALL TERMINÉ AVEC SUCCÈS")
    logger.info("=" * 60)
    logger.info("Zones mises à jour :")
    logger.info("  ✅ Bronze → CSV bruts dans MinIO")
    logger.info("  ✅ Silver → Parquet nettoyés dans MinIO")
    logger.info("  ✅ Gold   → Tables analytiques dans PostgreSQL")
    logger.info("Tables disponibles dans Metabase :")
    logger.info("  - fact_player_value")
    logger.info("  - agg_value_by_position")
    logger.info("  - agg_value_by_league")
    logger.info("  - agg_value_by_age")
    logger.info("  - agg_top_players")
    logger.info("  - agg_value_by_nationality")
    logger.info(f"Exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

success_task = PythonOperator(
    task_id="pipeline_success",
    python_callable=pipeline_success,
    dag=dag,
)

# ─────────────────────────────────────────
# ORDRE D'EXÉCUTION
# ─────────────────────────────────────────
check_buckets_task >> check_data_task >> bronze_to_silver_task >> silver_to_gold_task >> success_task