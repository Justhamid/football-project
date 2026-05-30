"""
SCRIPT : upload_to_bronze.py
=============================
Ce script uploade automatiquement tous les fichiers CSV
depuis data/raw/ vers le bucket bronze de MinIO.

Utilisation :
    python upload_to_bronze.py

Structure uploadée dans MinIO/bronze :
    bronze/
    ├── Transfermarkt/
    │   ├── players.csv
    │   ├── player_valuations.csv
    │   └── ...
    └── FIFA 23 Players/
        ├── male_players.csv
        └── ...
"""

import boto3
import os
from botocore.client import Config
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
MINIO_ENDPOINT  = "http://localhost:9002"   # port exposé sur ta machine
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin"
BUCKET_NAME     = "bronze"

# Dossier local contenant les données brutes
RAW_DATA_PATH = Path(__file__).parent / "data" / "raw"

# ─────────────────────────────────────────
# CONNEXION MINIO
# ─────────────────────────────────────────
def get_minio_client():
    """Crée et retourne un client S3 connecté à MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

# ─────────────────────────────────────────
# CRÉATION DU BUCKET SI INEXISTANT
# ─────────────────────────────────────────
def ensure_bucket(s3_client, bucket: str):
    """Crée le bucket s'il n'existe pas déjà."""
    existing = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)
        logger.info(f"Bucket créé : {bucket}")
    else:
        logger.info(f"Bucket existant : {bucket}")

    # Créer aussi le bucket silver s'il n'existe pas
    if "silver" not in existing:
        s3_client.create_bucket(Bucket="silver")
        logger.info("Bucket créé : silver")

# ─────────────────────────────────────────
# UPLOAD DES FICHIERS
# ─────────────────────────────────────────
def upload_directory(s3_client, local_path: Path, bucket: str):
    """
    Parcourt récursivement le dossier local_path
    et uploade chaque fichier dans le bucket MinIO
    en conservant la structure de dossiers.
    """
    if not local_path.exists():
        raise FileNotFoundError(f"Dossier introuvable : {local_path}")

    files = list(local_path.rglob("*"))
    csv_files = [f for f in files if f.is_file() and f.suffix.lower() == ".csv"]

    if not csv_files:
        raise ValueError(f"Aucun fichier CSV trouvé dans {local_path}")

    logger.info(f"{len(csv_files)} fichiers CSV trouvés dans {local_path}")
    logger.info("=" * 60)

    success_count = 0
    error_count = 0

    for file_path in csv_files:
        # Clé S3 = chemin relatif depuis data/raw/
        # Ex: data/raw/Transfermarkt/players.csv → Transfermarkt/players.csv
        relative_path = file_path.relative_to(local_path)
        s3_key = str(relative_path).replace("\\", "/")  # Windows → slash Unix

        file_size = file_path.stat().st_size
        file_size_mb = round(file_size / (1024 * 1024), 2)

        try:
            logger.info(f"Upload : {s3_key} ({file_size_mb} MB)...")

            s3_client.upload_file(
                Filename=str(file_path),
                Bucket=bucket,
                Key=s3_key
            )

            logger.info(f"Uploadé : {s3_key}")
            success_count += 1

        except Exception as e:
            logger.error(f"Erreur upload {s3_key} : {e}")
            error_count += 1

    logger.info("=" * 60)
    logger.info(f"RÉSUMÉ : {success_count} fichiers uploadés, {error_count} erreurs")
    return success_count, error_count

# ─────────────────────────────────────────
# VÉRIFICATION APRÈS UPLOAD
# ─────────────────────────────────────────
def verify_upload(s3_client, bucket: str):
    """Liste les fichiers présents dans MinIO après upload."""
    logger.info("🔍 Vérification du contenu de MinIO/bronze...")

    response = s3_client.list_objects_v2(Bucket=bucket)
    objects = response.get("Contents", [])

    if not objects:
        logger.warning("Aucun fichier trouvé dans le bucket !")
        return

    logger.info(f"{len(objects)} fichiers dans MinIO/{bucket} :")
    for obj in objects:
        size_mb = round(obj["Size"] / (1024 * 1024), 2)
        logger.info(f"   • {obj['Key']} ({size_mb} MB)")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Démarrage de l'upload vers MinIO/bronze")
    logger.info(f"Source : {RAW_DATA_PATH}")
    logger.info(f"Destination : MinIO → bucket '{BUCKET_NAME}'")
    logger.info("=" * 60)

    try:
        # 1. Connexion
        s3 = get_minio_client()
        logger.info("Connexion à MinIO établie")

        # 2. Création des buckets
        ensure_bucket(s3, BUCKET_NAME)

        # 3. Upload
        success, errors = upload_directory(s3, RAW_DATA_PATH, BUCKET_NAME)

        # 4. Vérification
        verify_upload(s3, BUCKET_NAME)

        if errors == 0:
            logger.info("Upload terminé avec succès !")
        else:
            logger.warning(f"Upload terminé avec {errors} erreur(s)")

    except FileNotFoundError as e:
        logger.error(f"{e}")
        logger.error("Vérifie que le dossier data/raw/ existe et contient tes CSV")
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")
        raise