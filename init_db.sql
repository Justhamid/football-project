-- Création de la base football
CREATE DATABASE football;
GRANT ALL PRIVILEGES ON DATABASE football TO airflow;

-- Connexion à la base football pour créer les rôles dedans
\c football

-- ─────────────────────────────────────────
-- RÔLE 1 : role_spark_etl
-- Utilisé par Spark pour écrire en Gold
-- ─────────────────────────────────────────
CREATE ROLE role_spark_etl WITH LOGIN PASSWORD 'spark_etl_2024';
GRANT CONNECT ON DATABASE football TO role_spark_etl;
GRANT USAGE ON SCHEMA public TO role_spark_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO role_spark_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_spark_etl;

-- ─────────────────────────────────────────
-- RÔLE 2 : role_metabase_read
-- Utilisé par Metabase — lecture seule
-- ─────────────────────────────────────────
CREATE ROLE role_metabase_read WITH LOGIN PASSWORD 'metabase_read_2024';
GRANT CONNECT ON DATABASE football TO role_metabase_read;
GRANT USAGE ON SCHEMA public TO role_metabase_read;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_metabase_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO role_metabase_read;