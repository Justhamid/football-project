-- ================================================
-- init_db.sql
-- Initialisation PostgreSQL au démarrage Docker
-- Crée la base football + les rôles sécurisés
-- ================================================

-- Création de la base de données Gold
CREATE DATABASE football;
GRANT ALL PRIVILEGES ON DATABASE football TO airflow;

-- Connexion à la base football
\c football

-- ================================================
-- CONTRAINTES D'INTÉGRITÉ : Création des tables
-- avec PRIMARY KEY, FOREIGN KEY, NOT NULL, CHECK
-- ================================================

-- Table dim_player
CREATE TABLE IF NOT EXISTS dim_player (
    player_id       INT PRIMARY KEY,
    player_name     VARCHAR(255) NOT NULL,
    position        VARCHAR(50),
    nationality     VARCHAR(100),
    age             INT CHECK (age >= 14 AND age <= 50),
    height_cm       FLOAT CHECK (height_cm > 100 AND height_cm < 250),
    foot            VARCHAR(20),
    overall_rating  INT CHECK (overall_rating >= 1 AND overall_rating <= 99),
    potential_rating INT CHECK (potential_rating >= 1 AND potential_rating <= 99),
    wage_eur        FLOAT CHECK (wage_eur >= 0)
);

-- Table dim_club
CREATE TABLE IF NOT EXISTS dim_club (
    club_id                  INT PRIMARY KEY,
    club_name                VARCHAR(255) NOT NULL,
    domestic_competition_id  VARCHAR(50),
    squad_size               INT CHECK (squad_size >= 0),
    average_age              FLOAT CHECK (average_age >= 14 AND average_age <= 50),
    total_market_value       FLOAT CHECK (total_market_value >= 0)
);

-- Table dim_competition
CREATE TABLE IF NOT EXISTS dim_competition (
    competition_id    VARCHAR(50) PRIMARY KEY,
    competition_name  VARCHAR(255) NOT NULL,
    country_name      VARCHAR(100),
    type              VARCHAR(50)
);

-- Table de faits centrale
CREATE TABLE IF NOT EXISTS fact_player_value (
    player_id             INT NOT NULL,
    player_name           VARCHAR(255) NOT NULL,
    position              VARCHAR(50),
    nationality           VARCHAR(100),
    age                   INT CHECK (age >= 14 AND age <= 50),
    overall_rating        INT CHECK (overall_rating >= 1 AND overall_rating <= 99),
    potential_rating      INT CHECK (potential_rating >= 1 AND potential_rating <= 99),
    latest_market_value   FLOAT NOT NULL CHECK (latest_market_value > 0),
    valuation_date        DATE,
    competition_name      VARCHAR(255),
    league_country        VARCHAR(100),
    total_goals           INT CHECK (total_goals >= 0),
    total_assists         INT CHECK (total_assists >= 0),
    total_appearances     INT CHECK (total_appearances >= 0),
    total_minutes         INT CHECK (total_minutes >= 0),
    goals_per_game        FLOAT CHECK (goals_per_game >= 0),
    foot                  VARCHAR(20)
);

-- Tables agrégées (pas de contraintes strictes car pré-calculées)
CREATE TABLE IF NOT EXISTS agg_value_by_position (
    position          VARCHAR(50),
    avg_market_value  FLOAT CHECK (avg_market_value >= 0),
    max_market_value  FLOAT CHECK (max_market_value >= 0),
    player_count      INT CHECK (player_count >= 0),
    avg_fifa_rating   FLOAT,
    avg_goals         FLOAT
);

CREATE TABLE IF NOT EXISTS agg_value_by_league (
    competition_name  VARCHAR(255),
    league_country    VARCHAR(100),
    avg_market_value  FLOAT CHECK (avg_market_value >= 0),
    max_market_value  FLOAT CHECK (max_market_value >= 0),
    player_count      INT CHECK (player_count >= 0),
    avg_fifa_rating   FLOAT
);

CREATE TABLE IF NOT EXISTS agg_value_by_age (
    age               INT CHECK (age >= 14 AND age <= 50),
    avg_market_value  FLOAT CHECK (avg_market_value >= 0),
    player_count      INT CHECK (player_count >= 0),
    avg_fifa_rating   FLOAT
);

CREATE TABLE IF NOT EXISTS agg_top_players (
    player_name        VARCHAR(255),
    position           VARCHAR(50),
    nationality        VARCHAR(100),
    age                INT,
    latest_market_value FLOAT CHECK (latest_market_value >= 0),
    competition_name   VARCHAR(255),
    overall_rating     INT,
    total_goals        INT,
    total_assists      INT,
    goals_per_game     FLOAT
);

CREATE TABLE IF NOT EXISTS agg_value_by_nationality (
    nationality       VARCHAR(100),
    avg_market_value  FLOAT CHECK (avg_market_value >= 0),
    player_count      INT CHECK (player_count >= 0),
    avg_fifa_rating   FLOAT
);

-- Transfert de propriété vers role_spark_etl
-- (permet à Spark de DROP/RECREATE les tables via le swap atomique)
ALTER TABLE dim_player              OWNER TO role_spark_etl;
ALTER TABLE dim_club                OWNER TO role_spark_etl;
ALTER TABLE dim_competition         OWNER TO role_spark_etl;
ALTER TABLE fact_player_value       OWNER TO role_spark_etl;
ALTER TABLE agg_value_by_position   OWNER TO role_spark_etl;
ALTER TABLE agg_value_by_league     OWNER TO role_spark_etl;
ALTER TABLE agg_value_by_age        OWNER TO role_spark_etl;
ALTER TABLE agg_top_players         OWNER TO role_spark_etl;
ALTER TABLE agg_value_by_nationality OWNER TO role_spark_etl;

-- ================================================
-- SÉCURITÉ : Rôles PostgreSQL
-- Principe du moindre privilège
-- ================================================

-- Rôle 1 : role_spark_etl
-- Utilisé par Spark pour écrire en Gold
CREATE ROLE role_spark_etl WITH LOGIN PASSWORD 'spark_etl_2024';
GRANT CONNECT ON DATABASE football TO role_spark_etl;
GRANT USAGE, CREATE ON SCHEMA public TO role_spark_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO role_spark_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_spark_etl;

-- Rôle 2 : role_metabase_read
-- Utilisé par Metabase — lecture seule
CREATE ROLE role_metabase_read WITH LOGIN PASSWORD 'metabase_read_2024';
GRANT CONNECT ON DATABASE football TO role_metabase_read;
GRANT USAGE ON SCHEMA public TO role_metabase_read;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_metabase_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO role_metabase_read;