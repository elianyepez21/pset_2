-- Ejecutar UNA sola vez en Snowflake (worksheet) con un rol administrador.
-- Crea el rol, el warehouse, la base y los 5 esquemas del pipeline. No contiene credenciales.
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS PSET2_ROLE;
-- Reemplaza TU_USUARIO por el usuario de Snowflake que usarán Kestra, dbt y Spark.
GRANT ROLE PSET2_ROLE TO USER TU_USUARIO;

CREATE WAREHOUSE IF NOT EXISTS WH_PSET2
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
GRANT USAGE, OPERATE ON WAREHOUSE WH_PSET2 TO ROLE PSET2_ROLE;

CREATE DATABASE IF NOT EXISTS HOTELS_DB;
GRANT OWNERSHIP ON DATABASE HOTELS_DB TO ROLE PSET2_ROLE COPY CURRENT GRANTS;

USE ROLE PSET2_ROLE;
USE DATABASE HOTELS_DB;
CREATE SCHEMA IF NOT EXISTS RAW;     -- datos originales, sin tocar (Kestra)
CREATE SCHEMA IF NOT EXISTS BRONZE;  -- tipado y renombrado, capa de staging (dbt)
CREATE SCHEMA IF NOT EXISTS SILVER;  -- limpio y deduplicado (dbt)
CREATE SCHEMA IF NOT EXISTS GOLD;    -- star schema (dbt)
CREATE SCHEMA IF NOT EXISTS OBT;     -- One Big Table (Spark)
