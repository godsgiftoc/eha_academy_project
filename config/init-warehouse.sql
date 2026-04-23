-- Creates a second database for the pipeline's bronze/silver/gold tables,
-- keeping Airflow metadata isolated from warehouse data.
CREATE DATABASE warehouse;
GRANT ALL PRIVILEGES ON DATABASE warehouse TO airflow;
