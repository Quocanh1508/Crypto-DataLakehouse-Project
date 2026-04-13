-- Initialize Airflow database and user in PostgreSQL
-- This script runs automatically when postgres container starts

-- Create Airflow user
CREATE USER airflow WITH PASSWORD 'airflow';

-- Create Airflow database
CREATE DATABASE airflow OWNER airflow;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER DATABASE airflow OWNER TO airflow;

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO airflow;

-- Set default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;

-- Grant execute on functions
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO airflow;
