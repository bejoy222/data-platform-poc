-- Create schema registry database
CREATE DATABASE schema_registry;

-- Create airflow database (for later)
CREATE DATABASE airflow;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE schema_registry TO platform;
GRANT ALL PRIVILEGES ON DATABASE airflow TO platform;
