-- Connect to the weather_db database
\c weather_db;

-- Ensure the user exists (backup for environment variables)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'weather_user') THEN
        CREATE USER weather_user WITH PASSWORD 'weather_pass';
        RAISE NOTICE 'Created user weather_user';
ELSE
        RAISE NOTICE 'User weather_user already exists';
END IF;
END
$$;

-- Grant comprehensive privileges
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;
GRANT ALL PRIVILEGES ON SCHEMA public TO weather_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO weather_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO weather_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO weather_user;

-- Ensure the user can create tables and schemas
ALTER USER weather_user CREATEDB;
ALTER USER weather_user CREATEROLE;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO weather_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO weather_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO weather_user;

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create a simple function to check database connectivity
CREATE OR REPLACE FUNCTION check_db_connection()
RETURNS TEXT AS $$
BEGIN
RETURN 'Weather ETL Database is ready! Time: ' || now()::text;
END;
$$ LANGUAGE plpgsql;

-- Log the initialization
DO $$
BEGIN
    RAISE NOTICE '=== Weather ETL Database Initialization ===';
    RAISE NOTICE 'Database: %', current_database();
    RAISE NOTICE 'Current User: %', current_user;
    RAISE NOTICE 'Current Schema: %', current_schema();
    RAISE NOTICE 'PostgreSQL Version: %', version();
    RAISE NOTICE 'Initialization completed at: %', now();
    RAISE NOTICE '==========================================';
END $$;