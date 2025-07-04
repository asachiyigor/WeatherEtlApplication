\c
weather_db;

-- Set the search path
SET
search_path TO public;

-- Create Liquibase changelog tables (if using Liquibase)
CREATE TABLE IF NOT EXISTS databasechangelog
(
    id
    VARCHAR
(
    255
) NOT NULL,
    author VARCHAR
(
    255
) NOT NULL,
    filename VARCHAR
(
    255
) NOT NULL,
    dateexecuted TIMESTAMP NOT NULL,
    orderexecuted INTEGER NOT NULL,
    exectype VARCHAR
(
    10
) NOT NULL,
    md5sum VARCHAR
(
    35
),
    description VARCHAR
(
    255
),
    comments VARCHAR
(
    255
),
    tag VARCHAR
(
    255
),
    liquibase VARCHAR
(
    20
),
    contexts VARCHAR
(
    255
),
    labels VARCHAR
(
    255
),
    deployment_id VARCHAR
(
    10
)
    );

CREATE TABLE IF NOT EXISTS databasechangeloglock
(
    id
    INTEGER
    NOT
    NULL,
    locked
    BOOLEAN
    NOT
    NULL,
    lockgranted
    TIMESTAMP,
    lockedby
    VARCHAR
(
    255
),
    CONSTRAINT pk_databasechangeloglock PRIMARY KEY
(
    id
)
    );

-- Insert initial lock record
INSERT INTO databasechangeloglock (id, locked)
VALUES (1, FALSE) ON CONFLICT (id) DO NOTHING;

-- Create basic weather data tables
CREATE TABLE IF NOT EXISTS weather_locations
(
    id
    SERIAL
    PRIMARY
    KEY,
    city_name
    VARCHAR
(
    100
) NOT NULL,
    country_code VARCHAR
(
    3
) NOT NULL,
    latitude DECIMAL
(
    10,
    7
) NOT NULL,
    longitude DECIMAL
(
    10,
    7
) NOT NULL,
    timezone VARCHAR
(
    50
),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE
(
    city_name,
    country_code
)
    );

CREATE TABLE IF NOT EXISTS weather_data
(
    id
    SERIAL
    PRIMARY
    KEY,
    location_id
    INTEGER
    REFERENCES
    weather_locations
(
    id
),
    recorded_at TIMESTAMP NOT NULL,
    temperature DECIMAL
(
    5,
    2
),
    humidity INTEGER,
    pressure DECIMAL
(
    7,
    2
),
    wind_speed DECIMAL
(
    5,
    2
),
    wind_direction INTEGER,
    weather_condition VARCHAR
(
    100
),
    visibility DECIMAL
(
    5,
    2
),
    uv_index DECIMAL
(
    3,
    1
),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE
(
    location_id,
    recorded_at
)
    );

CREATE TABLE IF NOT EXISTS etl_job_logs
(
    id
    SERIAL
    PRIMARY
    KEY,
    job_name
    VARCHAR
(
    100
) NOT NULL,
    status VARCHAR
(
    20
) NOT NULL CHECK
(
    status
    IN
(
    'STARTED',
    'COMPLETED',
    'FAILED'
)),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_weather_data_location_time ON weather_data(location_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_weather_data_recorded_at ON weather_data(recorded_at);
CREATE INDEX IF NOT EXISTS idx_weather_locations_city ON weather_locations(city_name);
CREATE INDEX IF NOT EXISTS idx_etl_job_logs_status ON etl_job_logs(status);

-- Grant permissions on all created objects
GRANT
ALL
PRIVILEGES
ON
ALL
TABLES IN SCHEMA public TO weather_user;
GRANT ALL PRIVILEGES ON ALL
SEQUENCES IN SCHEMA public TO weather_user;

-- Insert some sample location data
INSERT INTO weather_locations (city_name, country_code, latitude, longitude, timezone)
VALUES ('Moscow', 'RU', 55.7558, 37.6176, 'Europe/Moscow'),
       ('Saint Petersburg', 'RU', 59.9311, 30.3609, 'Europe/Moscow'),
       ('Novosibirsk', 'RU', 55.0084, 82.9357, 'Asia/Novosibirsk'),
       ('Yekaterinburg', 'RU', 56.8431, 60.6454, 'Asia/Yekaterinburg'),
       ('Kazan', 'RU', 55.8304, 49.0661, 'Europe/Moscow') ON CONFLICT (city_name, country_code) DO NOTHING;

-- Log schema creation
DO
$$
BEGIN
    RAISE
NOTICE '=== Weather ETL Schema Setup Complete ===';
    RAISE
NOTICE 'Tables created: weather_locations, weather_data, etl_job_logs';
    RAISE
NOTICE 'Liquibase tables: databasechangelog, databasechangeloglock';
    RAISE
NOTICE 'Sample locations inserted: % rows', (SELECT COUNT(*) FROM weather_locations);
    RAISE
NOTICE 'Schema setup completed at: %', now();
    RAISE
NOTICE '=====================================';
END $$;