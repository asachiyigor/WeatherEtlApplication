CREATE TABLE IF NOT EXISTS weather_data (
                                            id BIGSERIAL PRIMARY KEY,
                                            date DATE NOT NULL,
                                            latitude DOUBLE PRECISION NOT NULL,
                                            longitude DOUBLE PRECISION NOT NULL,

    -- Средние значения за 24 часа
                                            avg_temperature_2m_24h DOUBLE PRECISION,
                                            avg_relative_humidity_2m_24h DOUBLE PRECISION,
                                            avg_dew_point_2m_24h DOUBLE PRECISION,
                                            avg_apparent_temperature_24h DOUBLE PRECISION,
                                            avg_temperature_80m_24h DOUBLE PRECISION,
                                            avg_temperature_120m_24h DOUBLE PRECISION,
                                            avg_wind_speed_10m_24h DOUBLE PRECISION,
                                            avg_wind_speed_80m_24h DOUBLE PRECISION,
                                            avg_visibility_24h DOUBLE PRECISION,
                                            total_rain_24h DOUBLE PRECISION,
                                            total_showers_24h DOUBLE PRECISION,
                                            total_snowfall_24h DOUBLE PRECISION,

    -- Средние значения за период светового дня
                                            avg_temperature_2m_daylight DOUBLE PRECISION,
                                            avg_relative_humidity_2m_daylight DOUBLE PRECISION,
                                            avg_dew_point_2m_daylight DOUBLE PRECISION,
                                            avg_apparent_temperature_daylight DOUBLE PRECISION,
                                            avg_temperature_80m_daylight DOUBLE PRECISION,
                                            avg_temperature_120m_daylight DOUBLE PRECISION,
                                            avg_wind_speed_10m_daylight DOUBLE PRECISION,
                                            avg_wind_speed_80m_daylight DOUBLE PRECISION,
                                            avg_visibility_daylight DOUBLE PRECISION,
                                            total_rain_daylight DOUBLE PRECISION,
                                            total_showers_daylight DOUBLE PRECISION,
                                            total_snowfall_daylight DOUBLE PRECISION,

    -- Преобразованные единицы измерения
                                            wind_speed_10m_m_per_s DOUBLE PRECISION,
                                            wind_speed_80m_m_per_s DOUBLE PRECISION,
                                            temperature_2m_celsius DOUBLE PRECISION,
                                            apparent_temperature_celsius DOUBLE PRECISION,
                                            temperature_80m_celsius DOUBLE PRECISION,
                                            temperature_120m_celsius DOUBLE PRECISION,
                                            soil_temperature_0cm_celsius DOUBLE PRECISION,
                                            soil_temperature_6cm_celsius DOUBLE PRECISION,
                                            rain_mm DOUBLE PRECISION,
                                            showers_mm DOUBLE PRECISION,
                                            snowfall_mm DOUBLE PRECISION,

    -- Дополнительные поля
                                            daylight_hours DOUBLE PRECISION,
                                            sunset_iso VARCHAR(25),
    sunrise_iso VARCHAR(25),

    -- Метаданные
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Уникальное ограничение для предотвращения дубликатов
    CONSTRAINT uk_weather_data_date_location UNIQUE (date, latitude, longitude)
    );

-- Индексы для оптимизации запросов
CREATE INDEX idx_weather_data_date ON weather_data(date);
CREATE INDEX idx_weather_data_location ON weather_data(latitude, longitude);
CREATE INDEX idx_weather_data_date_location ON weather_data(date, latitude, longitude);
CREATE INDEX idx_weather_data_created_at ON weather_data(created_at);

-- Комментарии к таблице и столбцам
COMMENT ON TABLE weather_data IS 'Агрегированные данные погоды по дням';
COMMENT ON COLUMN weather_data.date IS 'Дата записи';
COMMENT ON COLUMN weather_data.latitude IS 'Широта местоположения';
COMMENT ON COLUMN weather_data.longitude IS 'Долгота местоположения';
COMMENT ON COLUMN weather_data.avg_temperature_2m_24h IS 'Средняя температура на высоте 2м за 24 часа (°C)';
COMMENT ON COLUMN weather_data.avg_relative_humidity_2m_24h IS 'Средняя относительная влажность на высоте 2м за 24 часа (%)';
COMMENT ON COLUMN weather_data.daylight_hours IS 'Продолжительность светового дня (часы)';
COMMENT ON COLUMN weather_data.sunset_iso IS 'Время заката в формате ISO 8601';
COMMENT ON COLUMN weather_data.sunrise_iso IS 'Время восхода в формате ISO 8601';