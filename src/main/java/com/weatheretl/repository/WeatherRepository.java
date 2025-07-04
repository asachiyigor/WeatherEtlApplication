package com.weatheretl.repository;

import com.weatheretl.model.output.WeatherRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public interface WeatherRepository extends JpaRepository<WeatherRecord, Long> {

    Optional<WeatherRecord> findByDateAndLatitudeAndLongitude(
            LocalDate date, Double latitude, Double longitude);

    List<WeatherRecord> findByDateBetween(LocalDate startDate, LocalDate endDate);

    List<WeatherRecord> findByDateBetweenAndLatitudeAndLongitude(
            LocalDate startDate, LocalDate endDate, Double latitude, Double longitude);

    boolean existsByDateAndLatitudeAndLongitude(
            LocalDate date, Double latitude, Double longitude);

    long countByDateBetween(LocalDate startDate, LocalDate endDate);

    @Query("SELECT DISTINCT w.latitude, w.longitude FROM WeatherRecord w")
    List<Object[]> findDistinctLocations();

    List<WeatherRecord> findByLatitudeAndLongitudeOrderByDateAsc(
            Double latitude, Double longitude);

    @Modifying
    @Transactional
    @Query("DELETE FROM WeatherRecord w WHERE w.date BETWEEN :startDate AND :endDate")
    int deleteByDateBetween(@Param("startDate") LocalDate startDate,
                            @Param("endDate") LocalDate endDate);

    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO weather_data (
            date, latitude, longitude, avg_temperature_2m_24h, avg_relative_humidity_2m_24h,
            avg_dew_point_2m_24h, avg_apparent_temperature_24h, avg_temperature_80m_24h,
            avg_temperature_120m_24h, avg_wind_speed_10m_24h, avg_wind_speed_80m_24h,
            avg_visibility_24h, total_rain_24h, total_showers_24h, total_snowfall_24h,
            avg_temperature_2m_daylight, avg_relative_humidity_2m_daylight, avg_dew_point_2m_daylight,
            avg_apparent_temperature_daylight, avg_temperature_80m_daylight, avg_temperature_120m_daylight,
            avg_wind_speed_10m_daylight, avg_wind_speed_80m_daylight, avg_visibility_daylight,
            total_rain_daylight, total_showers_daylight, total_snowfall_daylight,
            wind_speed_10m_m_per_s, wind_speed_80m_m_per_s, temperature_2m_celsius,
            apparent_temperature_celsius, temperature_80m_celsius, temperature_120m_celsius,
            soil_temperature_0cm_celsius, soil_temperature_6cm_celsius, rain_mm, showers_mm,
            snowfall_mm, daylight_hours, sunset_iso, sunrise_iso, created_at, updated_at
        ) VALUES (
            :#{#record.date}, :#{#record.latitude}, :#{#record.longitude}, :#{#record.avgTemperature2m24h},
            :#{#record.avgRelativeHumidity2m24h}, :#{#record.avgDewPoint2m24h}, :#{#record.avgApparentTemperature24h},
            :#{#record.avgTemperature80m24h}, :#{#record.avgTemperature120m24h}, :#{#record.avgWindSpeed10m24h},
            :#{#record.avgWindSpeed80m24h}, :#{#record.avgVisibility24h}, :#{#record.totalRain24h},
            :#{#record.totalShowers24h}, :#{#record.totalSnowfall24h}, :#{#record.avgTemperature2mDaylight},
            :#{#record.avgRelativeHumidity2mDaylight}, :#{#record.avgDewPoint2mDaylight}, :#{#record.avgApparentTemperatureDaylight},
            :#{#record.avgTemperature80mDaylight}, :#{#record.avgTemperature120mDaylight}, :#{#record.avgWindSpeed10mDaylight},
            :#{#record.avgWindSpeed80mDaylight}, :#{#record.avgVisibilityDaylight}, :#{#record.totalRainDaylight},
            :#{#record.totalShowersDaylight}, :#{#record.totalSnowfallDaylight}, :#{#record.windSpeed10mMPerS},
            :#{#record.windSpeed80mMPerS}, :#{#record.temperature2mCelsius}, :#{#record.apparentTemperatureCelsius},
            :#{#record.temperature80mCelsius}, :#{#record.temperature120mCelsius}, :#{#record.soilTemperature0cmCelsius},
            :#{#record.soilTemperature6cmCelsius}, :#{#record.rainMm}, :#{#record.showersMm}, :#{#record.snowfallMm},
            :#{#record.daylightHours}, :#{#record.sunsetIso}, :#{#record.sunriseIso}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        ) ON CONFLICT (date, latitude, longitude) DO UPDATE SET
            avg_temperature_2m_24h = EXCLUDED.avg_temperature_2m_24h,
            avg_relative_humidity_2m_24h = EXCLUDED.avg_relative_humidity_2m_24h,
            avg_dew_point_2m_24h = EXCLUDED.avg_dew_point_2m_24h,
            avg_apparent_temperature_24h = EXCLUDED.avg_apparent_temperature_24h,
            avg_temperature_80m_24h = EXCLUDED.avg_temperature_80m_24h,
            avg_temperature_120m_24h = EXCLUDED.avg_temperature_120m_24h,
            avg_wind_speed_10m_24h = EXCLUDED.avg_wind_speed_10m_24h,
            avg_wind_speed_80m_24h = EXCLUDED.avg_wind_speed_80m_24h,
            avg_visibility_24h = EXCLUDED.avg_visibility_24h,
            total_rain_24h = EXCLUDED.total_rain_24h,
            total_showers_24h = EXCLUDED.total_showers_24h,
            total_snowfall_24h = EXCLUDED.total_snowfall_24h,
            avg_temperature_2m_daylight = EXCLUDED.avg_temperature_2m_daylight,
            avg_relative_humidity_2m_daylight = EXCLUDED.avg_relative_humidity_2m_daylight,
            avg_dew_point_2m_daylight = EXCLUDED.avg_dew_point_2m_daylight,
            avg_apparent_temperature_daylight = EXCLUDED.avg_apparent_temperature_daylight,
            avg_temperature_80m_daylight = EXCLUDED.avg_temperature_80m_daylight,
            avg_temperature_120m_daylight = EXCLUDED.avg_temperature_120m_daylight,
            avg_wind_speed_10m_daylight = EXCLUDED.avg_wind_speed_10m_daylight,
            avg_wind_speed_80m_daylight = EXCLUDED.avg_wind_speed_80m_daylight,
            avg_visibility_daylight = EXCLUDED.avg_visibility_daylight,
            total_rain_daylight = EXCLUDED.total_rain_daylight,
            total_showers_daylight = EXCLUDED.total_showers_daylight,
            total_snowfall_daylight = EXCLUDED.total_snowfall_daylight,
            wind_speed_10m_m_per_s = EXCLUDED.wind_speed_10m_m_per_s,
            wind_speed_80m_m_per_s = EXCLUDED.wind_speed_80m_m_per_s,
            temperature_2m_celsius = EXCLUDED.temperature_2m_celsius,
            apparent_temperature_celsius = EXCLUDED.apparent_temperature_celsius,
            temperature_80m_celsius = EXCLUDED.temperature_80m_celsius,
            temperature_120m_celsius = EXCLUDED.temperature_120m_celsius,
            soil_temperature_0cm_celsius = EXCLUDED.soil_temperature_0cm_celsius,
            soil_temperature_6cm_celsius = EXCLUDED.soil_temperature_6cm_celsius,
            rain_mm = EXCLUDED.rain_mm,
            showers_mm = EXCLUDED.showers_mm,
            snowfall_mm = EXCLUDED.snowfall_mm,
            daylight_hours = EXCLUDED.daylight_hours,
            sunset_iso = EXCLUDED.sunset_iso,
            sunrise_iso = EXCLUDED.sunrise_iso,
            updated_at = CURRENT_TIMESTAMP
        """, nativeQuery = true)
    void upsertWeatherRecord(@Param("record") WeatherRecord record);
}