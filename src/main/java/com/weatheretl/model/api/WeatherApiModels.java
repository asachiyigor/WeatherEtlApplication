package com.weatheretl.model.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

public class WeatherApiModels {

    @Data
    public static class WeatherApiResponse {
        private double latitude;
        private double longitude;

        @JsonProperty("generationtime_ms")
        private double generationTimeMs;

        @JsonProperty("utc_offset_seconds")
        private int utcOffsetSeconds;

        private String timezone;

        @JsonProperty("timezone_abbreviation")
        private String timezoneAbbreviation;

        private double elevation;

        @JsonProperty("hourly_units")
        private HourlyUnits hourlyUnits;

        private HourlyData hourly;

        @JsonProperty("daily_units")
        private DailyUnits dailyUnits;

        private DailyData daily;
    }

    @Data
    public static class HourlyUnits {
        private String time;

        @JsonProperty("temperature_2m")
        private String temperature2m;

        @JsonProperty("relative_humidity_2m")
        private String relativeHumidity2m;

        @JsonProperty("dew_point_2m")
        private String dewPoint2m;

        @JsonProperty("apparent_temperature")
        private String apparentTemperature;

        @JsonProperty("temperature_80m")
        private String temperature80m;

        @JsonProperty("temperature_120m")
        private String temperature120m;

        @JsonProperty("wind_speed_10m")
        private String windSpeed10m;

        @JsonProperty("wind_speed_80m")
        private String windSpeed80m;

        @JsonProperty("wind_direction_10m")
        private String windDirection10m;

        @JsonProperty("wind_direction_80m")
        private String windDirection80m;

        private String visibility;
        private String evapotranspiration;

        @JsonProperty("weather_code")
        private String weatherCode;

        @JsonProperty("soil_temperature_0cm")
        private String soilTemperature0cm;

        @JsonProperty("soil_temperature_6cm")
        private String soilTemperature6cm;

        private String rain;
        private String showers;
        private String snowfall;
    }

    @Data
    public static class HourlyData {
        private List<Long> time;

        @JsonProperty("temperature_2m")
        private List<Double> temperature2m;

        @JsonProperty("relative_humidity_2m")
        private List<Integer> relativeHumidity2m;

        @JsonProperty("dew_point_2m")
        private List<Double> dewPoint2m;

        @JsonProperty("apparent_temperature")
        private List<Double> apparentTemperature;

        @JsonProperty("temperature_80m")
        private List<Double> temperature80m;

        @JsonProperty("temperature_120m")
        private List<Double> temperature120m;

        @JsonProperty("wind_speed_10m")
        private List<Double> windSpeed10m;

        @JsonProperty("wind_speed_80m")
        private List<Double> windSpeed80m;

        @JsonProperty("wind_direction_10m")
        private List<Integer> windDirection10m;

        @JsonProperty("wind_direction_80m")
        private List<Integer> windDirection80m;

        private List<Double> visibility;
        private List<Double> evapotranspiration;

        @JsonProperty("weather_code")
        private List<Integer> weatherCode;

        @JsonProperty("soil_temperature_0cm")
        private List<Double> soilTemperature0cm;

        @JsonProperty("soil_temperature_6cm")
        private List<Double> soilTemperature6cm;

        private List<Double> rain;
        private List<Double> showers;
        private List<Double> snowfall;
    }

    @Data
    public static class DailyUnits {
        private String time;
        private String sunrise;
        private String sunset;

        @JsonProperty("daylight_duration")
        private String daylightDuration;
    }

    @Data
    public static class DailyData {
        private List<Long> time;
        private List<Long> sunrise;
        private List<Long> sunset;

        @JsonProperty("daylight_duration")
        private List<Integer> daylightDuration;
    }
}