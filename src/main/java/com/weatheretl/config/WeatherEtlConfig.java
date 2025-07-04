package com.weatheretl.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "weather")
public class WeatherEtlConfig {

    private ApiConfig api = new ApiConfig();
    private DefaultLocationConfig defaultLocation = new DefaultLocationConfig();
    private OutputConfig output = new OutputConfig();

    @Data
    public static class ApiConfig {
        private String baseUrl = "https://api.open-meteo.com/v1/forecast";
        private Duration timeout = Duration.ofSeconds(30);
        private RetryConfig retry = new RetryConfig();
    }

    @Data
    public static class RetryConfig {
        private int maxAttempts = 3;
        private Duration delay = Duration.ofSeconds(1);
    }

    @Data
    public static class DefaultLocationConfig {
        private double latitude = 55.0344;
        private double longitude = 82.9434;
    }

    @Data
    public static class OutputConfig {
        private String csvPath = "./output/weather_data.csv";
        private int batchSize = 1000;
    }
}