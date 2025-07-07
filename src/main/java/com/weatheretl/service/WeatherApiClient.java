package com.weatheretl.service;

import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherApiClient {

    private final WebClient webClient;
    private final WeatherEtlConfig config;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public WeatherApiResponse fetchWeatherData(LocalDate startDate, LocalDate endDate) {
        return fetchWeatherData(
                config.getDefaultLocation().getLatitude(),
                config.getDefaultLocation().getLongitude(),
                startDate,
                endDate
        );
    }

    @Retryable(
            value = {WebClientException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public WeatherApiResponse fetchWeatherData(double latitude, double longitude,
                                               LocalDate startDate, LocalDate endDate) {
        log.info("Fetching weather data for coordinates: {}, {} from {} to {}",
                latitude, longitude, startDate, endDate);
        Map<String, Object> params = buildApiParams(latitude, longitude, startDate, endDate);
        try {
            WeatherApiResponse response = webClient
                    .get()
                    .uri(config.getApi().getBaseUrl(), uriBuilder -> {
                        params.forEach((key, value) -> {
                            if (value != null) {
                                uriBuilder.queryParam(key, value);
                            }
                        });
                        return uriBuilder.build();
                    })
                    .retrieve()
                    .bodyToMono(WeatherApiResponse.class)
                    .timeout(config.getApi().getTimeout())
                    .block();

            assert response != null;
            log.info("Successfully fetched weather data. Generation time: {} ms",
                    response.getGenerationTimeMs());
            return response;
        } catch (WebClientResponseException e) {
            log.error("API request failed with status: {} and body: {}",
                    e.getStatusCode(), e.getResponseBodyAsString());
            throw new WeatherApiException("Failed to fetch weather data: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error while fetching weather data", e);
            throw new WeatherApiException("Unexpected error: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> buildApiParams(double latitude, double longitude,
                                               LocalDate startDate, LocalDate endDate) {
        Map<String, Object> params = new HashMap<>();
        params.put("latitude", latitude);
        params.put("longitude", longitude);
        params.put("start_date", startDate.format(DATE_FORMATTER));
        params.put("end_date", endDate.format(DATE_FORMATTER));
        params.put("hourly", String.join(",",
                "temperature_2m",
                "relative_humidity_2m",
                "dew_point_2m",
                "apparent_temperature",
                "temperature_80m",
                "temperature_120m",
                "wind_speed_10m",
                "wind_speed_80m",
                "wind_direction_10m",
                "wind_direction_80m",
                "visibility",
                "evapotranspiration",
                "weather_code",
                "soil_temperature_0cm",
                "soil_temperature_6cm",
                "rain",
                "showers",
                "snowfall"
        ));
        params.put("daily", String.join(",",
                "sunrise",
                "sunset",
                "daylight_duration"
        ));
        params.put("timezone", "auto");
        params.put("timeformat", "unixtime");
        params.put("wind_speed_unit", "kn");
        params.put("temperature_unit", "fahrenheit");
        params.put("precipitation_unit", "inch");
        return params;
    }

    public static class WeatherApiException extends RuntimeException {
        public WeatherApiException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}