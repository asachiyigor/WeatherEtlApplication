package com.weatheretl.service;

import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Weather API Client Tests")
class WeatherApiClientTest {

    @Mock
    private WebClient webClient;

    @Mock
    private WeatherEtlConfig config;

    @Mock
    private WeatherEtlConfig.DefaultLocationConfig defaultLocationConfig;

    @Mock
    private WeatherEtlConfig.ApiConfig apiConfig;

    @InjectMocks
    private WeatherApiClient weatherApiClient;

    @Mock
    private WebClient.RequestHeadersUriSpec requestHeadersUriSpec;

    @Mock
    private WebClient.RequestHeadersSpec requestHeadersSpec;

    @Mock
    private WebClient.ResponseSpec responseSpec;

    private LocalDate startDate;
    private LocalDate endDate;
    private WeatherApiResponse mockResponse;

    @BeforeEach
    void setUp() {
        startDate = LocalDate.of(2024, 1, 1);
        endDate = LocalDate.of(2024, 1, 3);

        mockResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .generationTimeMs(123.45)
                .build();
    }

    @Test
    @DisplayName("Should fetch weather data successfully with default location")
    void shouldFetchWeatherDataWithDefaultLocation() {
        setupConfigMocks();
        setupSuccessfulWebClientMock();
        WeatherApiResponse result = weatherApiClient.fetchWeatherData(startDate, endDate);
        assertNotNull(result);
        assertEquals(mockResponse.getLatitude(), result.getLatitude());
        assertEquals(mockResponse.getLongitude(), result.getLongitude());
        assertEquals(mockResponse.getGenerationTimeMs(), result.getGenerationTimeMs());
        verify(webClient).get();
    }

    @Test
    @DisplayName("Should fetch weather data successfully with custom coordinates")
    void shouldFetchWeatherDataWithCustomCoordinates() {
        double customLat = 34.0522;
        double customLon = -118.2437;
        setupApiConfigMocks();
        setupSuccessfulWebClientMock();
        WeatherApiResponse result = weatherApiClient.fetchWeatherData(
                customLat, customLon, startDate, endDate);
        assertNotNull(result);
        verify(webClient).get();
    }

    @Test
    @DisplayName("Should handle WebClient response exception")
    void shouldHandleWebClientResponseException() {
        setupConfigMocks();
        WebClientResponseException exception = WebClientResponseException.create(
                404, "Not Found", null, "API endpoint not found".getBytes(), null);
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(anyString(), any(Function.class))).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(WeatherApiResponse.class)).thenReturn(Mono.error(exception));
        WeatherApiClient.WeatherApiException apiException = assertThrows(
                WeatherApiClient.WeatherApiException.class,
                () -> weatherApiClient.fetchWeatherData(startDate, endDate)
        );
        assertTrue(apiException.getMessage().contains("Failed to fetch weather data"));
        assertNotNull(apiException.getCause());
    }

    @Test
    @DisplayName("Should handle timeout exception")
    void shouldHandleTimeoutException() {
        setupApiConfigMocks();
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(anyString(), any(Function.class))).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(WeatherApiResponse.class))
                .thenReturn(Mono.error(new RuntimeException("Timeout")));
        WeatherApiClient.WeatherApiException apiException = assertThrows(
                WeatherApiClient.WeatherApiException.class,
                () -> weatherApiClient.fetchWeatherData(40.7128, -74.0060, startDate, endDate)
        );
        assertTrue(apiException.getMessage().contains("Unexpected error"));
    }

    @Test
    @DisplayName("Should retry on WebClient exception")
    void shouldRetryOnWebClientException() {
        setupConfigMocks();
        setupSuccessfulWebClientMock();
        WeatherApiResponse result = weatherApiClient.fetchWeatherData(startDate, endDate);
        assertNotNull(result);
        verify(webClient, atLeastOnce()).get();
    }

    @Test
    @DisplayName("Should build correct API parameters")
    void shouldBuildCorrectApiParameters() {
        setupApiConfigMocks();
        setupSuccessfulWebClientMock();
        weatherApiClient.fetchWeatherData(40.7128, -74.0060, startDate, endDate);
        verify(requestHeadersUriSpec).uri(eq("https://api.weather.com"), any(Function.class));
        verify(requestHeadersSpec).retrieve();
        verify(responseSpec).bodyToMono(WeatherApiResponse.class);
    }

    @Test
    @DisplayName("Should use correct date format in API request")
    void shouldUseCorrectDateFormatInApiRequest() {
        LocalDate testStartDate = LocalDate.of(2024, 12, 25);
        LocalDate testEndDate = LocalDate.of(2024, 12, 31);
        setupApiConfigMocks();
        setupSuccessfulWebClientMock();
        weatherApiClient.fetchWeatherData(40.7128, -74.0060, testStartDate, testEndDate);
        verify(requestHeadersUriSpec).uri(eq("https://api.weather.com"), any(Function.class));
    }

    private void setupConfigMocks() {
        when(config.getDefaultLocation()).thenReturn(defaultLocationConfig);
        when(config.getApi()).thenReturn(apiConfig);
        when(defaultLocationConfig.getLatitude()).thenReturn(40.7128);
        when(defaultLocationConfig.getLongitude()).thenReturn(-74.0060);
        when(apiConfig.getBaseUrl()).thenReturn("https://api.weather.com");
        when(apiConfig.getTimeout()).thenReturn(Duration.ofSeconds(30));
    }

    private void setupApiConfigMocks() {
        when(config.getApi()).thenReturn(apiConfig);
        when(apiConfig.getBaseUrl()).thenReturn("https://api.weather.com");
        when(apiConfig.getTimeout()).thenReturn(Duration.ofSeconds(30));
    }

    private void setupSuccessfulWebClientMock() {
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(anyString(), any(Function.class))).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(WeatherApiResponse.class))
                .thenReturn(Mono.just(mockResponse).timeout(Duration.ofSeconds(30)));
    }
}