package com.weatheretl.controller;

import com.weatheretl.service.WeatherDatabaseService;
import com.weatheretl.service.WeatherEtlService;
import com.weatheretl.service.WeatherEtlService.EtlResult;
import com.weatheretl.service.WeatherEtlService.EtlStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(WeatherEtlController.class)
@DisplayName("Weather ETL Controller Tests")
class WeatherEtlControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private WeatherEtlService weatherEtlService;

    private LocalDate startDate;
    private LocalDate endDate;
    private EtlResult successResult;
    private EtlResult failureResult;
    private EtlStats mockStats;

    @BeforeEach
    void setUp() {
        startDate = LocalDate.of(2024, 1, 1);
        endDate = LocalDate.of(2024, 1, 3);

        successResult = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(10)
                .csvExported(true)
                .databaseSaved(false)
                .build();
        failureResult = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .errorMessage("API connection failed")
                .apiResponseReceived(false)
                .recordsTransformed(0)
                .csvExported(false)
                .databaseSaved(false)
                .build();

        List<WeatherDatabaseService.LocationInfo> locations = Arrays.asList(
                new WeatherDatabaseService.LocationInfo(40.7128, -74.0060),
                new WeatherDatabaseService.LocationInfo(34.0522, -118.2437)
        );
        mockStats = EtlStats.builder()
                .totalRecordsInDatabase(1000L)
                .uniqueLocations(2)
                .locations(locations)
                .build();
    }

    @Test
    @DisplayName("Should execute API to CSV successfully")
    void shouldExecuteApiToCsvSuccessfully() throws Exception {
        when(weatherEtlService.executeApiToCsv(startDate, endDate, null))
                .thenReturn(successResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.startDate", is("2024-01-01")))
                .andExpect(jsonPath("$.endDate", is("2024-01-03")))
                .andExpect(jsonPath("$.recordsTransformed", is(10)))
                .andExpect(jsonPath("$.csvExported", is(true)))
                .andExpect(jsonPath("$.databaseSaved", is(false)))
                .andExpect(jsonPath("$.errorMessage").doesNotExist());
    }

    @Test
    @DisplayName("Should execute API to CSV with custom path")
    void shouldExecuteApiToCsvWithCustomPath() throws Exception {
        String customPath = "/custom/path/weather.csv";
        when(weatherEtlService.executeApiToCsv(startDate, endDate, customPath))
                .thenReturn(successResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .param("csvPath", customPath)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)));
    }

    @Test
    @DisplayName("Should handle API to CSV failure")
    void shouldHandleApiToCsvFailure() throws Exception {
        when(weatherEtlService.executeApiToCsv(startDate, endDate, null))
                .thenReturn(failureResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success", is(false)))
                .andExpect(jsonPath("$.errorMessage", is("API connection failed")));
    }

    @Test
    @DisplayName("Should execute API to Database successfully")
    void shouldExecuteApiToDatabaseSuccessfully() throws Exception {
        EtlResult dbResult = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(15)
                .csvExported(false)
                .databaseSaved(true)
                .build();

        when(weatherEtlService.executeApiToDatabase(startDate, endDate))
                .thenReturn(dbResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-database")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.recordsTransformed", is(15)))
                .andExpect(jsonPath("$.csvExported", is(false)))
                .andExpect(jsonPath("$.databaseSaved", is(true)));
    }

    @Test
    @DisplayName("Should execute API to CSV and Database successfully")
    void shouldExecuteApiToCsvAndDatabaseSuccessfully() throws Exception {
        EtlResult bothResult = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(20)
                .csvExported(true)
                .databaseSaved(true)
                .build();
        when(weatherEtlService.executeApiToCsvAndDatabase(startDate, endDate, null))
                .thenReturn(bothResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-all")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.recordsTransformed", is(20)))
                .andExpect(jsonPath("$.csvExported", is(true)))
                .andExpect(jsonPath("$.databaseSaved", is(true)));
    }

    @Test
    @DisplayName("Should get ETL statistics successfully")
    void shouldGetEtlStatisticsSuccessfully() throws Exception {
        when(weatherEtlService.getEtlStats()).thenReturn(mockStats);
        mockMvc.perform(get("/api/v1/weather-etl/stats")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalRecordsInDatabase", is(1000)))
                .andExpect(jsonPath("$.uniqueLocations", is(2)))
                .andExpect(jsonPath("$.locations", hasSize(2)))
                .andExpect(jsonPath("$.locations[0].latitude", is(40.7128)))
                .andExpect(jsonPath("$.locations[0].longitude", is(-74.0060)))
                .andExpect(jsonPath("$.locations[1].latitude", is(34.0522)))
                .andExpect(jsonPath("$.locations[1].longitude", is(-118.2437)));
    }

    @Test
    @DisplayName("Should handle statistics service exception")
    void shouldHandleStatisticsServiceException() throws Exception {
        when(weatherEtlService.getEtlStats()).thenThrow(new RuntimeException("Database connection failed"));
        mockMvc.perform(get("/api/v1/weather-etl/stats")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError());
    }

    @Test
    @DisplayName("Should return health status")
    void shouldReturnHealthStatus() throws Exception {
        mockMvc.perform(get("/api/v1/weather-etl/health")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    @DisplayName("Should validate required start date parameter")
    void shouldValidateRequiredStartDateParameter() throws Exception {
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Should validate required end date parameter")
    void shouldValidateRequiredEndDateParameter() throws Exception {
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2024-01-01")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Should validate date format")
    void shouldValidateDateFormat() throws Exception {
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "invalid-date")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Should handle service exception in API to CSV")
    void shouldHandleServiceExceptionInApiToCsv() throws Exception {
        when(weatherEtlService.executeApiToCsv(any(LocalDate.class), any(LocalDate.class), eq(null)))
                .thenThrow(new RuntimeException("Unexpected service error"));
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success", is(false)))
                .andExpect(jsonPath("$.errorMessage", containsString("Internal server error")));
    }

    @Test
    @DisplayName("Should handle service exception in API to Database")
    void shouldHandleServiceExceptionInApiToDatabase() throws Exception {
        when(weatherEtlService.executeApiToDatabase(any(LocalDate.class), any(LocalDate.class)))
                .thenThrow(new RuntimeException("Database unavailable"));
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-database")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success", is(false)))
                .andExpect(jsonPath("$.errorMessage", containsString("Internal server error")));
    }

    @Test
    @DisplayName("Should handle service exception in API to All")
    void shouldHandleServiceExceptionInApiToAll() throws Exception {
        when(weatherEtlService.executeApiToCsvAndDatabase(any(LocalDate.class), any(LocalDate.class), eq(null)))
                .thenThrow(new RuntimeException("Multiple systems failure"));
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-all")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-01-03")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success", is(false)))
                .andExpect(jsonPath("$.errorMessage", containsString("Internal server error")));
    }

    @Test
    @DisplayName("Should handle edge case dates")
    void shouldHandleEdgeCaseDates() throws Exception {
        LocalDate edgeStartDate = LocalDate.of(2020, 1, 1);
        LocalDate edgeEndDate = LocalDate.of(2025, 12, 31);
        EtlResult edgeResult = EtlResult.builder()
                .startDate(edgeStartDate)
                .endDate(edgeEndDate)
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(100)
                .csvExported(true)
                .databaseSaved(false)
                .build();
        when(weatherEtlService.executeApiToCsv(edgeStartDate, edgeEndDate, null))
                .thenReturn(edgeResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2020-01-01")
                        .param("endDate", "2025-12-31")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.recordsTransformed", is(100)));
    }

    @Test
    @DisplayName("Should handle same start and end date")
    void shouldHandleSameStartAndEndDate() throws Exception {
        LocalDate sameDate = LocalDate.of(2024, 6, 15);
        EtlResult sameDateResult = EtlResult.builder()
                .startDate(sameDate)
                .endDate(sameDate)
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(1)
                .csvExported(true)
                .databaseSaved(false)
                .build();
        when(weatherEtlService.executeApiToCsv(sameDate, sameDate, null))
                .thenReturn(sameDateResult);
        mockMvc.perform(post("/api/v1/weather-etl/execute/api-to-csv")
                        .param("startDate", "2024-06-15")
                        .param("endDate", "2024-06-15")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success", is(true)))
                .andExpect(jsonPath("$.recordsTransformed", is(1)));
    }
}