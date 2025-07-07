package com.weatheretl.service;

import com.weatheretl.model.api.WeatherApiModels.DailyData;
import com.weatheretl.model.api.WeatherApiModels.HourlyData;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.service.CsvExportService.CsvExportException;
import com.weatheretl.service.WeatherApiClient.WeatherApiException;
import com.weatheretl.service.WeatherDatabaseService.DatabaseOperationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Weather ETL Service Tests")
class WeatherEtlServiceTest {

    @Mock
    private WeatherApiClient weatherApiClient;

    @Mock
    private WeatherTransformer weatherTransformer;

    @Mock
    private CsvExportService csvExportService;

    @Mock
    private WeatherDatabaseService weatherDatabaseService;

    @InjectMocks
    private WeatherEtlService weatherEtlService;

    private LocalDate startDate;
    private LocalDate endDate;
    private WeatherApiResponse mockApiResponse;
    private List<WeatherRecord> mockWeatherRecords;

    @BeforeEach
    void setUp() {
        startDate = LocalDate.of(2024, 1, 1);
        endDate = LocalDate.of(2024, 1, 3);
        mockApiResponse = createMockApiResponse();
        mockWeatherRecords = createMockWeatherRecords();
    }

    @Test
    @DisplayName("Should successfully execute API to CSV ETL process")
    void shouldExecuteApiToCsvSuccessfully() throws Exception {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doNothing().when(csvExportService).exportToCsv(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate);
        assertTrue(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertTrue(result.isCsvExported());
        assertEquals(3, result.getRecordsTransformed());
        assertNull(result.getErrorMessage());
        assertEquals(startDate, result.getStartDate());
        assertEquals(endDate, result.getEndDate());
        verify(weatherApiClient).fetchWeatherData(startDate, endDate);
        verify(weatherTransformer).transformWeatherData(mockApiResponse);
        verify(csvExportService).exportToCsv(mockWeatherRecords);
    }

    @Test
    @DisplayName("Should execute API to CSV with custom path")
    void shouldExecuteApiToCsvWithCustomPath() throws Exception {
        String customPath = "/custom/path/weather.csv";
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doNothing().when(csvExportService).exportToCsv(mockWeatherRecords, customPath);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate, customPath);
        assertTrue(result.isSuccess());
        verify(csvExportService).exportToCsv(mockWeatherRecords, customPath);
    }

    @Test
    @DisplayName("Should handle API exception in ETL process")
    void shouldHandleApiException() {
        WeatherApiException apiException = new WeatherApiException("API failed", new RuntimeException());
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenThrow(apiException);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate);
        assertFalse(result.isSuccess());
        assertFalse(result.isApiResponseReceived());
        assertFalse(result.isCsvExported());
        assertEquals("API error: API failed", result.getErrorMessage());
        verify(weatherApiClient).fetchWeatherData(startDate, endDate);
        verifyNoInteractions(weatherTransformer, csvExportService);
    }

    @Test
    @DisplayName("Should handle CSV export exception")
    void shouldHandleCsvExportException() throws Exception {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doThrow(new CsvExportException("CSV failed", new RuntimeException()))
                .when(csvExportService).exportToCsv(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate);
        assertFalse(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertFalse(result.isCsvExported());
        assertEquals("CSV export error: CSV failed", result.getErrorMessage());
    }

    @Test
    @DisplayName("Should handle empty transformed records")
    void shouldHandleEmptyTransformedRecords() {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(Collections.emptyList());
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate);
        assertFalse(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertFalse(result.isCsvExported());
        assertEquals(0, result.getRecordsTransformed());
        assertEquals("No records were transformed from API response", result.getErrorMessage());
        verifyNoInteractions(csvExportService);
    }

    @Test
    @DisplayName("Should successfully execute API to Database ETL process")
    void shouldExecuteApiToDatabaseSuccessfully() {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doNothing().when(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToDatabase(startDate, endDate);
        assertTrue(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertTrue(result.isDatabaseSaved());
        assertEquals(3, result.getRecordsTransformed());
        verify(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
    }

    @Test
    @DisplayName("Should handle database exception")
    void shouldHandleDatabaseException() {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doThrow(new DatabaseOperationException("Database failed", new RuntimeException()))
                .when(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToDatabase(startDate, endDate);
        assertFalse(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertFalse(result.isDatabaseSaved());
        assertEquals("Database error: Database failed", result.getErrorMessage());
    }

    @Test
    @DisplayName("Should execute API to CSV and Database successfully")
    void shouldExecuteApiToCsvAndDatabaseSuccessfully() throws Exception {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doNothing().when(csvExportService).exportToCsv(mockWeatherRecords);
        doNothing().when(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsvAndDatabase(startDate, endDate);
        assertTrue(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertTrue(result.isCsvExported());
        assertTrue(result.isDatabaseSaved());
        assertEquals(3, result.getRecordsTransformed());
        verify(csvExportService).exportToCsv(mockWeatherRecords);
        verify(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
    }

    @Test
    @DisplayName("Should continue with database save if CSV export fails")
    void shouldContinueWithDatabaseIfCsvFails() throws Exception {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doThrow(new CsvExportException("CSV failed", new RuntimeException()))
                .when(csvExportService).exportToCsv(mockWeatherRecords);
        doNothing().when(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsvAndDatabase(startDate, endDate);
        assertTrue(result.isSuccess());
        assertFalse(result.isCsvExported());
        assertTrue(result.isDatabaseSaved());
        assertTrue(result.getErrorMessage().contains("CSV export failed"));
    }

    @Test
    @DisplayName("Should process JSON data successfully")
    void shouldProcessJsonDataSuccessfully() throws Exception {
        when(weatherTransformer.transformWeatherData(mockApiResponse)).thenReturn(mockWeatherRecords);
        doNothing().when(csvExportService).exportToCsv(mockWeatherRecords);
        doNothing().when(weatherDatabaseService).saveWeatherRecords(mockWeatherRecords);
        WeatherEtlService.EtlResult result = weatherEtlService.processJsonData(
                mockApiResponse, true, true, null);
        assertTrue(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertTrue(result.isCsvExported());
        assertTrue(result.isDatabaseSaved());
        assertEquals(3, result.getRecordsTransformed());
    }

    @Test
    @DisplayName("Should get ETL stats successfully")
    void shouldGetEtlStatsSuccessfully() {
        WeatherDatabaseService.DatabaseStats dbStats = WeatherDatabaseService.DatabaseStats.builder()
                .totalRecords(100L)
                .uniqueLocations(5)
                .locations(Arrays.asList(
                        new WeatherDatabaseService.LocationInfo(40.7128, -74.0060),
                        new WeatherDatabaseService.LocationInfo(34.0522, -118.2437)
                ))
                .build();
        when(weatherDatabaseService.getDatabaseStats()).thenReturn(dbStats);
        WeatherEtlService.EtlStats stats = weatherEtlService.getEtlStats();
        assertNotNull(stats);
        assertEquals(100L, stats.getTotalRecordsInDatabase());
        assertEquals(5, stats.getUniqueLocations());
        assertEquals(2, stats.getLocations().size());
        verify(weatherDatabaseService).getDatabaseStats();
    }

    private WeatherApiResponse createMockApiResponse() {
        return WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .generationTimeMs(123.45)
                .hourly(createMockHourlyData())
                .daily(createMockDailyData())
                .build();
    }

    private HourlyData createMockHourlyData() {
        return HourlyData.builder()
                .time(Arrays.asList(1704067200L, 1704070800L, 1704074400L))
                .temperature2m(Arrays.asList(20.5, 21.0, 22.3))
                .relativeHumidity2m(Arrays.asList(65, 63, 68))
                .build();
    }

    private DailyData createMockDailyData() {
        return DailyData.builder()
                .time(List.of(1704067200L))
                .sunrise(List.of(1704091800L))
                .sunset(List.of(1704130200L))
                .daylightDuration(List.of(38400))
                .build();
    }

    private List<WeatherRecord> createMockWeatherRecords() {
        WeatherRecord record1 = new WeatherRecord();
        record1.setDate(LocalDate.of(2024, 1, 1));
        record1.setLatitude(40.7128);
        record1.setLongitude(-74.0060);
        record1.setAvgTemperature2m24h(20.5);

        WeatherRecord record2 = new WeatherRecord();
        record2.setDate(LocalDate.of(2024, 1, 2));
        record2.setLatitude(40.7128);
        record2.setLongitude(-74.0060);
        record2.setAvgTemperature2m24h(21.0);

        WeatherRecord record3 = new WeatherRecord();
        record3.setDate(LocalDate.of(2024, 1, 3));
        record3.setLatitude(40.7128);
        record3.setLongitude(-74.0060);
        record3.setAvgTemperature2m24h(22.3);

        return Arrays.asList(record1, record2, record3);
    }
}