package com.weatheretl.integration;

import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.api.WeatherApiModels.DailyData;
import com.weatheretl.model.api.WeatherApiModels.HourlyData;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.service.CsvExportService;
import com.weatheretl.service.WeatherApiClient;
import com.weatheretl.service.WeatherDatabaseService;
import com.weatheretl.service.WeatherEtlService;
import com.weatheretl.service.WeatherTransformer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Weather ETL Service Integration Tests")
class WeatherEtlServiceIntegrationTest {

    @Autowired
    private WeatherEtlService weatherEtlService;

    @MockBean
    private WeatherApiClient weatherApiClient;

    @Autowired
    private WeatherTransformer weatherTransformer;

    @TempDir
    Path tempDir;

    private WeatherApiResponse mockApiResponse;
    private LocalDate startDate;
    private LocalDate endDate;

    @BeforeEach
    void setUp() {
        startDate = LocalDate.of(2024, 1, 1);
        endDate = LocalDate.of(2024, 1, 2);
        mockApiResponse = createRealisticApiResponse();
    }

    @Test
    @DisplayName("Should execute full ETL pipeline from API to CSV integration")
    void shouldExecuteFullEtlPipelineApiToCsv() throws Exception {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        String csvPath = tempDir.resolve("integration_test.csv").toString();
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate, csvPath);
        assertTrue(result.isSuccess(), "ETL process should succeed");
        assertTrue(result.isApiResponseReceived(), "API response should be received");
        assertTrue(result.isCsvExported(), "CSV should be exported");
        assertEquals(2, result.getRecordsTransformed(), "Should transform 2 records");
        assertNull(result.getErrorMessage(), "Should have no error message");
        assertTrue(Files.exists(Path.of(csvPath)), "CSV file should exist");
        List<String> lines = Files.readAllLines(Path.of(csvPath));
        assertEquals(3, lines.size(), "Should have header + 2 data rows");
        List<WeatherRecord> transformedRecords = weatherTransformer.transformWeatherData(mockApiResponse);
        assertEquals(2, transformedRecords.size(), "Should have 2 transformed records");
        assertEquals(40.7128, transformedRecords.get(0).getLatitude(), 0.001, "First record should have correct latitude");
        assertEquals(-74.0060, transformedRecords.get(0).getLongitude(), 0.001, "First record should have correct longitude");
        String firstDataRow = lines.get(1);
        assertFalse(firstDataRow.trim().isEmpty(), "First data row should not be empty");
        assertTrue(firstDataRow.contains("2024-01-01"), "Should contain the expected start date");
        String secondDataRow = lines.get(2);
        assertFalse(secondDataRow.trim().isEmpty(), "Second data row should not be empty");
        assertTrue(secondDataRow.contains("2024-01-02"), "Should contain the expected end date");
    }

    @Test
    @DisplayName("Should execute full ETL pipeline from API to Database integration")
    void shouldExecuteFullEtlPipelineApiToDatabase() {
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(mockApiResponse);
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToDatabase(startDate, endDate);
        assertTrue(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertTrue(result.isDatabaseSaved());
        assertEquals(2, result.getRecordsTransformed());
    }

    @Test
    @DisplayName("Should transform realistic API response correctly")
    void shouldTransformRealisticApiResponseCorrectly() {
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(mockApiResponse);
        assertNotNull(records);
        assertEquals(2, records.size());
        WeatherRecord record1 = records.get(0);
        assertEquals(LocalDate.of(2024, 1, 1), record1.getDate());
        assertEquals(40.7128, record1.getLatitude(), 0.001);
        assertEquals(-74.0060, record1.getLongitude(), 0.001);
        assertNotNull(record1.getAvgTemperature2m24h());
        assertNotNull(record1.getTemperature2mCelsius());
        assertNotNull(record1.getTotalRain24h());
        assertNotNull(record1.getRainMm());
        assertNotNull(record1.getAvgWindSpeed10m24h());
        assertNotNull(record1.getWindSpeed10mMPerS());
        assertNotNull(record1.getDaylightHours());
        assertNotNull(record1.getSunriseIso());
        assertNotNull(record1.getSunsetIso());
    }

    @Test
    @DisplayName("Should handle processing JSON data directly")
    void shouldHandleProcessingJsonDataDirectly() {
        String csvPath = tempDir.resolve("json_processing_test.csv").toString();
        WeatherEtlService.EtlResult result = weatherEtlService.processJsonData(
                mockApiResponse, true, false, csvPath);
        assertTrue(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertTrue(result.isCsvExported());
        assertFalse(result.isDatabaseSaved());
        assertEquals(2, result.getRecordsTransformed());
        assertTrue(Files.exists(Path.of(csvPath)));
    }

    @Test
    @DisplayName("Should validate data quality after transformation")
    void shouldValidateDataQualityAfterTransformation() {
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(mockApiResponse);
        for (WeatherRecord record : records) {
            assertNotNull(record.getDate());
            assertNotNull(record.getLatitude());
            assertNotNull(record.getLongitude());
            if (record.getAvgTemperature2m24h() != null) {
                assertTrue(record.getAvgTemperature2m24h() >= -100 && record.getAvgTemperature2m24h() <= 150);
            }
            if (record.getAvgRelativeHumidity2m24h() != null) {
                assertTrue(record.getAvgRelativeHumidity2m24h() >= 0 && record.getAvgRelativeHumidity2m24h() <= 100);
            }
            if (record.getAvgWindSpeed10m24h() != null) {
                assertTrue(record.getAvgWindSpeed10m24h() >= 0);
            }
            if (record.getTotalRain24h() != null) {
                assertTrue(record.getTotalRain24h() >= 0);
            }
            if (record.getDaylightHours() != null) {
                assertTrue(record.getDaylightHours() >= 0 && record.getDaylightHours() <= 24);
            }
            if (record.getAvgTemperature2m24h() != null && record.getTemperature2mCelsius() != null) {
                double expectedCelsius = (record.getAvgTemperature2m24h() - 32) * 5.0 / 9.0;
                assertEquals(expectedCelsius, record.getTemperature2mCelsius(), 0.1);
            }
            if (record.getTotalRain24h() != null && record.getRainMm() != null) {
                double expectedMm = record.getTotalRain24h() * 25.4;
                assertEquals(expectedMm, record.getRainMm(), 0.1);
            }
        }
    }

    @Test
    @DisplayName("Should handle end-to-end error scenarios gracefully")
    void shouldHandleEndToEndErrorScenariosGracefully() {
        WeatherApiResponse corruptedResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(null)
                .daily(null)
                .build();
        when(weatherApiClient.fetchWeatherData(startDate, endDate)).thenReturn(corruptedResponse);
        String csvPath = tempDir.resolve("error_test.csv").toString();
        WeatherEtlService.EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate, csvPath);
        assertFalse(result.isSuccess());
        assertTrue(result.isApiResponseReceived());
        assertFalse(result.isCsvExported());
        assertEquals(0, result.getRecordsTransformed());
        assertEquals("No records were transformed from API response", result.getErrorMessage());
        assertFalse(Files.exists(Path.of(csvPath)));
    }

    @Test
    @DisplayName("Should validate complex unit conversions")
    void shouldValidateComplexUnitConversions() {
        WeatherApiResponse preciseResponse = createPreciseConversionApiResponse();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(preciseResponse);
        assertEquals(1, records.size());
        WeatherRecord record = records.get(0);
        assertEquals(32.0, record.getAvgTemperature2m24h(), 0.1);
        assertEquals(0.0, record.getTemperature2mCelsius(), 0.1);
        assertEquals(1.0, record.getTotalRain24h(), 0.1);
        assertEquals(25.4, record.getRainMm(), 0.1);
        assertEquals(10.0, record.getAvgWindSpeed10m24h(), 0.1);
        assertEquals(5.14, record.getWindSpeed10mMPerS(), 0.1);
        assertEquals(10.0, record.getDaylightHours(), 0.1);
    }

    @Test
    @DisplayName("Should handle multiple location processing")
    void shouldHandleMultipleLocationProcessing() throws Exception {
        WeatherApiResponse multiLocationResponse = createMultiLocationApiResponse();
        String csvPath = tempDir.resolve("multi_location_test.csv").toString();
        WeatherEtlService.EtlResult result = weatherEtlService.processJsonData(
                multiLocationResponse, true, false, csvPath);
        assertTrue(result.isSuccess(), "ETL process should succeed");
        assertEquals(1, result.getRecordsTransformed(), "Should transform 1 record");
        assertTrue(Files.exists(Path.of(csvPath)), "CSV file should be created");
        List<String> lines = Files.readAllLines(Path.of(csvPath));
        assertTrue(lines.size() >= 2, "CSV should have at least header and one data row");
        List<WeatherRecord> transformedRecords = weatherTransformer.transformWeatherData(multiLocationResponse);
        assertEquals(1, transformedRecords.size(), "Should have exactly 1 transformed record");
        WeatherRecord record = transformedRecords.get(0);
        assertEquals(34.0522, record.getLatitude(), 0.001, "Should have Los Angeles latitude");
        assertEquals(-118.2437, record.getLongitude(), 0.001, "Should have Los Angeles longitude");
        String dataLine = lines.get(1);
        assertFalse(dataLine.trim().isEmpty(), "Data line should not be empty");
        assertTrue(dataLine.matches(".*\\d+.*"), "Data line should contain numeric values");
        assertTrue(dataLine.contains("2024-01-01"), "Data should contain the expected date");
    }

    @Test
    @DisplayName("Should validate CSV content structure")
    void shouldValidateCsvContentStructure() throws Exception {
        String csvPath = tempDir.resolve("structure_test.csv").toString();
        WeatherEtlService.EtlResult result = weatherEtlService.processJsonData(
                mockApiResponse, true, false, csvPath);
        assertTrue(result.isSuccess());
        assertTrue(Files.exists(Path.of(csvPath)));
        List<String> lines = Files.readAllLines(Path.of(csvPath));
        String header = lines.get(0);
        String[] headerColumns = header.split(",");
        assertTrue(headerColumns.length > 5);
        for (int i = 1; i < lines.size(); i++) {
            String[] dataColumns = lines.get(i).split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            assertTrue(dataColumns.length >= headerColumns.length / 2,
                    "Row " + i + " should have reasonable number of columns");
        }
    }

    private WeatherApiResponse createRealisticApiResponse() {
        return WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .generationTimeMs(42.123)
                .utcOffsetSeconds(-18000)
                .timezone("America/New_York")
                .timezoneAbbreviation("EST")
                .elevation(10.0)
                .hourly(createRealisticHourlyData())
                .daily(createRealisticDailyData())
                .build();
    }

    private HourlyData createRealisticHourlyData() {
        return HourlyData.builder()
                .time(Arrays.asList(
                        1704067200L, 1704070800L, 1704074400L, 1704078000L, 1704081600L, 1704085200L,
                        1704153600L, 1704157200L, 1704160800L, 1704164400L, 1704168000L, 1704171600L
                ))
                .temperature2m(Arrays.asList(
                        28.4, 27.9, 29.1, 31.5, 34.2, 36.8, // Jan 1: -2°C to 2°C
                        25.6, 24.8, 26.7, 29.3, 32.1, 35.2  // Jan 2: -4°C to 2°C
                ))
                .relativeHumidity2m(Arrays.asList(
                        72, 75, 69, 65, 61, 58,
                        78, 81, 76, 71, 67, 63
                ))
                .dewPoint2m(Arrays.asList(
                        19.2, 18.8, 19.7, 20.1, 19.9, 20.4,
                        18.1, 17.9, 18.8, 19.2, 19.1, 19.6
                ))
                .apparentTemperature(Arrays.asList(
                        25.1, 24.6, 26.3, 28.7, 31.4, 34.0,
                        22.3, 21.5, 23.4, 26.0, 28.8, 31.9
                ))
                .temperature80m(Arrays.asList(
                        28.8, 28.3, 29.5, 31.9, 34.6, 37.2,
                        26.0, 25.2, 27.1, 29.7, 32.5, 35.6
                ))
                .temperature120m(Arrays.asList(
                        29.1, 28.6, 29.8, 32.2, 34.9, 37.5,
                        26.3, 25.5, 27.4, 30.0, 32.8, 35.9
                ))
                .windSpeed10m(Arrays.asList(
                        4.2, 4.8, 3.9, 5.1, 5.7, 6.2,
                        3.8, 4.1, 4.5, 5.3, 5.9, 6.4
                ))
                .windSpeed80m(Arrays.asList(
                        5.2, 5.8, 4.9, 6.1, 6.7, 7.2,
                        4.8, 5.1, 5.5, 6.3, 6.9, 7.4
                ))
                .windDirection10m(Arrays.asList(
                        245, 250, 248, 252, 255, 258,
                        242, 246, 249, 253, 256, 260
                ))
                .windDirection80m(Arrays.asList(
                        248, 253, 251, 255, 258, 261,
                        245, 249, 252, 256, 259, 263
                ))
                .visibility(Arrays.asList(
                        24140.0, 24140.0, 24140.0, 24140.0, 24140.0, 24140.0,
                        24140.0, 24140.0, 24140.0, 24140.0, 24140.0, 24140.0
                ))
                .evapotranspiration(Arrays.asList(
                        0.0, 0.0, 0.0, 0.002, 0.008, 0.012,
                        0.0, 0.0, 0.0, 0.001, 0.007, 0.011
                ))
                .weatherCode(Arrays.asList(
                        1, 1, 2, 2, 3, 3,
                        0, 1, 1, 2, 2, 3
                ))
                .soilTemperature0cm(Arrays.asList(
                        30.2, 29.8, 30.1, 31.2, 32.8, 34.1,
                        28.9, 28.5, 29.3, 30.4, 31.9, 33.2
                ))
                .soilTemperature6cm(Arrays.asList(
                        31.1, 30.9, 31.0, 31.5, 32.3, 33.0,
                        30.5, 30.3, 30.6, 31.1, 31.8, 32.5
                ))
                .rain(Arrays.asList(
                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                        0.0, 0.004, 0.008, 0.012, 0.004, 0.0
                ))
                .showers(Arrays.asList(
                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                ))
                .snowfall(Arrays.asList(
                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                ))
                .build();
    }

    private DailyData createRealisticDailyData() {
        return DailyData.builder()
                .time(Arrays.asList(
                        1704067200L,
                        1704153600L
                ))
                .sunrise(Arrays.asList(
                        1704110400L,
                        1704196800L
                ))
                .sunset(Arrays.asList(
                        1704146400L,
                        1704232800L
                ))
                .daylightDuration(Arrays.asList(
                        36000,
                        36000
                ))
                .build();
    }

    private WeatherApiResponse createPreciseConversionApiResponse() {
        return WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(HourlyData.builder()
                        .time(Arrays.asList(1704067200L, 1704070800L, 1704074400L))
                        .temperature2m(Arrays.asList(32.0, 32.0, 32.0))
                        .rain(Arrays.asList(1.0, 0.0, 0.0))
                        .windSpeed10m(Arrays.asList(10.0, 10.0, 10.0))
                        .relativeHumidity2m(Arrays.asList(50, 50, 50))
                        .dewPoint2m(Arrays.asList(20.0, 20.0, 20.0))
                        .apparentTemperature(Arrays.asList(30.0, 30.0, 30.0))
                        .temperature80m(Arrays.asList(32.5, 32.5, 32.5))
                        .temperature120m(Arrays.asList(33.0, 33.0, 33.0))
                        .windSpeed80m(Arrays.asList(12.0, 12.0, 12.0))
                        .visibility(Arrays.asList(10000.0, 10000.0, 10000.0))
                        .soilTemperature0cm(Arrays.asList(35.0, 35.0, 35.0))
                        .soilTemperature6cm(Arrays.asList(36.0, 36.0, 36.0))
                        .showers(Arrays.asList(0.0, 0.0, 0.0))
                        .snowfall(Arrays.asList(0.0, 0.0, 0.0))
                        .build())
                .daily(DailyData.builder()
                        .time(List.of(1704067200L))
                        .sunrise(List.of(1704091800L))
                        .sunset(List.of(1704127800L))
                        .daylightDuration(List.of(36000))
                        .build())
                .build();
    }

    private WeatherApiResponse createMultiLocationApiResponse() {
        return WeatherApiResponse.builder()
                .latitude(34.0522)
                .longitude(-118.2437)
                .hourly(HourlyData.builder()
                        .time(Arrays.asList(1704067200L, 1704070800L, 1704074400L))
                        .temperature2m(Arrays.asList(65.0, 68.0, 72.0))
                        .relativeHumidity2m(Arrays.asList(45, 42, 40))
                        .rain(Arrays.asList(0.0, 0.0, 0.0))
                        .windSpeed10m(Arrays.asList(3.5, 4.0, 3.8))
                        .dewPoint2m(Arrays.asList(35.0, 36.0, 37.0))
                        .apparentTemperature(Arrays.asList(64.0, 67.0, 71.0))
                        .temperature80m(Arrays.asList(66.0, 69.0, 73.0))
                        .temperature120m(Arrays.asList(67.0, 70.0, 74.0))
                        .windSpeed80m(Arrays.asList(4.5, 5.0, 4.8))
                        .visibility(Arrays.asList(15000.0, 15000.0, 15000.0))
                        .soilTemperature0cm(Arrays.asList(60.0, 62.0, 65.0))
                        .soilTemperature6cm(Arrays.asList(58.0, 60.0, 63.0))
                        .showers(Arrays.asList(0.0, 0.0, 0.0))
                        .snowfall(Arrays.asList(0.0, 0.0, 0.0))
                        .build())
                .daily(DailyData.builder()
                        .time(List.of(1704067200L))
                        .sunrise(List.of(1704114000L))
                        .sunset(List.of(1704150000L))
                        .daylightDuration(List.of(36000))
                        .build())
                .build();
    }
}