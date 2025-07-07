package com.weatheretl.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.service.WeatherEtlService;
import com.weatheretl.service.WeatherEtlService.EtlResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Weather ETL CLI Tests")
class WeatherEtlCliTest {

    @Mock
    private WeatherEtlService weatherEtlService;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private WeatherEtlCli weatherEtlCli;

    @TempDir
    Path tempDir;

    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    private EtlResult successResult;
    private EtlResult failureResult;

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
        weatherEtlCli.setTestMode(true);
        successResult = EtlResult.builder()
                .startDate(LocalDate.of(2024, 1, 1))
                .endDate(LocalDate.of(2024, 1, 3))
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(10)
                .csvExported(true)
                .databaseSaved(false)
                .build();
        failureResult = EtlResult.builder()
                .startDate(LocalDate.of(2024, 1, 1))
                .endDate(LocalDate.of(2024, 1, 3))
                .success(false)
                .errorMessage("API connection failed")
                .apiResponseReceived(false)
                .recordsTransformed(0)
                .csvExported(false)
                .databaseSaved(false)
                .build();
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    @DisplayName("Should start as web service when no arguments provided")
    void shouldStartAsWebServiceWhenNoArgumentsProvided() throws Exception {
        weatherEtlCli.run();
        String output = outputStream.toString();
        assertThat(output).contains("Starting Weather ETL application as web service");
        assertThat(output).contains("WEATHER ETL PIPELINE");
        assertThat(output).contains("Web interface available at: http://localhost:8080");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should display help when --help flag provided")
    void shouldDisplayHelpWhenHelpFlagProvided() throws Exception {
        weatherEtlCli.run("--help");
        String output = outputStream.toString();
        assertThat(output).contains("WEATHER ETL PIPELINE - CLI USAGE");
        assertThat(output).contains("SYNOPSIS:");
        assertThat(output).contains("--source=<api|json>");
        assertThat(output).contains("--output=<csv|database|all>");
        assertThat(output).contains("EXAMPLES:");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should display help when -h flag provided")
    void shouldDisplayHelpWhenShortHelpFlagProvided() throws Exception {
        weatherEtlCli.run("-h");
        String output = outputStream.toString();
        assertThat(output).contains("WEATHER ETL PIPELINE - CLI USAGE");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should execute API to CSV successfully")
    void shouldExecuteApiToCsvSuccessfully() {
        when(weatherEtlService.executeApiToCsv(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                null
        )).thenReturn(successResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        verify(weatherEtlService).executeApiToCsv(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                null
        );
        String output = outputStream.toString();
        assertThat(output).contains("✅ SUCCESS");
        assertThat(output).contains("Records processed: 10");
        assertThat(output).contains("CSV exported: ✅");
        assertThat(output).contains("Database saved: ❌");
    }

    @Test
    @DisplayName("Should execute API to CSV with custom path")
    void shouldExecuteApiToCsvWithCustomPath() {
        String customPath = "/custom/path/weather.csv";
        when(weatherEtlService.executeApiToCsv(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                customPath
        )).thenReturn(successResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03",
                "--csv-path=" + customPath
        ));
        verify(weatherEtlService).executeApiToCsv(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                customPath
        );
    }

    @Test
    @DisplayName("Should execute API to Database successfully")
    void shouldExecuteApiToDatabaseSuccessfully() {
        EtlResult dbResult = EtlResult.builder()
                .startDate(LocalDate.of(2024, 1, 1))
                .endDate(LocalDate.of(2024, 1, 3))
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(15)
                .csvExported(false)
                .databaseSaved(true)
                .build();

        when(weatherEtlService.executeApiToDatabase(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3)
        )).thenReturn(dbResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=api",
                "--output=database",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        verify(weatherEtlService).executeApiToDatabase(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3)
        );
        String output = outputStream.toString();
        assertThat(output).contains("✅ SUCCESS");
        assertThat(output).contains("Records processed: 15");
        assertThat(output).contains("CSV exported: ❌");
        assertThat(output).contains("Database saved: ✅");
    }

    @Test
    @DisplayName("Should execute API to All (CSV + Database) successfully")
    void shouldExecuteApiToAllSuccessfully() {
        EtlResult allResult = EtlResult.builder()
                .startDate(LocalDate.of(2024, 1, 1))
                .endDate(LocalDate.of(2024, 1, 3))
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(20)
                .csvExported(true)
                .databaseSaved(true)
                .build();

        when(weatherEtlService.executeApiToCsvAndDatabase(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                null
        )).thenReturn(allResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=api",
                "--output=all",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        verify(weatherEtlService).executeApiToCsvAndDatabase(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                null
        );
        String output = outputStream.toString();
        assertThat(output).contains("✅ SUCCESS");
        assertThat(output).contains("Records processed: 20");
        assertThat(output).contains("CSV exported: ✅");
        assertThat(output).contains("Database saved: ✅");
    }

    @Test
    @DisplayName("Should process JSON file to CSV successfully")
    void shouldProcessJsonFileToCsvSuccessfully() throws Exception {
        Path jsonFile = tempDir.resolve("weather-data.json");
        Files.writeString(jsonFile, "{\"latitude\": 40.7128, \"longitude\": -74.0060}");
        WeatherApiResponse mockResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .build();
        when(objectMapper.readValue(any(File.class), eq(WeatherApiResponse.class)))
                .thenReturn(mockResponse);
        when(weatherEtlService.processJsonData(mockResponse, true, false, null))
                .thenReturn(successResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=json",
                "--output=csv",
                "--json-path=" + jsonFile
        ));
        verify(objectMapper).readValue(any(File.class), eq(WeatherApiResponse.class));
        verify(weatherEtlService).processJsonData(mockResponse, true, false, null);
        String output = outputStream.toString();
        assertThat(output).contains("✅ SUCCESS");
    }

    @Test
    @DisplayName("Should process JSON file to Database successfully")
    void shouldProcessJsonFileToDatabaseSuccessfully() throws Exception {
        Path jsonFile = tempDir.resolve("weather-data.json");
        Files.writeString(jsonFile, "{\"latitude\": 40.7128, \"longitude\": -74.0060}");
        WeatherApiResponse mockResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .build();
        when(objectMapper.readValue(any(File.class), eq(WeatherApiResponse.class)))
                .thenReturn(mockResponse);
        when(weatherEtlService.processJsonData(mockResponse, false, true, null))
                .thenReturn(successResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=json",
                "--output=database",
                "--json-path=" + jsonFile
        ));
        verify(weatherEtlService).processJsonData(mockResponse, false, true, null);
    }

    @Test
    @DisplayName("Should process JSON file to All (CSV + Database) successfully")
    void shouldProcessJsonFileToAllSuccessfully() throws Exception {
        Path jsonFile = tempDir.resolve("weather-data.json");
        Files.writeString(jsonFile, "{\"latitude\": 40.7128, \"longitude\": -74.0060}");
        WeatherApiResponse mockResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .build();
        when(objectMapper.readValue(any(File.class), eq(WeatherApiResponse.class)))
                .thenReturn(mockResponse);
        when(weatherEtlService.processJsonData(mockResponse, true, true, null))
                .thenReturn(successResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=json",
                "--output=all",
                "--json-path=" + jsonFile
        ));
        verify(weatherEtlService).processJsonData(mockResponse, true, true, null);
    }

    @Test
    @DisplayName("Should handle missing source parameter")
    void shouldHandleMissingSourceParameter() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--output=csv",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        assertThat(exception.getMessage()).contains("Source parameter is required");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle missing output parameter")
    void shouldHandleMissingOutputParameter() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=api",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        assertThat(exception.getMessage()).contains("Output parameter is required");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle missing start date for API source")
    void shouldHandleMissingStartDateForApiSource() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--end-date=2024-01-03"
        ));
        assertThat(exception.getMessage()).contains("Start date and end date are required for API source");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle missing end date for API source")
    void shouldHandleMissingEndDateForApiSource() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=2024-01-01"
        ));
        assertThat(exception.getMessage()).contains("Start date and end date are required for API source");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle invalid date format")
    void shouldHandleInvalidDateFormat() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=invalid-date",
                "--end-date=2024-01-03"
        ));
        assertThat(exception.getMessage()).contains("Invalid date format");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle start date after end date")
    void shouldHandleStartDateAfterEndDate() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=2024-01-10",
                "--end-date=2024-01-05"
        ));
        assertThat(exception.getMessage()).contains("Start date must be before or equal to end date");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle invalid source parameter")
    void shouldHandleInvalidSourceParameter() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=invalid",
                "--output=csv",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        assertThat(exception.getMessage()).contains("Invalid source: invalid");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle invalid output parameter")
    void shouldHandleInvalidOutputParameter() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=api",
                "--output=invalid",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        assertThat(exception.getMessage()).contains("Invalid output: invalid");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle missing JSON path for JSON source")
    void shouldHandleMissingJsonPathForJsonSource() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=json",
                "--output=csv"
        ));
        assertThat(exception.getMessage()).contains("JSON path is required for JSON source");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle non-existent JSON file")
    void shouldHandleNonExistentJsonFile() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=json",
                "--output=csv",
                "--json-path=/non/existent/file.json"
        ));
        assertThat(exception.getMessage()).contains("JSON file not found");
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle ETL service failure")
    void shouldHandleEtlServiceFailure() {
        when(weatherEtlService.executeApiToCsv(any(), any(), any()))
                .thenReturn(failureResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        String output = outputStream.toString();
        assertThat(output).contains("❌ FAILED");
        assertThat(output).contains("Error: API connection failed");
    }

    @Test
    @DisplayName("Should handle JSON parsing exception")
    void shouldHandleJsonParsingException() throws Exception {
        Path jsonFile = tempDir.resolve("invalid.json");
        Files.writeString(jsonFile, "invalid json content");
        when(objectMapper.readValue(any(File.class), eq(WeatherApiResponse.class)))
                .thenThrow(new IOException("Invalid JSON format"));
        RuntimeException exception = assertThrows(RuntimeException.class, () -> weatherEtlCli.run(
                "--source=json",
                "--output=csv",
                "--json-path=" + jsonFile
        ));
        assertThat(exception.getCause()).isInstanceOf(IOException.class);
        verifyNoInteractions(weatherEtlService);
    }

    @Test
    @DisplayName("Should handle same start and end date")
    void shouldHandleSameStartAndEndDate() {
        EtlResult sameDateResult = EtlResult.builder()
                .startDate(LocalDate.of(2024, 6, 15))
                .endDate(LocalDate.of(2024, 6, 15))
                .success(true)
                .apiResponseReceived(true)
                .recordsTransformed(1)
                .csvExported(true)
                .databaseSaved(false)
                .build();
        when(weatherEtlService.executeApiToCsv(
                LocalDate.of(2024, 6, 15),
                LocalDate.of(2024, 6, 15),
                null
        )).thenReturn(sameDateResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=api",
                "--output=csv",
                "--start-date=2024-06-15",
                "--end-date=2024-06-15"
        ));
        verify(weatherEtlService).executeApiToCsv(
                LocalDate.of(2024, 6, 15),
                LocalDate.of(2024, 6, 15),
                null
        );
        String output = outputStream.toString();
        assertThat(output).contains("✅ SUCCESS");
        assertThat(output).contains("Period: 2024-06-15 to 2024-06-15");
    }

    @Test
    @DisplayName("Should handle case insensitive parameters")
    void shouldHandleCaseInsensitiveParameters() {
        when(weatherEtlService.executeApiToCsv(any(), any(), any()))
                .thenReturn(successResult);
        assertDoesNotThrow(() -> weatherEtlCli.run(
                "--source=API",
                "--output=CSV",
                "--start-date=2024-01-01",
                "--end-date=2024-01-03"
        ));
        verify(weatherEtlService).executeApiToCsv(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 3),
                null
        );
    }
}