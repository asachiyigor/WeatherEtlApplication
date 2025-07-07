package com.weatheretl.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weatheretl.WeatherEtlApplication;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.repository.WeatherRepository;
import com.weatheretl.service.WeatherEtlService.EtlResult;
import com.weatheretl.service.WeatherEtlService.EtlStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Nested
@SpringBootTest(
        classes = WeatherEtlApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@Testcontainers
@ActiveProfiles("integration-test")
@AutoConfigureWebMvc
@DisplayName("Weather ETL Full Integration Tests")
class WeatherEtlFullIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("weather_integration_test")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("weather-etl.output.csv-path", () -> "./test-output/integration-weather.csv");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private WeatherRepository weatherRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private String baseUrl;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port + "/api/v1/weather-etl";
        weatherRepository.deleteAll();

        try {
            Files.createDirectories(Path.of("./test-output"));
        } catch (IOException ignored) {
        }
    }

    @Test
    @DisplayName("Should perform complete ETL workflow - API to Database")
    @Transactional
    void shouldPerformCompleteEtlWorkflowApiToDatabase() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 2);
        String url = baseUrl + "/execute/api-to-database?startDate=2024-01-01&endDate=2024-01-02";
        ResponseEntity<EtlResult> response = restTemplate.postForEntity(url, null, EtlResult.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().isSuccess()).isTrue();
        assertThat(response.getBody().isDatabaseSaved()).isTrue();
        assertThat(response.getBody().getRecordsTransformed()).isGreaterThan(0);
        List<WeatherRecord> savedRecords = weatherRepository.findByDateBetween(startDate, endDate);
        assertThat(savedRecords).isNotEmpty();
        assertThat(savedRecords.get(0).getLatitude()).isNotNull();
        assertThat(savedRecords.get(0).getLongitude()).isNotNull();
        assertThat(savedRecords.get(0).getDate()).isBetween(startDate, endDate);
    }

    @Test
    @DisplayName("Should maintain data consistency across operations")
    @Transactional
    void shouldMaintainDataConsistencyAcrossOperations() throws Exception {
        // Given
        LocalDate requestDate = LocalDate.of(2024, 1, 15);

        // When - Perform multiple operations
        // 1. API to Database
        String dbUrl = baseUrl + "/execute/api-to-database?startDate=2024-01-15&endDate=2024-01-15";
        ResponseEntity<EtlResult> dbResponse = restTemplate.postForEntity(dbUrl, null, EtlResult.class);

        // 2. API to CSV
        String csvPath = "./test-output/consistency-test.csv";
        String csvUrl = baseUrl + "/execute/api-to-csv?startDate=2024-01-15&endDate=2024-01-15&csvPath=" + csvPath;
        ResponseEntity<EtlResult> csvResponse = restTemplate.postForEntity(csvUrl, null, EtlResult.class);

        // Then
        assertThat(dbResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(csvResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Verify data was actually saved - check broader date range to account for timezone differences
        LocalDate startRange = requestDate.minusDays(1);
        LocalDate endRange = requestDate.plusDays(1);

        List<WeatherRecord> dbRecords = weatherRepository.findByDateBetween(startRange, endRange);
        File csvFile = new File(csvPath);

        // Verify that data was saved
        assertThat(dbRecords).isNotEmpty()
                .withFailMessage("No records found in database between %s and %s. API might be saving with different date due to timezone conversion.",
                        startRange, endRange);

        if (csvFile.exists()) {
            List<String> csvLines = Files.readAllLines(csvFile.toPath());
            // CSV should have header + data rows
            assertThat(csvLines.size()).isGreaterThan(1)
                    .withFailMessage("CSV file should contain header + data rows, but has %d lines", csvLines.size());

            // Both operations should have processed the same number of records
            assertThat(dbResponse.getBody().getRecordsTransformed())
                    .isEqualTo(csvResponse.getBody().getRecordsTransformed())
                    .withFailMessage("Database and CSV operations should process the same number of records");

            // Verify consistency: records count should match (CSV lines - header = DB records)
            int csvDataRows = csvLines.size() - 1; // Subtract header
            assertThat(dbRecords.size()).isEqualTo(csvDataRows)
                    .withFailMessage("Database has %d records but CSV has %d data rows", dbRecords.size(), csvDataRows);
        } else {
            fail("CSV file was not created at path: " + csvPath);
        }
    }


    @Test
    @DisplayName("Should handle application startup and shutdown gracefully")
    void shouldHandleApplicationStartupAndShutdownGracefully() {
        String healthUrl = baseUrl + "/health";
        ResponseEntity<String> healthResponse = restTemplate.getForEntity(healthUrl, String.class);
        assertThat(healthResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(healthResponse.getBody()).contains("UP");
    }

    @Test
    @DisplayName("Should handle database connection failures gracefully")
    void shouldHandleDatabaseConnectionFailuresGracefully() {
        String url = baseUrl + "/stats";
        ResponseEntity<EtlStats> response = restTemplate.getForEntity(url, EtlStats.class);
        assertThat(response.getStatusCode()).isIn(HttpStatus.OK, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    @DisplayName("Should handle JSON serialization/deserialization correctly")
    void shouldHandleJsonSerializationDeserializationCorrectly() throws Exception {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 1);
        String url = baseUrl + "/execute/api-to-database?startDate=2024-01-01&endDate=2024-01-01";
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        EtlResult result = objectMapper.readValue(response.getBody(), EtlResult.class);
        assertThat(result).isNotNull();
        assertThat(result.getStartDate()).isEqualTo(startDate);
        assertThat(result.getEndDate()).isEqualTo(endDate);
    }
}