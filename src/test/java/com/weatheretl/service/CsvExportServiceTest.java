package com.weatheretl.service;

import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.service.CsvExportService.CsvExportException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@DisplayName("CSV Export Service Tests")
class CsvExportServiceTest {

    @Mock
    private WeatherEtlConfig config;

    @Mock
    private WeatherEtlConfig.OutputConfig outputConfig;

    @InjectMocks
    private CsvExportService csvExportService;

    @TempDir
    Path tempDir;

    private List<WeatherRecord> mockWeatherRecords;
    private String testCsvPath;

    @BeforeEach
    void setUp() {
        testCsvPath = tempDir.resolve("test_weather.csv").toString();
        lenient().when(config.getOutput()).thenReturn(outputConfig);
        lenient().when(outputConfig.getCsvPath()).thenReturn(testCsvPath);
        mockWeatherRecords = createMockWeatherRecords();
    }

    @Test
    @DisplayName("Should export records to CSV successfully")
    void shouldExportRecordsToCsvSuccessfully() throws Exception {
        Path csvPath = tempDir.resolve("test_weather.csv");
        String csvFilePath = csvPath.toString();
        csvExportService.exportToCsv(mockWeatherRecords, csvFilePath);
        assertTrue(Files.exists(csvPath),
                "CSV file should exist at path: " + csvPath.toAbsolutePath());
        assertTrue(Files.size(csvPath) > 0,
                "CSV file should not be empty");
        List<String> lines = Files.readAllLines(csvPath);
        assertFalse(lines.isEmpty(), "CSV file should contain lines");
        String header = lines.get(0);
        assertNotNull(header, "Header should not be null");
        assertFalse(header.trim().isEmpty(), "Header should not be empty");
        String headerUpper = header.toUpperCase();
        assertTrue(headerUpper.contains("AVG_TEMPERATURE_2M_24H"),
                "Header should contain 'AVG_TEMPERATURE_2M_24H' column, but was: " + header);
        assertTrue(headerUpper.contains("AVG_RELATIVE_HUMIDITY_2M_24H"),
                "Header should contain 'AVG_RELATIVE_HUMIDITY_2M_24H' column, but was: " + header);
        assertTrue(headerUpper.contains("TEMPERATURE_2M_CELSIUS"),
                "Header should contain 'TEMPERATURE_2M_CELSIUS' column, but was: " + header);
        assertTrue(headerUpper.contains("RAIN_MM"),
                "Header should contain 'RAIN_MM' column, but was: " + header);
        assertTrue(headerUpper.contains("DAYLIGHT_HOURS"),
                "Header should contain 'DAYLIGHT_HOURS' column, but was: " + header);
        assertTrue(headerUpper.contains("SUNRISE_ISO"),
                "Header should contain 'SUNRISE_ISO' column, but was: " + header);
        assertTrue(headerUpper.contains("SUNSET_ISO"),
                "Header should contain 'SUNSET_ISO' column, but was: " + header);
        int expectedLines = mockWeatherRecords.size() + 1;
        assertEquals(expectedLines, lines.size(),
                "Should have " + expectedLines + " lines (header + " + mockWeatherRecords.size() + " records)");

        for (int i = 1; i < lines.size(); i++) {
            String dataLine = lines.get(i);
            assertFalse(dataLine.trim().isEmpty(),
                    "Data line " + i + " should not be empty");
            assertTrue(dataLine.contains(","),
                    "Data line " + i + " should contain CSV separators (commas)");
        }
        assertTrue(csvExportService.csvFileExists(csvFilePath),
                "Service should report that CSV file exists");
        long recordCount = csvExportService.getCsvFileSize(csvFilePath);
        assertEquals(mockWeatherRecords.size(), recordCount,
                "Service should report correct number of records");
    }

    @Test
    @DisplayName("Should export records to custom CSV path")
    void shouldExportRecordsToCustomCsvPath() throws Exception {
        String customPath = tempDir.resolve("custom_weather.csv").toString();
        csvExportService.exportToCsv(mockWeatherRecords, customPath);
        assertTrue(Files.exists(Path.of(customPath)));
        List<String> lines = Files.readAllLines(Path.of(customPath));
        assertEquals(mockWeatherRecords.size() + 1, lines.size());
    }

    @Test
    @DisplayName("Should create directories if they don't exist")
    void shouldCreateDirectoriesIfNotExist() throws Exception {
        String pathWithNewDirs = tempDir.resolve("new/sub/dir/weather.csv").toString();
        csvExportService.exportToCsv(mockWeatherRecords, pathWithNewDirs);
        assertTrue(Files.exists(Path.of(pathWithNewDirs)));
        assertTrue(Files.exists(Path.of(pathWithNewDirs).getParent()));
    }

    @Test
    @DisplayName("Should handle empty records list")
    void shouldHandleEmptyRecordsList() {
        assertDoesNotThrow(() -> csvExportService.exportToCsv(Collections.emptyList(), testCsvPath));
        assertFalse(Files.exists(Path.of(testCsvPath)));
    }

    @Test
    @DisplayName("Should handle null records list")
    void shouldHandleNullRecordsList(){
         assertDoesNotThrow(() -> csvExportService.exportToCsv(null, testCsvPath));
        assertFalse(Files.exists(Path.of(testCsvPath)));
    }

    @Test
    @DisplayName("Should throw exception for invalid file path")
    void shouldThrowExceptionForInvalidFilePath() {
        try {
            Path readOnlyFile = tempDir.resolve("readonly.csv");
            Files.createFile(readOnlyFile);
            readOnlyFile.toFile().setReadOnly();
            String invalidPath = readOnlyFile.toString();
            assertThrows(CsvExportException.class,
                    () -> csvExportService.exportToCsv(mockWeatherRecords, invalidPath));

        } catch (Exception e) {
            String invalidPath;
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                invalidPath = "Z:\\nonexistent\\deeply\\nested\\path\\weather.csv";
            } else {
                 invalidPath = "/proc/invalid/path/weather.csv";
            }
            assertThrows(CsvExportException.class,
                    () -> csvExportService.exportToCsv(mockWeatherRecords, invalidPath));
        }
    }

    @Test
    @DisplayName("Should check if path is writable")
    void shouldCheckIfPathIsWritable() {
        String writablePath = tempDir.resolve("writable.csv").toString();
        String nonWritablePath = "///invalid/path/non-writable.csv";
        assertTrue(csvExportService.isWritable(writablePath));
        assertFalse(csvExportService.isWritable(nonWritablePath));
    }

    @Test
    @DisplayName("Should get CSV file size correctly")
    void shouldGetCsvFileSizeCorrectly() throws Exception {
        csvExportService.exportToCsv(mockWeatherRecords, testCsvPath);
        long size = csvExportService.getCsvFileSize(testCsvPath);
        assertEquals(mockWeatherRecords.size(), size);
    }

    @Test
    @DisplayName("Should return 0 for non-existent file size")
    void shouldReturn0ForNonExistentFileSize() {
        String nonExistentPath = tempDir.resolve("non-existent.csv").toString();
        long size = csvExportService.getCsvFileSize(nonExistentPath);
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Should check if CSV file exists")
    void shouldCheckIfCsvFileExists() throws Exception {
        String existingPath = testCsvPath;
        String nonExistentPath = tempDir.resolve("non-existent.csv").toString();
        csvExportService.exportToCsv(mockWeatherRecords, existingPath);
        assertTrue(csvExportService.csvFileExists(existingPath));
        assertFalse(csvExportService.csvFileExists(nonExistentPath));
    }

    @Test
    @DisplayName("Should delete CSV file successfully")
    void shouldDeleteCsvFileSuccessfully() throws Exception {
        csvExportService.exportToCsv(mockWeatherRecords, testCsvPath);
        assertTrue(Files.exists(Path.of(testCsvPath)));
        boolean deleted = csvExportService.deleteCsvFile(testCsvPath);
        assertTrue(deleted);
        assertFalse(Files.exists(Path.of(testCsvPath)));
    }

    @Test
    @DisplayName("Should return false when deleting non-existent file")
    void shouldReturnFalseWhenDeletingNonExistentFile() {
        String nonExistentPath = tempDir.resolve("non-existent.csv").toString();
        boolean deleted = csvExportService.deleteCsvFile(nonExistentPath);
        assertFalse(deleted);
    }

    @Test
    @DisplayName("Should get CSV file info correctly")
    void shouldGetCsvFileInfoCorrectly() throws Exception {
        csvExportService.exportToCsv(mockWeatherRecords, testCsvPath);
        CsvExportService.CsvFileInfo info = csvExportService.getCsvFileInfo(testCsvPath);
        assertTrue(info.isExists());
        assertEquals(testCsvPath, info.getPath());
        assertNotNull(info.getSize());
        assertNotNull(info.getRecordCount());
        assertNotNull(info.getLastModified());
        assertEquals(mockWeatherRecords.size(), info.getRecordCount());
    }

    @Test
    @DisplayName("Should get CSV file info for non-existent file")
    void shouldGetCsvFileInfoForNonExistentFile() {
        String nonExistentPath = tempDir.resolve("non-existent.csv").toString();
        CsvExportService.CsvFileInfo info = csvExportService.getCsvFileInfo(nonExistentPath);
        assertFalse(info.isExists());
        assertEquals(nonExistentPath, info.getPath());
        assertNull(info.getSize());
        assertNull(info.getRecordCount());
        assertNull(info.getLastModified());
    }

    @Test
    @DisplayName("Should handle CSV with special characters")
    void shouldHandleCsvWithSpecialCharacters() throws Exception {
        List<WeatherRecord> specialRecords = Collections.singletonList(
                WeatherRecord.builder()
                        .date(LocalDate.of(2024, 1, 1))
                        .latitude(40.7128)
                        .longitude(-74.0060)
                        .sunriseIso("2024-01-01T07:20:00Z")
                        .sunsetIso("2024-01-01T17:30:00Z")
                        .build()
        );
        csvExportService.exportToCsv(specialRecords, testCsvPath);
        assertTrue(Files.exists(Path.of(testCsvPath)));
        List<String> lines = Files.readAllLines(Path.of(testCsvPath));
        String dataLine = lines.get(1);
        assertTrue(dataLine.contains("2024-01-01T07:20:00Z"));
        assertTrue(dataLine.contains("2024-01-01T17:30:00Z"));
    }

    @Test
    @DisplayName("Should handle large number of records")
    void shouldHandleLargeNumberOfRecords() throws Exception {
        List<WeatherRecord> largeRecordSet = createLargeRecordSet();
        csvExportService.exportToCsv(largeRecordSet, testCsvPath);
        assertTrue(Files.exists(Path.of(testCsvPath)));
        long recordCount = csvExportService.getCsvFileSize(testCsvPath);
        assertEquals(1000, recordCount);
    }

    @Test
    @DisplayName("Should validate CSV content structure")
    void shouldValidateCsvContentStructure() throws Exception {
        csvExportService.exportToCsv(mockWeatherRecords, testCsvPath);
        List<String> lines = Files.readAllLines(Path.of(testCsvPath));
        String header = lines.get(0);
        String[] headerColumns = header.split(",");
        assertTrue(headerColumns.length > 10);
        for (int i = 1; i < lines.size(); i++) {
            String[] dataColumns = lines.get(i).split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            assertEquals(headerColumns.length, dataColumns.length,
                    "Row " + i + " should have same number of columns as header");
        }
    }

    @Test
    @DisplayName("Should handle concurrent access safely")
    void shouldHandleConcurrentAccessSafely() throws Exception {
        String path1 = tempDir.resolve("concurrent1.csv").toString();
        String path2 = tempDir.resolve("concurrent2.csv").toString();
        Thread thread1 = new Thread(() -> {
            try {
                csvExportService.exportToCsv(mockWeatherRecords, path1);
            } catch (Exception e) {
                fail("Thread 1 export failed: " + e.getMessage());
            }
        });
        Thread thread2 = new Thread(() -> {
            try {
                csvExportService.exportToCsv(mockWeatherRecords, path2);
            } catch (Exception e) {
                fail("Thread 2 export failed: " + e.getMessage());
            }
        });

        thread1.start();
        thread2.start();
        thread1.join(5000);
        thread2.join(5000);

        assertTrue(Files.exists(Path.of(path1)));
        assertTrue(Files.exists(Path.of(path2)));
        assertEquals(mockWeatherRecords.size(), csvExportService.getCsvFileSize(path1));
        assertEquals(mockWeatherRecords.size(), csvExportService.getCsvFileSize(path2));
    }

    private List<WeatherRecord> createMockWeatherRecords() {
        return Arrays.asList(
                WeatherRecord.builder()
                        .date(LocalDate.of(2024, 1, 1))
                        .latitude(40.7128)
                        .longitude(-74.0060)
                        .avgTemperature2m24h(20.5)
                        .avgRelativeHumidity2m24h(65.0)
                        .totalRain24h(0.5)
                        .temperature2mCelsius(-6.39)
                        .rainMm(12.7)
                        .daylightHours(10.67)
                        .sunriseIso("2024-01-01T07:20:00Z")
                        .sunsetIso("2024-01-01T17:30:00Z")
                        .build(),
                WeatherRecord.builder()
                        .date(LocalDate.of(2024, 1, 2))
                        .latitude(40.7128)
                        .longitude(-74.0060)
                        .avgTemperature2m24h(21.0)
                        .avgRelativeHumidity2m24h(63.0)
                        .totalRain24h(0.3)
                        .temperature2mCelsius(-6.11)
                        .rainMm(7.62)
                        .daylightHours(10.68)
                        .sunriseIso("2024-01-02T07:20:00Z")
                        .sunsetIso("2024-01-02T17:31:00Z")
                        .build(),
                WeatherRecord.builder()
                        .date(LocalDate.of(2024, 1, 3))
                        .latitude(40.7128)
                        .longitude(-74.0060)
                        .avgTemperature2m24h(22.3)
                        .avgRelativeHumidity2m24h(68.0)
                        .totalRain24h(0.0)
                        .temperature2mCelsius(-5.39)
                        .rainMm(0.0)
                        .daylightHours(10.69)
                        .sunriseIso("2024-01-03T07:19:00Z")
                        .sunsetIso("2024-01-03T17:32:00Z")
                        .build()
        );
    }

    private List<WeatherRecord> createLargeRecordSet() {
        List<WeatherRecord> records = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            records.add(WeatherRecord.builder()
                    .date(LocalDate.of(2024, 1, 1).plusDays(i % 365))
                    .latitude(40.7128 + (i % 10) * 0.1)
                    .longitude(-74.0060 + (i % 10) * 0.1)
                    .avgTemperature2m24h(20.0 + (i % 30))
                    .avgRelativeHumidity2m24h(50.0 + (i % 50))
                    .build());
        }
        return records;
    }
}