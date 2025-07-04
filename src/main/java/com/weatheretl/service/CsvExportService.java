package com.weatheretl.service;

import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.output.WeatherRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CsvExportService {

    private final WeatherEtlConfig config;

    public void exportToCsv(List<WeatherRecord> records) throws CsvExportException {
        exportToCsv(records, config.getOutput().getCsvPath());
    }

    public void exportToCsv(List<WeatherRecord> records, String filePath) throws CsvExportException {
        if (records == null || records.isEmpty()) {
            log.warn("No records to export to CSV");
            return;
        }
        log.info("Exporting {} records to CSV file: {}", records.size(), filePath);
        try {
            Path path = Paths.get(filePath);
            Path parentDir = path.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
                log.info("Created directory: {}", parentDir);
            }
            try (FileWriter writer = new FileWriter(filePath)) {
                StatefulBeanToCsv<WeatherRecord> beanToCsv = new StatefulBeanToCsvBuilder<WeatherRecord>(writer)
                        .withSeparator(',')
                        .withQuotechar('"')
                        .withEscapechar('\\')
                        .build();
                beanToCsv.write(records);
                log.info("Successfully exported {} records to CSV file: {}", records.size(), filePath);
            }

        } catch (IOException e) {
            log.error("Failed to write CSV file: {}", filePath, e);
            throw new CsvExportException("Failed to write CSV file: " + e.getMessage(), e);
        } catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
            log.error("CSV data formatting error", e);
            throw new CsvExportException("CSV data formatting error: " + e.getMessage(), e);
        }
    }

    public boolean isWritable(String filePath) {
        try {
            Path path = Paths.get(filePath);
            Path parentDir = path.getParent();

            if (parentDir != null && !Files.exists(parentDir)) {
                return Files.isWritable(parentDir.getParent());
            }

            return Files.isWritable(parentDir != null ? parentDir : path);

        } catch (Exception e) {
            log.warn("Cannot check if path is writable: {}", filePath, e);
            return false;
        }
    }

    public long getCsvFileSize(String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (Files.exists(path)) {
                return Files.lines(path).count() - 1;
            }
            return 0;
        } catch (IOException e) {
            log.warn("Cannot read CSV file size: {}", filePath, e);
            return 0;
        }
    }

    public boolean csvFileExists(String filePath) {
        return Files.exists(Paths.get(filePath));
    }

    public boolean deleteCsvFile(String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (Files.exists(path)) {
                Files.delete(path);
                log.info("Deleted CSV file: {}", filePath);
                return true;
            }
            return false;
        } catch (IOException e) {
            log.error("Failed to delete CSV file: {}", filePath, e);
            return false;
        }
    }

    public CsvFileInfo getCsvFileInfo(String filePath) {
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            return CsvFileInfo.builder()
                    .exists(false)
                    .path(filePath)
                    .build();
        }

        try {
            return CsvFileInfo.builder()
                    .exists(true)
                    .path(filePath)
                    .size(Files.size(path))
                    .recordCount(getCsvFileSize(filePath))
                    .lastModified(Files.getLastModifiedTime(path).toInstant())
                    .build();
        } catch (IOException e) {
            log.error("Failed to get CSV file info: {}", filePath, e);
            return CsvFileInfo.builder()
                    .exists(true)
                    .path(filePath)
                    .build();
        }
    }

    @lombok.Data
    @lombok.Builder
    public static class CsvFileInfo {
        private boolean exists;
        private String path;
        private Long size;
        private Long recordCount;
        private java.time.Instant lastModified;
    }

    public static class CsvExportException extends Exception {
        public CsvExportException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}