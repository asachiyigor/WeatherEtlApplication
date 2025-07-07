package com.weatheretl.service;

import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.service.CsvExportService.CsvExportException;
import com.weatheretl.service.WeatherApiClient.WeatherApiException;
import com.weatheretl.service.WeatherDatabaseService.DatabaseOperationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherEtlService {
    private final WeatherApiClient weatherApiClient;
    private final WeatherTransformer weatherTransformer;
    private final CsvExportService csvExportService;
    private final WeatherDatabaseService weatherDatabaseService;

    public EtlResult executeApiToCsv(LocalDate startDate, LocalDate endDate) {
        return executeApiToCsv(startDate, endDate, null);
    }

    public EtlResult executeApiToCsv(LocalDate startDate, LocalDate endDate, String csvPath) {
        log.info("Starting ETL process: API -> CSV for period {} to {}", startDate, endDate);

        EtlResult result = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .build();
        try {
            WeatherApiResponse apiResponse = weatherApiClient.fetchWeatherData(startDate, endDate);
            result.setApiResponseReceived(true);

            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());

            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from API response");
                return result;
            }
            if (csvPath != null) {
                csvExportService.exportToCsv(records, csvPath);
            } else {
                csvExportService.exportToCsv(records);
            }
            result.setCsvExported(true);
            result.setSuccess(true);
            log.info("ETL process completed successfully: {} records exported to CSV", records.size());
        } catch (WeatherApiException e) {
            result.setErrorMessage("API error: " + e.getMessage());
            log.error("ETL process failed at API stage", e);
        } catch (CsvExportException e) {
            result.setErrorMessage("CSV export error: " + e.getMessage());
            log.error("ETL process failed at CSV export stage", e);
        } catch (Exception e) {
            result.setErrorMessage("Unexpected error: " + e.getMessage());
            log.error("ETL process failed with unexpected error", e);
        }
        return result;
    }

    public EtlResult executeApiToDatabase(LocalDate startDate, LocalDate endDate) {
        log.info("Starting ETL process: API -> Database for period {} to {}", startDate, endDate);
        EtlResult result = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .build();
        try {
            WeatherApiResponse apiResponse = weatherApiClient.fetchWeatherData(startDate, endDate);
            result.setApiResponseReceived(true);
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());
            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from API response");
                return result;
            }
            weatherDatabaseService.saveWeatherRecords(records);
            result.setDatabaseSaved(true);
            result.setSuccess(true);
            log.info("ETL process completed successfully: {} records saved to database", records.size());
        } catch (WeatherApiException e) {
            result.setErrorMessage("API error: " + e.getMessage());
            log.error("ETL process failed at API stage", e);
        } catch (DatabaseOperationException e) {
            result.setErrorMessage("Database error: " + e.getMessage());
            log.error("ETL process failed at database stage", e);
        } catch (Exception e) {
            result.setErrorMessage("Unexpected error: " + e.getMessage());
            log.error("ETL process failed with unexpected error", e);
        }
        return result;
    }

    public EtlResult executeApiToCsvAndDatabase(LocalDate startDate, LocalDate endDate) {
        return executeApiToCsvAndDatabase(startDate, endDate, null);
    }

    public EtlResult executeApiToCsvAndDatabase(LocalDate startDate, LocalDate endDate, String csvPath) {
        log.info("Starting ETL process: API -> CSV + Database for period {} to {}", startDate, endDate);
        EtlResult result = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .build();
        try {
            WeatherApiResponse apiResponse = weatherApiClient.fetchWeatherData(startDate, endDate);
            result.setApiResponseReceived(true);
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());
            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from API response");
                return result;
            }
            try {
                if (csvPath != null) {
                    csvExportService.exportToCsv(records, csvPath);
                } else {
                    csvExportService.exportToCsv(records);
                }
                result.setCsvExported(true);
            } catch (CsvExportException e) {
                log.error("CSV export failed, but continuing with database save", e);
                result.setErrorMessage("CSV export failed: " + e.getMessage());
            }
            try {
                weatherDatabaseService.saveWeatherRecords(records);
                result.setDatabaseSaved(true);
            } catch (DatabaseOperationException e) {
                log.error("Database save failed", e);
                if (result.getErrorMessage() == null) {
                    result.setErrorMessage("Database save failed: " + e.getMessage());
                } else {
                    result.setErrorMessage(result.getErrorMessage() + "; Database save failed: " + e.getMessage());
                }
            }
            result.setSuccess(result.isCsvExported() || result.isDatabaseSaved());
            if (result.isSuccess()) {
                log.info("ETL process completed: {} records processed (CSV: {}, DB: {})",
                        records.size(), result.isCsvExported(), result.isDatabaseSaved());
            }
        } catch (WeatherApiException e) {
            result.setErrorMessage("API error: " + e.getMessage());
            log.error("ETL process failed at API stage", e);
        } catch (Exception e) {
            result.setErrorMessage("Unexpected error: " + e.getMessage());
            log.error("ETL process failed with unexpected error", e);
        }
        return result;
    }

    public EtlResult processJsonData(WeatherApiResponse apiResponse, boolean saveToCsv,
                                     boolean saveToDatabase, String csvPath) {
        log.info("Processing JSON data: CSV={}, DB={}", saveToCsv, saveToDatabase);
        EtlResult result = EtlResult.builder()
                .apiResponseReceived(true)
                .success(false)
                .build();
        try {
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());
            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from JSON data");
                return result;
            }
            if (saveToCsv) {
                try {
                    if (csvPath != null) {
                        csvExportService.exportToCsv(records, csvPath);
                    } else {
                        csvExportService.exportToCsv(records);
                    }
                    result.setCsvExported(true);
                } catch (CsvExportException e) {
                    log.error("CSV export failed", e);
                    result.setErrorMessage("CSV export failed: " + e.getMessage());
                }
            }
            if (saveToDatabase) {
                try {
                    weatherDatabaseService.saveWeatherRecords(records);
                    result.setDatabaseSaved(true);
                } catch (DatabaseOperationException e) {
                    log.error("Database save failed", e);
                    if (result.getErrorMessage() == null) {
                        result.setErrorMessage("Database save failed: " + e.getMessage());
                    } else {
                        result.setErrorMessage(result.getErrorMessage() + "; Database save failed: " + e.getMessage());
                    }
                }
            }
            boolean csvSuccess = !saveToCsv || result.isCsvExported();
            boolean dbSuccess = !saveToDatabase || result.isDatabaseSaved();
            result.setSuccess(csvSuccess && dbSuccess);
            if (result.isSuccess()) {
                log.info("JSON processing completed successfully: {} records processed", records.size());
            }
        } catch (Exception e) {
            result.setErrorMessage("Unexpected error: " + e.getMessage());
            log.error("JSON processing failed with unexpected error", e);
        }
        return result;
    }

    public EtlStats getEtlStats() {
        WeatherDatabaseService.DatabaseStats dbStats = weatherDatabaseService.getDatabaseStats();

        return EtlStats.builder()
                .totalRecordsInDatabase(dbStats.getTotalRecords())
                .uniqueLocations(dbStats.getUniqueLocations())
                .locations(dbStats.getLocations())
                .build();
    }

    @lombok.Data
    @lombok.Builder
    public static class EtlResult {
        private LocalDate startDate;
        private LocalDate endDate;
        private boolean success;
        private String errorMessage;

        private boolean apiResponseReceived;
        private int recordsTransformed;
        private boolean csvExported;
        private boolean databaseSaved;
    }

    @lombok.Data
    @lombok.Builder
    public static class EtlStats {
        private long totalRecordsInDatabase;
        private int uniqueLocations;
        private List<WeatherDatabaseService.LocationInfo> locations;
    }
}