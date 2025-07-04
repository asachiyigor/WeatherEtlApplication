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

/**
 * Главный сервис ETL процесса для погодных данных
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherEtlService {

    private final WeatherApiClient weatherApiClient;
    private final WeatherTransformer weatherTransformer;
    private final CsvExportService csvExportService;
    private final WeatherDatabaseService weatherDatabaseService;

    /**
     * Выполнение полного ETL процесса: API -> CSV
     */
    public EtlResult executeApiToCsv(LocalDate startDate, LocalDate endDate) {
        return executeApiToCsv(startDate, endDate, null);
    }

    /**
     * Выполнение полного ETL процесса: API -> CSV с указанием пути
     */
    public EtlResult executeApiToCsv(LocalDate startDate, LocalDate endDate, String csvPath) {
        log.info("Starting ETL process: API -> CSV for period {} to {}", startDate, endDate);

        EtlResult result = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .build();

        try {
            // Extract: получение данных от API
            WeatherApiResponse apiResponse = weatherApiClient.fetchWeatherData(startDate, endDate);
            result.setApiResponseReceived(true);

            // Transform: преобразование данных
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());

            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from API response");
                return result;
            }

            // Load: экспорт в CSV
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

    /**
     * Выполнение полного ETL процесса: API -> Database
     */
    public EtlResult executeApiToDatabase(LocalDate startDate, LocalDate endDate) {
        log.info("Starting ETL process: API -> Database for period {} to {}", startDate, endDate);

        EtlResult result = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .build();

        try {
            // Extract: получение данных от API
            WeatherApiResponse apiResponse = weatherApiClient.fetchWeatherData(startDate, endDate);
            result.setApiResponseReceived(true);

            // Transform: преобразование данных
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());

            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from API response");
                return result;
            }

            // Load: сохранение в базу данных
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

    /**
     * Выполнение полного ETL процесса: API -> CSV + Database
     */
    public EtlResult executeApiToCsvAndDatabase(LocalDate startDate, LocalDate endDate) {
        return executeApiToCsvAndDatabase(startDate, endDate, null);
    }

    /**
     * Выполнение полного ETL процесса: API -> CSV + Database с указанием пути CSV
     */
    public EtlResult executeApiToCsvAndDatabase(LocalDate startDate, LocalDate endDate, String csvPath) {
        log.info("Starting ETL process: API -> CSV + Database for period {} to {}", startDate, endDate);

        EtlResult result = EtlResult.builder()
                .startDate(startDate)
                .endDate(endDate)
                .success(false)
                .build();

        try {
            // Extract: получение данных от API
            WeatherApiResponse apiResponse = weatherApiClient.fetchWeatherData(startDate, endDate);
            result.setApiResponseReceived(true);

            // Transform: преобразование данных
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());

            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from API response");
                return result;
            }

            // Load: экспорт в CSV
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

            // Load: сохранение в базу данных
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

            // Считаем успешным если хотя бы одна операция сохранения удалась
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

    /**
     * Обработка данных из готового JSON (для тестирования или работы с файлами)
     */
    public EtlResult processJsonData(WeatherApiResponse apiResponse, boolean saveToCsv,
                                     boolean saveToDatabase, String csvPath) {
        log.info("Processing JSON data: CSV={}, DB={}", saveToCsv, saveToDatabase);

        EtlResult result = EtlResult.builder()
                .apiResponseReceived(true)
                .success(false)
                .build();

        try {
            // Transform: преобразование данных
            List<WeatherRecord> records = weatherTransformer.transformWeatherData(apiResponse);
            result.setRecordsTransformed(records.size());

            if (records.isEmpty()) {
                result.setErrorMessage("No records were transformed from JSON data");
                return result;
            }

            // Load: экспорт в CSV
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

            // Load: сохранение в базу данных
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

            // Считаем успешным если выполнились все запрошенные операции
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

    /**
     * Получение статистики по обработанным данным
     */
    public EtlStats getEtlStats() {
        WeatherDatabaseService.DatabaseStats dbStats = weatherDatabaseService.getDatabaseStats();

        return EtlStats.builder()
                .totalRecordsInDatabase(dbStats.getTotalRecords())
                .uniqueLocations(dbStats.getUniqueLocations())
                .locations(dbStats.getLocations())
                .build();
    }

    /**
     * Результат выполнения ETL процесса
     */
    @lombok.Data
    @lombok.Builder
    public static class EtlResult {
        private LocalDate startDate;
        private LocalDate endDate;
        private boolean success;
        private String errorMessage;

        // Статус выполнения этапов
        private boolean apiResponseReceived;
        private int recordsTransformed;
        private boolean csvExported;
        private boolean databaseSaved;
    }

    /**
     * Статистика ETL процесса
     */
    @lombok.Data
    @lombok.Builder
    public static class EtlStats {
        private long totalRecordsInDatabase;
        private int uniqueLocations;
        private List<WeatherDatabaseService.LocationInfo> locations;
    }
}