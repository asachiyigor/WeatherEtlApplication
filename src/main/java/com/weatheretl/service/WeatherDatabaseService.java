package com.weatheretl.service;

import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.repository.WeatherRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherDatabaseService {
    private final WeatherRepository weatherRepository;
    private final WeatherEtlConfig config;

    @Transactional
    public void saveWeatherRecords(List<WeatherRecord> records) {
        if (records == null || records.isEmpty()) {
            log.warn("No records to save to database");
            return;
        }
        log.info("Saving {} weather records to database", records.size());
        int batchSize = config.getOutput().getBatchSize();
        int totalRecords = records.size();
        int processedRecords = 0;
        for (int i = 0; i < totalRecords; i += batchSize) {
            int endIndex = Math.min(i + batchSize, totalRecords);
            List<WeatherRecord> batch = records.subList(i, endIndex);

            for (WeatherRecord record : batch) {
                try {
                    upsertWeatherRecord(record);
                    processedRecords++;
                } catch (Exception e) {
                    log.error("Failed to save weather record for date: {}, location: {}, {}",
                            record.getDate(), record.getLatitude(), record.getLongitude(), e);
                }
            }
            log.debug("Processed batch {}/{} records", processedRecords, totalRecords);
        }
        log.info("Successfully saved {} weather records to database", processedRecords);
    }

    @Transactional
    public void upsertWeatherRecord(WeatherRecord record) {
        try {
            Optional<WeatherRecord> existingRecord = weatherRepository
                    .findByDateAndLatitudeAndLongitude(
                            record.getDate(),
                            record.getLatitude(),
                            record.getLongitude()
                    );

            if (existingRecord.isPresent()) {
                WeatherRecord existing = existingRecord.get();
                updateExistingRecord(existing, record);
                weatherRepository.save(existing);
                log.debug("Updated existing weather record for date: {}, location: {}, {}",
                        record.getDate(), record.getLatitude(), record.getLongitude());
            } else {
                weatherRepository.save(record);
                log.debug("Inserted new weather record for date: {}, location: {}, {}",
                        record.getDate(), record.getLatitude(), record.getLongitude());
            }
        } catch (Exception e) {
            log.error("Failed to upsert weather record", e);
            throw new DatabaseOperationException("Failed to upsert weather record", e);
        }
    }

    private void updateExistingRecord(WeatherRecord existing, WeatherRecord newRecord) {
        existing.setAvgTemperature2m24h(newRecord.getAvgTemperature2m24h());
        existing.setAvgRelativeHumidity2m24h(newRecord.getAvgRelativeHumidity2m24h());
        existing.setAvgDewPoint2m24h(newRecord.getAvgDewPoint2m24h());
        existing.setAvgApparentTemperature24h(newRecord.getAvgApparentTemperature24h());
        existing.setAvgTemperature80m24h(newRecord.getAvgTemperature80m24h());
        existing.setAvgTemperature120m24h(newRecord.getAvgTemperature120m24h());
        existing.setAvgWindSpeed10m24h(newRecord.getAvgWindSpeed10m24h());
        existing.setAvgWindSpeed80m24h(newRecord.getAvgWindSpeed80m24h());
        existing.setAvgVisibility24h(newRecord.getAvgVisibility24h());
        existing.setTotalRain24h(newRecord.getTotalRain24h());
        existing.setTotalShowers24h(newRecord.getTotalShowers24h());
        existing.setTotalSnowfall24h(newRecord.getTotalSnowfall24h());

        existing.setAvgTemperature2mDaylight(newRecord.getAvgTemperature2mDaylight());
        existing.setAvgRelativeHumidity2mDaylight(newRecord.getAvgRelativeHumidity2mDaylight());
        existing.setAvgDewPoint2mDaylight(newRecord.getAvgDewPoint2mDaylight());
        existing.setAvgApparentTemperatureDaylight(newRecord.getAvgApparentTemperatureDaylight());
        existing.setAvgTemperature80mDaylight(newRecord.getAvgTemperature80mDaylight());
        existing.setAvgTemperature120mDaylight(newRecord.getAvgTemperature120mDaylight());
        existing.setAvgWindSpeed10mDaylight(newRecord.getAvgWindSpeed10mDaylight());
        existing.setAvgWindSpeed80mDaylight(newRecord.getAvgWindSpeed80mDaylight());
        existing.setAvgVisibilityDaylight(newRecord.getAvgVisibilityDaylight());
        existing.setTotalRainDaylight(newRecord.getTotalRainDaylight());
        existing.setTotalShowersDaylight(newRecord.getTotalShowersDaylight());
        existing.setTotalSnowfallDaylight(newRecord.getTotalSnowfallDaylight());

        existing.setWindSpeed10mMPerS(newRecord.getWindSpeed10mMPerS());
        existing.setWindSpeed80mMPerS(newRecord.getWindSpeed80mMPerS());
        existing.setTemperature2mCelsius(newRecord.getTemperature2mCelsius());
        existing.setApparentTemperatureCelsius(newRecord.getApparentTemperatureCelsius());
        existing.setTemperature80mCelsius(newRecord.getTemperature80mCelsius());
        existing.setTemperature120mCelsius(newRecord.getTemperature120mCelsius());
        existing.setSoilTemperature0cmCelsius(newRecord.getSoilTemperature0cmCelsius());
        existing.setSoilTemperature6cmCelsius(newRecord.getSoilTemperature6cmCelsius());
        existing.setRainMm(newRecord.getRainMm());
        existing.setShowersMm(newRecord.getShowersMm());
        existing.setSnowfallMm(newRecord.getSnowfallMm());

        existing.setDaylightHours(newRecord.getDaylightHours());
        existing.setSunsetIso(newRecord.getSunsetIso());
        existing.setSunriseIso(newRecord.getSunriseIso());
    }

    public List<WeatherRecord> getWeatherRecords(LocalDate startDate, LocalDate endDate) {
        log.info("Fetching weather records from {} to {}", startDate, endDate);
        return weatherRepository.findByDateBetween(startDate, endDate);
    }

    public List<WeatherRecord> getWeatherRecords(LocalDate startDate, LocalDate endDate,
                                                 double latitude, double longitude) {
        log.info("Fetching weather records from {} to {} for location: {}, {}",
                startDate, endDate, latitude, longitude);
        return weatherRepository.findByDateBetweenAndLatitudeAndLongitude(
                startDate, endDate, latitude, longitude);
    }

    public Optional<WeatherRecord> getWeatherRecord(LocalDate date, double latitude, double longitude) {
        return weatherRepository.findByDateAndLatitudeAndLongitude(date, latitude, longitude);
    }

    public boolean recordExists(LocalDate date, double latitude, double longitude) {
        return weatherRepository.existsByDateAndLatitudeAndLongitude(date, latitude, longitude);
    }

    public long getRecordCount(LocalDate startDate, LocalDate endDate) {
        return weatherRepository.countByDateBetween(startDate, endDate);
    }

    public List<LocationInfo> getUniqueLocations() {
        return weatherRepository.findDistinctLocations()
                .stream()
                .map(objects -> new LocationInfo((Double) objects[0], (Double) objects[1]))
                .toList();
    }

    @Transactional
    public int deleteRecords(LocalDate startDate, LocalDate endDate) {
        log.info("Deleting weather records from {} to {}", startDate, endDate);
        int deletedCount = weatherRepository.deleteByDateBetween(startDate, endDate);
        log.info("Deleted {} weather records", deletedCount);
        return deletedCount;
    }

    public DatabaseStats getDatabaseStats() {
        long totalRecords = weatherRepository.count();
        List<LocationInfo> locations = getUniqueLocations();

        return DatabaseStats.builder()
                .totalRecords(totalRecords)
                .uniqueLocations(locations.size())
                .locations(locations)
                .build();
    }

    public record LocationInfo(Double latitude, Double longitude) {
    }

    @lombok.Data
    @lombok.Builder
    public static class DatabaseStats {
        private long totalRecords;
        private int uniqueLocations;
        private List<LocationInfo> locations;
    }

    public static class DatabaseOperationException extends RuntimeException {
        public DatabaseOperationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}