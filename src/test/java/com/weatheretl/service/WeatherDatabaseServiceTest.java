package com.weatheretl.service;

import com.weatheretl.config.WeatherEtlConfig;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.repository.WeatherRepository;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Weather Database Service Tests")
class WeatherDatabaseServiceTest {

    @Mock
    private WeatherRepository weatherRepository;

    @Mock
    private WeatherEtlConfig config;

    @Mock
    private WeatherEtlConfig.OutputConfig outputConfig;

    @InjectMocks
    private WeatherDatabaseService weatherDatabaseService;

    private List<WeatherRecord> mockWeatherRecords;
    private WeatherRecord mockWeatherRecord;

    @BeforeEach
    void setUp() {
        lenient().when(config.getOutput()).thenReturn(outputConfig);
        lenient().when(outputConfig.getBatchSize()).thenReturn(100);
        mockWeatherRecord = createMockWeatherRecord();
        mockWeatherRecords = Arrays.asList(
                mockWeatherRecord,
                createMockWeatherRecord(LocalDate.of(2024, 1, 2)),
                createMockWeatherRecord(LocalDate.of(2024, 1, 3))
        );
    }

    @Test
    @DisplayName("Should save weather records successfully")
    void shouldSaveWeatherRecordsSuccessfully() {
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenReturn(Optional.empty());
        when(weatherRepository.save(any(WeatherRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        assertDoesNotThrow(() -> weatherDatabaseService.saveWeatherRecords(mockWeatherRecords));
        verify(weatherRepository, times(3)).findByDateAndLatitudeAndLongitude(any(), any(), any());
        verify(weatherRepository, times(3)).save(any(WeatherRecord.class));
    }

    @Test
    @DisplayName("Should handle empty records list")
    void shouldHandleEmptyRecordsList() {
        assertDoesNotThrow(() -> weatherDatabaseService.saveWeatherRecords(Collections.emptyList()));
        verifyNoInteractions(weatherRepository);
    }

    @Test
    @DisplayName("Should handle null records list")
    void shouldHandleNullRecordsList() {
        assertDoesNotThrow(() -> weatherDatabaseService.saveWeatherRecords(null));
        verifyNoInteractions(weatherRepository);
    }

    @Test
    @DisplayName("Should process records in batches")
    void shouldProcessRecordsInBatches() {
        when(outputConfig.getBatchSize()).thenReturn(2);
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenReturn(Optional.empty());
        when(weatherRepository.save(any(WeatherRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        weatherDatabaseService.saveWeatherRecords(mockWeatherRecords);
        verify(weatherRepository, times(3)).save(any(WeatherRecord.class));
    }

    @Test
    @DisplayName("Should upsert new weather record")
    void shouldUpsertNewWeatherRecord() {
        when(weatherRepository.findByDateAndLatitudeAndLongitude(
                mockWeatherRecord.getDate(),
                mockWeatherRecord.getLatitude(),
                mockWeatherRecord.getLongitude()))
                .thenReturn(Optional.empty());
        when(weatherRepository.save(mockWeatherRecord)).thenReturn(mockWeatherRecord);
        assertDoesNotThrow(() -> weatherDatabaseService.upsertWeatherRecord(mockWeatherRecord));
        verify(weatherRepository).findByDateAndLatitudeAndLongitude(
                mockWeatherRecord.getDate(),
                mockWeatherRecord.getLatitude(),
                mockWeatherRecord.getLongitude());
        verify(weatherRepository).save(mockWeatherRecord);
    }

    @Test
    @DisplayName("Should update existing weather record")
    void shouldUpdateExistingWeatherRecord() {
        WeatherRecord existingRecord = createMockWeatherRecord();
        existingRecord.setAvgTemperature2m24h(15.0);
        when(weatherRepository.findByDateAndLatitudeAndLongitude(
                mockWeatherRecord.getDate(),
                mockWeatherRecord.getLatitude(),
                mockWeatherRecord.getLongitude()))
                .thenReturn(Optional.of(existingRecord));
        when(weatherRepository.save(any(WeatherRecord.class))).thenReturn(existingRecord);
        weatherDatabaseService.upsertWeatherRecord(mockWeatherRecord);
        verify(weatherRepository).save(existingRecord);
        assertEquals(mockWeatherRecord.getAvgTemperature2m24h(), existingRecord.getAvgTemperature2m24h());
    }

    @Test
    @DisplayName("Should handle database exception during upsert")
    void shouldHandleDatabaseExceptionDuringUpsert() {
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenThrow(new RuntimeException("Database error"));
        DatabaseOperationException exception = assertThrows(
                DatabaseOperationException.class,
                () -> weatherDatabaseService.upsertWeatherRecord(mockWeatherRecord)
        );
        assertTrue(exception.getMessage().contains("Failed to upsert weather record"));
        assertNotNull(exception.getCause());
    }

    @Test
    @DisplayName("Should get weather records by date range")
    void shouldGetWeatherRecordsByDateRange() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 3);
        when(weatherRepository.findByDateBetween(startDate, endDate))
                .thenReturn(mockWeatherRecords);
        List<WeatherRecord> result = weatherDatabaseService.getWeatherRecords(startDate, endDate);
        assertEquals(3, result.size());
        verify(weatherRepository).findByDateBetween(startDate, endDate);
    }

    @Test
    @DisplayName("Should get weather records by date range and location")
    void shouldGetWeatherRecordsByDateRangeAndLocation() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 3);
        double latitude = 40.7128;
        double longitude = -74.0060;
        when(weatherRepository.findByDateBetweenAndLatitudeAndLongitude(
                startDate, endDate, latitude, longitude))
                .thenReturn(mockWeatherRecords);
        List<WeatherRecord> result = weatherDatabaseService.getWeatherRecords(
                startDate, endDate, latitude, longitude);
        assertEquals(3, result.size());
        verify(weatherRepository).findByDateBetweenAndLatitudeAndLongitude(
                startDate, endDate, latitude, longitude);
    }

    @Test
    @DisplayName("Should get single weather record")
    void shouldGetSingleWeatherRecord() {
        LocalDate date = LocalDate.of(2024, 1, 1);
        double latitude = 40.7128;
        double longitude = -74.0060;
        when(weatherRepository.findByDateAndLatitudeAndLongitude(date, latitude, longitude))
                .thenReturn(Optional.of(mockWeatherRecord));
        Optional<WeatherRecord> result = weatherDatabaseService.getWeatherRecord(date, latitude, longitude);
        assertTrue(result.isPresent());
        assertEquals(mockWeatherRecord, result.get());
    }

    @Test
    @DisplayName("Should check if record exists")
    void shouldCheckIfRecordExists() {
        LocalDate date = LocalDate.of(2024, 1, 1);
        double latitude = 40.7128;
        double longitude = -74.0060;
        when(weatherRepository.existsByDateAndLatitudeAndLongitude(date, latitude, longitude))
                .thenReturn(true);
        boolean exists = weatherDatabaseService.recordExists(date, latitude, longitude);
        assertTrue(exists);
        verify(weatherRepository).existsByDateAndLatitudeAndLongitude(date, latitude, longitude);
    }

    @Test
    @DisplayName("Should get record count by date range")
    void shouldGetRecordCountByDateRange() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 3);
        when(weatherRepository.countByDateBetween(startDate, endDate)).thenReturn(3L);
        long count = weatherDatabaseService.getRecordCount(startDate, endDate);
        assertEquals(3L, count);
        verify(weatherRepository).countByDateBetween(startDate, endDate);
    }

    @Test
    @DisplayName("Should get unique locations")
    void shouldGetUniqueLocations() {
        List<Object[]> locationObjects = Arrays.asList(
                new Object[]{40.7128, -74.0060},
                new Object[]{34.0522, -118.2437},
                new Object[]{41.8781, -87.6298}
        );
        when(weatherRepository.findDistinctLocations()).thenReturn(locationObjects);
        List<WeatherDatabaseService.LocationInfo> locations = weatherDatabaseService.getUniqueLocations();
        assertEquals(3, locations.size());
        assertEquals(40.7128, locations.get(0).latitude());
        assertEquals(-74.0060, locations.get(0).longitude());
        verify(weatherRepository).findDistinctLocations();
    }

    @Test
    @DisplayName("Should delete records by date range")
    void shouldDeleteRecordsByDateRange() {
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 3);
        when(weatherRepository.deleteByDateBetween(startDate, endDate)).thenReturn(3);
        int deletedCount = weatherDatabaseService.deleteRecords(startDate, endDate);
        assertEquals(3, deletedCount);
        verify(weatherRepository).deleteByDateBetween(startDate, endDate);
    }

    @Test
    @DisplayName("Should get database stats")
    void shouldGetDatabaseStats() {
        when(weatherRepository.count()).thenReturn(100L);
        List<Object[]> locationObjects = Arrays.asList(
                new Object[]{40.7128, -74.0060},
                new Object[]{34.0522, -118.2437}
        );
        when(weatherRepository.findDistinctLocations()).thenReturn(locationObjects);
        WeatherDatabaseService.DatabaseStats stats = weatherDatabaseService.getDatabaseStats();
        assertEquals(100L, stats.getTotalRecords());
        assertEquals(2, stats.getUniqueLocations());
        assertEquals(2, stats.getLocations().size());
        verify(weatherRepository).count();
        verify(weatherRepository).findDistinctLocations();
    }

    @Test
    @DisplayName("Should handle exception during save and continue with other records")
    void shouldHandleExceptionDuringSaveAndContinue() {
        WeatherRecord problematicRecord = mockWeatherRecords.get(1);
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenReturn(Optional.empty());
        when(weatherRepository.save(any(WeatherRecord.class)))
                .thenAnswer(invocation -> {
                    WeatherRecord record = invocation.getArgument(0);
                    if (record.equals(problematicRecord)) {
                        throw new RuntimeException("Database constraint violation");
                    }
                    return record;
                });
        assertDoesNotThrow(() -> weatherDatabaseService.saveWeatherRecords(mockWeatherRecords));
        verify(weatherRepository, times(3)).save(any(WeatherRecord.class));
    }

    @Test
    @DisplayName("Should validate record update field mapping")
    void shouldValidateRecordUpdateFieldMapping() {
        WeatherRecord existingRecord = createMockWeatherRecord();
        WeatherRecord updatedRecord = createMockWeatherRecord();
        updatedRecord.setAvgTemperature2m24h(25.0);
        updatedRecord.setAvgRelativeHumidity2m24h(70.0);
        updatedRecord.setTotalRain24h(1.5);
        updatedRecord.setTemperature2mCelsius(-3.89);
        updatedRecord.setRainMm(38.1);
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenReturn(Optional.of(existingRecord));
        when(weatherRepository.save(any(WeatherRecord.class))).thenReturn(existingRecord);
        weatherDatabaseService.upsertWeatherRecord(updatedRecord);
        assertEquals(updatedRecord.getAvgTemperature2m24h(), existingRecord.getAvgTemperature2m24h());
        assertEquals(updatedRecord.getAvgRelativeHumidity2m24h(), existingRecord.getAvgRelativeHumidity2m24h());
        assertEquals(updatedRecord.getTotalRain24h(), existingRecord.getTotalRain24h());
        assertEquals(updatedRecord.getTemperature2mCelsius(), existingRecord.getTemperature2mCelsius());
        assertEquals(updatedRecord.getRainMm(), existingRecord.getRainMm());
    }

    @Test
    @DisplayName("Should handle large batch processing")
    void shouldHandleLargeBatchProcessing() {
        when(outputConfig.getBatchSize()).thenReturn(10);
        List<WeatherRecord> largeRecordSet = createLargeRecordSet(25);
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenReturn(Optional.empty());
        when(weatherRepository.save(any(WeatherRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        weatherDatabaseService.saveWeatherRecords(largeRecordSet);
        verify(weatherRepository, times(25)).save(any(WeatherRecord.class));
    }

    @Test
    @DisplayName("Should maintain transactional integrity")
    void shouldMaintainTransactionalIntegrity() {
        when(weatherRepository.findByDateAndLatitudeAndLongitude(any(), any(), any()))
                .thenReturn(Optional.empty());
        when(weatherRepository.save(any(WeatherRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        weatherDatabaseService.saveWeatherRecords(mockWeatherRecords);
        verify(weatherRepository, times(3)).save(any(WeatherRecord.class));
    }

    private WeatherRecord createMockWeatherRecord() {
        return createMockWeatherRecord(LocalDate.of(2024, 1, 1));
    }

    private WeatherRecord createMockWeatherRecord(LocalDate date) {
        return WeatherRecord.builder()
                .date(date)
                .latitude(40.7128)
                .longitude(-74.0060)
                .avgTemperature2m24h(20.5)
                .avgRelativeHumidity2m24h(65.0)
                .avgDewPoint2m24h(15.2)
                .avgApparentTemperature24h(19.8)
                .avgTemperature80m24h(20.1)
                .avgTemperature120m24h(19.9)
                .avgWindSpeed10m24h(5.5)
                .avgWindSpeed80m24h(6.2)
                .avgVisibility24h(9800.0)
                .totalRain24h(0.1)
                .totalShowers24h(0.0)
                .totalSnowfall24h(0.0)
                .avgTemperature2mDaylight(21.0)
                .avgRelativeHumidity2mDaylight(63.0)
                .avgDewPoint2mDaylight(15.5)
                .avgApparentTemperatureDaylight(20.2)
                .avgTemperature80mDaylight(20.5)
                .avgTemperature120mDaylight(20.3)
                .avgWindSpeed10mDaylight(5.8)
                .avgWindSpeed80mDaylight(6.5)
                .avgVisibilityDaylight(10000.0)
                .totalRainDaylight(0.05)
                .totalShowersDaylight(0.0)
                .totalSnowfallDaylight(0.0)
                .windSpeed10mMPerS(2.83)
                .windSpeed80mMPerS(3.19)
                .temperature2mCelsius(-6.39)
                .apparentTemperatureCelsius(-6.78)
                .temperature80mCelsius(-6.61)
                .temperature120mCelsius(-6.72)
                .soilTemperature0cmCelsius(-7.50)
                .soilTemperature6cmCelsius(-8.11)
                .rainMm(2.54)
                .showersMm(0.0)
                .snowfallMm(0.0)
                .daylightHours(10.67)
                .sunriseIso("2024-01-01T07:20:00Z")
                .sunsetIso("2024-01-01T17:30:00Z")
                .build();
    }

    private List<WeatherRecord> createLargeRecordSet(int count) {
        List<WeatherRecord> records = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(createMockWeatherRecord(LocalDate.of(2024, 1, 1).plusDays(i)));
        }
        return records;
    }
}