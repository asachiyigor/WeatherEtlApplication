package com.weatheretl.service;

import com.weatheretl.model.api.WeatherApiModels.DailyData;
import com.weatheretl.model.api.WeatherApiModels.HourlyData;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.model.output.WeatherRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
@DisplayName("Weather Transformer Tests")
class WeatherTransformerTest {

    @InjectMocks
    private WeatherTransformer weatherTransformer;

    private WeatherApiResponse validApiResponse;
    private HourlyData validHourlyData;
    private DailyData validDailyData;

    @BeforeEach
    void setUp() {
        validHourlyData = createValidHourlyData();
        validDailyData = createValidDailyData();
        validApiResponse = createValidApiResponse();
    }

    @Test
    @DisplayName("Should transform valid API response to weather records")
    void shouldTransformValidApiResponseToWeatherRecords() {
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(validApiResponse);
        assertThat(records).hasSize(2); // 2 days of data
        WeatherRecord firstRecord = records.get(0);
        assertThat(firstRecord.getDate()).isEqualTo(LocalDate.of(2024, 1, 1));
        assertThat(firstRecord.getLatitude()).isEqualTo(40.7128);
        assertThat(firstRecord.getLongitude()).isEqualTo(-74.0060);
        assertThat(firstRecord.getAvgTemperature2m24h()).isEqualTo(21.25); // Average of 20.0, 21.0, 22.0, 22.0
        assertThat(firstRecord.getAvgRelativeHumidity2m24h()).isEqualTo(66.25); // Average of 65, 63, 68, 69
        assertThat(firstRecord.getSunriseIso()).isEqualTo("2024-01-01T06:30:00Z");
        assertThat(firstRecord.getSunsetIso()).isEqualTo("2024-01-01T18:15:00Z");
        assertThat(firstRecord.getDaylightHours()).isEqualTo(10.67);
        assertThat(firstRecord.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Should handle empty hourly data")
    void shouldHandleEmptyHourlyData() {
        WeatherApiResponse emptyHourlyResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(HourlyData.builder()
                        .time(Collections.emptyList())
                        .temperature2m(Collections.emptyList())
                        .relativeHumidity2m(Collections.emptyList())
                        .build())
                .daily(validDailyData)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(emptyHourlyResponse);
        assertThat(records).isEmpty();
    }

    @Test
    @DisplayName("Should handle null hourly data")
    void shouldHandleNullHourlyData() {
        WeatherApiResponse nullHourlyResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(null)
                .daily(validDailyData)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(nullHourlyResponse);
        assertThat(records).isEmpty();
    }

    @Test
    @DisplayName("Should handle null API response")
    void shouldHandleNullApiResponse() {
        assertThrows(IllegalArgumentException.class, () -> {
            weatherTransformer.transformWeatherData(null);
        });
    }

    @Test
    @DisplayName("Should handle mismatched hourly data lengths")
    void shouldHandleMismatchedHourlyDataLengths() {
        HourlyData mismatchedData = HourlyData.builder()
                .time(Arrays.asList(1704067200L, 1704070800L, 1704074400L)) // 3 timestamps
                .temperature2m(Arrays.asList(20.0, 21.0)) // Only 2 temperatures
                .relativeHumidity2m(Arrays.asList(65, 63, 68)) // 3 humidity values
                .dewPoint2m(Arrays.asList(12.0)) // Only 1 dew point value
                .windSpeed10m(Arrays.asList(5.0, 5.5, 6.0, 6.5)) // 4 wind speed values
                .build();
        WeatherApiResponse mismatchedResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(mismatchedData)
                .daily(validDailyData)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(mismatchedResponse);
        assertThat(records).isNotNull();
        assertThat(records).hasSize(1);
    }

    @Test
    @DisplayName("Should handle null values in hourly data")
    void shouldHandleNullValuesInHourlyData() {
        HourlyData nullValuesData = HourlyData.builder()
                .time(Arrays.asList(1704067200L, 1704070800L, 1704074400L, 1704078000L))
                .temperature2m(Arrays.asList(20.0, null, 22.0, 23.0))
                .relativeHumidity2m(Arrays.asList(65, 63, null, 69))
                .dewPoint2m(Arrays.asList(12.0, null, 14.0, 15.0))
                .windSpeed10m(Arrays.asList(5.0, 5.5, null, 6.5))
                .build();
        WeatherApiResponse nullValuesResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(nullValuesData)
                .daily(validDailyData)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(nullValuesResponse);
        assertThat(records).hasSize(1);
        WeatherRecord record = records.get(0);
        assertThat(record.getAvgTemperature2m24h()).isEqualTo(21.67); // (20.0 + 22.0 + 23.0) / 3
        assertThat(record.getAvgRelativeHumidity2m24h()).isEqualTo(65.67); // (65 + 63 + 69) / 3
        assertThat(record.getAvgDewPoint2m24h()).isEqualTo(13.67); // (12.0 + 14.0 + 15.0) / 3
        assertThat(record.getAvgWindSpeed10m24h()).isEqualTo(5.67); // (5.0 + 5.5 + 6.5) / 3
    }

    @Test
    @DisplayName("Should group hourly data by day correctly")
    void shouldGroupHourlyDataByDayCorrectly() {
        HourlyData multiDayData = HourlyData.builder()
                .time(Arrays.asList(
                        1704067200L, // 2024-01-01 00:00:00 UTC
                        1704070800L, // 2024-01-01 01:00:00 UTC
                        1704153600L, // 2024-01-02 00:00:00 UTC
                        1704157200L, // 2024-01-02 01:00:00 UTC
                        1704240000L  // 2024-01-03 00:00:00 UTC
                ))
                .temperature2m(Arrays.asList(20.0, 21.0, 18.0, 19.0, 17.0))
                .relativeHumidity2m(Arrays.asList(65, 63, 70, 68, 72))
                .dewPoint2m(Arrays.asList(12.0, 13.0, 11.0, 12.0, 10.0))
                .windSpeed10m(Arrays.asList(5.0, 5.5, 4.5, 5.0, 4.0))
                .build();
        DailyData multiDayDaily = DailyData.builder()
                .time(Arrays.asList(1704067200L, 1704153600L, 1704240000L))
                .sunrise(Arrays.asList(1704090600L, 1704177000L, 1704263400L))
                .sunset(Arrays.asList(1704132900L, 1704219300L, 1704305700L))
                .daylightDuration(Arrays.asList(38400, 38400, 38400))
                .build();
        WeatherApiResponse multiDayResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(multiDayData)
                .daily(multiDayDaily)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(multiDayResponse);
        assertThat(records).hasSize(3);
        WeatherRecord day1 = records.get(0);
        assertThat(day1.getDate()).isEqualTo(LocalDate.of(2024, 1, 1));
        assertThat(day1.getAvgTemperature2m24h()).isEqualTo(20.5); // (20.0 + 21.0) / 2
        WeatherRecord day2 = records.get(1);
        assertThat(day2.getDate()).isEqualTo(LocalDate.of(2024, 1, 2));
        assertThat(day2.getAvgTemperature2m24h()).isEqualTo(18.5); // (18.0 + 19.0) / 2
        WeatherRecord day3 = records.get(2);
        assertThat(day3.getDate()).isEqualTo(LocalDate.of(2024, 1, 3));
        assertThat(day3.getAvgTemperature2m24h()).isEqualTo(17.0); // Only one value
    }

    @Test
    @DisplayName("Should calculate daylight averages correctly")
    void shouldCalculateDaylightAveragesCorrectly() {
        HourlyData daylightData = HourlyData.builder()
                .time(Arrays.asList(
                        1704063600L,
                        1704095400L,
                        1704106200L,
                        1704117000L,
                        1704135000L
                ))
                .temperature2m(Arrays.asList(15.0, 20.0, 25.0, 28.0, 18.0))
                .relativeHumidity2m(Arrays.asList(80, 70, 60, 55, 75))
                .dewPoint2m(Arrays.asList(10.0, 12.0, 14.0, 16.0, 11.0))
                .windSpeed10m(Arrays.asList(3.0, 4.0, 5.0, 6.0, 3.5))
                .build();
        DailyData daylightDaily = DailyData.builder()
                .time(List.of(1704067200L))
                .sunrise(List.of(1704090600L))
                .sunset(List.of(1704132900L))
                .daylightDuration(List.of(38400))
                .build();
        WeatherApiResponse daylightResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(daylightData)
                .daily(daylightDaily)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(daylightResponse);
        assertThat(records).hasSize(1);
        WeatherRecord record = records.get(0);
        assertThat(record.getAvgTemperature2mDaylight()).isEqualTo(24.33); // (20.0 + 25.0 + 28.0) / 3
        assertThat(record.getAvgRelativeHumidity2mDaylight()).isEqualTo(61.67); // (70 + 60 + 55) / 3
        assertThat(record.getAvgDewPoint2mDaylight()).isEqualTo(14.0); // (12.0 + 14.0 + 16.0) / 3
        assertThat(record.getAvgWindSpeed10mDaylight()).isEqualTo(5.0); // (4.0 + 5.0 + 6.0) / 3
    }

    @Test
    @DisplayName("Should handle missing daily data gracefully")
    void shouldHandleMissingDailyDataGracefully() {
        WeatherApiResponse noDailyResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(validHourlyData)
                .daily(null)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(noDailyResponse);
        assertThat(records).hasSize(2);
        WeatherRecord record = records.get(0);
        assertThat(record.getAvgTemperature2m24h()).isNotNull();
        assertThat(record.getSunriseIso()).isNull();
        assertThat(record.getSunsetIso()).isNull();
        assertThat(record.getDaylightHours()).isNull();
        assertThat(record.getAvgTemperature2mDaylight()).isNull();
    }

    @Test
    @DisplayName("Should round calculated values to appropriate precision")
    void shouldRoundCalculatedValuesToAppropriatePrecision() {
        HourlyData precisionData = HourlyData.builder()
                .time(Arrays.asList(1704067200L, 1704070800L, 1704074400L))
                .temperature2m(Arrays.asList(20.123456, 21.987654, 22.555555))
                .relativeHumidity2m(Arrays.asList(65, 63, 68))
                .build();
        DailyData precisionDaily = DailyData.builder()
                .time(List.of(1704067200L))
                .sunrise(List.of(1704090600L))
                .sunset(List.of(1704132900L))
                .daylightDuration(List.of(38400))
                .build();

        WeatherApiResponse precisionResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(precisionData)
                .daily(precisionDaily)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(precisionResponse);
        assertThat(records).hasSize(1);
        WeatherRecord record = records.get(0);
        assertThat(record.getAvgTemperature2m24h()).isEqualTo(21.56);
        assertThat(record.getAvgRelativeHumidity2m24h()).isEqualTo(65.33);
    }

    @Test
    @DisplayName("Should validate coordinates in transformed records")
    void shouldValidateCoordinatesInTransformedRecords() {
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(validApiResponse);
        assertThat(records).isNotEmpty();
        for (WeatherRecord record : records) {
            assertThat(record.getLatitude()).isNotNull();
            assertThat(record.getLongitude()).isNotNull();
            assertThat(record.getLatitude()).isEqualTo(validApiResponse.getLatitude());
            assertThat(record.getLongitude()).isEqualTo(validApiResponse.getLongitude());
            assertThat(record.getCreatedAt()).isNotNull();
        }
    }

    @Test
    @DisplayName("Should handle missing hourly data for specific dates")
    void shouldHandleMissingHourlyDataForSpecificDates() {
        HourlyData limitedHourlyData = HourlyData.builder()
                .time(Arrays.asList(1704067200L, 1704070800L))
                .temperature2m(Arrays.asList(20.0, 21.0))
                .relativeHumidity2m(Arrays.asList(65, 63))
                .build();
        DailyData extendedDailyData = DailyData.builder()
                .time(Arrays.asList(1704067200L, 1704153600L))
                .sunrise(Arrays.asList(1704090600L, 1704177000L))
                .sunset(Arrays.asList(1704132900L, 1704219300L))
                .daylightDuration(Arrays.asList(38400, 38400))
                .build();
        WeatherApiResponse mixedResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(limitedHourlyData)
                .daily(extendedDailyData)
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(mixedResponse);
        assertThat(records).hasSize(1);
        WeatherRecord record = records.get(0);
        assertThat(record.getDate()).isEqualTo(LocalDate.of(2024, 1, 1));
    }

    @Test
    @DisplayName("Should handle empty daily data but non-empty hourly data")
    void shouldHandleEmptyDailyData() {
        WeatherApiResponse emptyDailyResponse = WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .hourly(validHourlyData)
                .daily(DailyData.builder()
                        .time(Collections.emptyList())
                        .sunrise(Collections.emptyList())
                        .sunset(Collections.emptyList())
                        .daylightDuration(Collections.emptyList())
                        .build())
                .build();
        List<WeatherRecord> records = weatherTransformer.transformWeatherData(emptyDailyResponse);
        assertThat(records).hasSize(2);
        for (WeatherRecord record : records) {
            assertThat(record.getSunriseIso()).isNull();
            assertThat(record.getSunsetIso()).isNull();
            assertThat(record.getDaylightHours()).isNull();
        }
    }

    private WeatherApiResponse createValidApiResponse() {
        return WeatherApiResponse.builder()
                .latitude(40.7128)
                .longitude(-74.0060)
                .generationTimeMs(123.45)
                .hourly(validHourlyData)
                .daily(validDailyData)
                .build();
    }

    private HourlyData createValidHourlyData() {
        return HourlyData.builder()
                .time(Arrays.asList(
                        1704067200L,
                        1704070800L,
                        1704074400L,
                        1704078000L,
                        1704153600L,
                        1704157200L
                ))
                .temperature2m(Arrays.asList(20.0, 21.0, 22.0, 22.0, 18.0, 19.0))
                .relativeHumidity2m(Arrays.asList(65, 63, 68, 69, 70, 68))
                .dewPoint2m(Arrays.asList(12.0, 13.0, 14.0, 14.5, 11.0, 12.5))
                .apparentTemperature(Arrays.asList(19.5, 20.5, 21.5, 21.8, 17.5, 18.5))
                .temperature80m(Arrays.asList(19.5, 20.5, 21.5, 21.5, 17.5, 18.5))
                .temperature120m(Arrays.asList(19.0, 20.0, 21.0, 21.0, 17.0, 18.0))
                .windSpeed10m(Arrays.asList(5.0, 5.5, 6.0, 6.2, 4.8, 5.2))
                .windSpeed80m(Arrays.asList(7.0, 7.5, 8.0, 8.5, 6.5, 7.2))
                .visibility(Arrays.asList(15000.0, 16000.0, 18000.0, 20000.0, 14000.0, 15500.0))
                .rain(Arrays.asList(0.0, 0.5, 1.0, 0.3, 0.0, 0.2))
                .showers(Arrays.asList(0.0, 0.2, 0.5, 0.1, 0.0, 0.1))
                .snowfall(Arrays.asList(0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
                .soilTemperature0cm(Arrays.asList(15.0, 15.5, 16.0, 16.2, 14.5, 15.0))
                .soilTemperature6cm(Arrays.asList(14.5, 15.0, 15.5, 15.8, 14.0, 14.5))
                .build();
    }

    private DailyData createValidDailyData() {
        return DailyData.builder()
                .time(Arrays.asList(1704067200L, 1704153600L))
                .sunrise(Arrays.asList(1704090600L, 1704177000L))
                .sunset(Arrays.asList(1704132900L, 1704219300L))
                .daylightDuration(Arrays.asList(38400, 38400))
                .build();
    }
}