package com.weatheretl.service;

import com.weatheretl.model.api.WeatherApiModels.DailyData;
import com.weatheretl.model.api.WeatherApiModels.HourlyData;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.model.output.WeatherRecord;
import com.weatheretl.util.WeatherConverter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WeatherTransformer {

    public List<WeatherRecord> transformWeatherData(WeatherApiResponse apiResponse) {
        if (apiResponse == null) {
            throw new IllegalArgumentException("API response cannot be null");
        }
        log.info("Transforming weather data for location: {}, {}",
                apiResponse.getLatitude(), apiResponse.getLongitude());
        List<WeatherRecord> records = new ArrayList<>();
        HourlyData hourlyData = apiResponse.getHourly();
        DailyData dailyData = apiResponse.getDaily();
        if (hourlyData == null || hourlyData.getTime() == null || hourlyData.getTime().isEmpty()) {
            log.warn("Missing hourly or daily data in API response");
            return records;
        }
        if (dailyData == null || dailyData.getTime() == null || dailyData.getTime().isEmpty()) {
            log.warn("Missing daily data, processing only hourly data");
            return processHourlyDataOnly(apiResponse, hourlyData);
        }
        List<LocalDate> dates = extractDatesFromDailyData(dailyData);
        for (int dayIndex = 0; dayIndex < dates.size(); dayIndex++) {
            LocalDate date = dates.get(dayIndex);
            Long sunriseTimestamp = getSafeValue(dailyData.getSunrise(), dayIndex);
            Long sunsetTimestamp = getSafeValue(dailyData.getSunset(), dayIndex);
            Integer daylightDuration = getSafeValue(dailyData.getDaylightDuration(), dayIndex);
            List<Integer> dayHourlyIndices = getHourlyIndicesForDay(hourlyData.getTime(), date);
            if (dayHourlyIndices.isEmpty()) {
                log.warn("No hourly data found for date: {}", date);
                continue;
            }
            WeatherRecord record = createWeatherRecord(
                    apiResponse, date, dayHourlyIndices, hourlyData,
                    sunriseTimestamp, sunsetTimestamp, daylightDuration
            );
            records.add(record);
        }
        log.info("Successfully transformed {} weather records", records.size());
        return records;
    }

    private List<WeatherRecord> processHourlyDataOnly(WeatherApiResponse apiResponse, HourlyData hourlyData) {
        List<WeatherRecord> records = new ArrayList<>();
        List<LocalDate> uniqueDates = extractUniqueDatesFromHourlyData(hourlyData.getTime());
        for (LocalDate date : uniqueDates) {
            List<Integer> dayHourlyIndices = getHourlyIndicesForDay(hourlyData.getTime(), date);
            if (!dayHourlyIndices.isEmpty()) {
                WeatherRecord record = createWeatherRecord(
                        apiResponse, date, dayHourlyIndices, hourlyData,
                        null, null, null
                );
                records.add(record);
            }
        }
        return records;
    }

    private List<LocalDate> extractUniqueDatesFromHourlyData(List<Long> timestamps) {
        List<LocalDate> uniqueDates = new ArrayList<>();
        if (timestamps != null) {
            for (Long timestamp : timestamps) {
                if (timestamp != null) {
                    LocalDate date = Instant.ofEpochSecond(timestamp)
                            .atZone(ZoneOffset.UTC)
                            .toLocalDate();
                    if (!uniqueDates.contains(date)) {
                        uniqueDates.add(date);
                    }
                }
            }
        }
        return uniqueDates;
    }

    private WeatherRecord createWeatherRecord(WeatherApiResponse apiResponse,
                                              LocalDate date,
                                              List<Integer> dayHourlyIndices,
                                              HourlyData hourlyData,
                                              Long sunriseTimestamp,
                                              Long sunsetTimestamp,
                                              Integer daylightDuration) {
        WeatherRecord.WeatherRecordBuilder builder = WeatherRecord.builder()
                .date(date)
                .latitude(apiResponse.getLatitude())
                .longitude(apiResponse.getLongitude())
                .createdAt(LocalDateTime.now())
                .avgTemperature2m24h(calculateDayAverage(hourlyData.getTemperature2m(), dayHourlyIndices))
                .avgRelativeHumidity2m24h(calculateDayAverage(hourlyData.getRelativeHumidity2m(), dayHourlyIndices))
                .avgDewPoint2m24h(calculateDayAverage(hourlyData.getDewPoint2m(), dayHourlyIndices))
                .avgApparentTemperature24h(calculateDayAverage(hourlyData.getApparentTemperature(), dayHourlyIndices))
                .avgTemperature80m24h(calculateDayAverage(hourlyData.getTemperature80m(), dayHourlyIndices))
                .avgTemperature120m24h(calculateDayAverage(hourlyData.getTemperature120m(), dayHourlyIndices))
                .avgWindSpeed10m24h(calculateDayAverage(hourlyData.getWindSpeed10m(), dayHourlyIndices))
                .avgWindSpeed80m24h(calculateDayAverage(hourlyData.getWindSpeed80m(), dayHourlyIndices))
                .avgVisibility24h(calculateDayAverage(hourlyData.getVisibility(), dayHourlyIndices))
                .totalRain24h(calculateDaySum(hourlyData.getRain(), dayHourlyIndices))
                .totalShowers24h(calculateDaySum(hourlyData.getShowers(), dayHourlyIndices))
                .totalSnowfall24h(calculateDaySum(hourlyData.getSnowfall(), dayHourlyIndices))
                .windSpeed10mMPerS(convertValue(
                        calculateDayAverage(hourlyData.getWindSpeed10m(), dayHourlyIndices),
                        WeatherConverter::knotsToMetersPerSecond))
                .windSpeed80mMPerS(convertValue(
                        calculateDayAverage(hourlyData.getWindSpeed80m(), dayHourlyIndices),
                        WeatherConverter::knotsToMetersPerSecond))
                .temperature2mCelsius(convertValue(
                        calculateDayAverage(hourlyData.getTemperature2m(), dayHourlyIndices),
                        WeatherConverter::fahrenheitToCelsius))
                .apparentTemperatureCelsius(convertValue(
                        calculateDayAverage(hourlyData.getApparentTemperature(), dayHourlyIndices),
                        WeatherConverter::fahrenheitToCelsius))
                .temperature80mCelsius(convertValue(
                        calculateDayAverage(hourlyData.getTemperature80m(), dayHourlyIndices),
                        WeatherConverter::fahrenheitToCelsius))
                .temperature120mCelsius(convertValue(
                        calculateDayAverage(hourlyData.getTemperature120m(), dayHourlyIndices),
                        WeatherConverter::fahrenheitToCelsius))
                .soilTemperature0cmCelsius(convertValue(
                        calculateDayAverage(hourlyData.getSoilTemperature0cm(), dayHourlyIndices),
                        WeatherConverter::fahrenheitToCelsius))
                .soilTemperature6cmCelsius(convertValue(
                        calculateDayAverage(hourlyData.getSoilTemperature6cm(), dayHourlyIndices),
                        WeatherConverter::fahrenheitToCelsius))
                .rainMm(convertValue(
                        calculateDaySum(hourlyData.getRain(), dayHourlyIndices),
                        WeatherConverter::inchesToMillimeters))
                .showersMm(convertValue(
                        calculateDaySum(hourlyData.getShowers(), dayHourlyIndices),
                        WeatherConverter::inchesToMillimeters))
                .snowfallMm(convertValue(
                        calculateDaySum(hourlyData.getSnowfall(), dayHourlyIndices),
                        WeatherConverter::inchesToMillimeters));
        if (sunriseTimestamp != null && sunsetTimestamp != null) {
            builder
                    .avgTemperature2mDaylight(calculateDaylightAverage(hourlyData.getTemperature2m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgRelativeHumidity2mDaylight(calculateDaylightAverage(hourlyData.getRelativeHumidity2m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgDewPoint2mDaylight(calculateDaylightAverage(hourlyData.getDewPoint2m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgApparentTemperatureDaylight(calculateDaylightAverage(hourlyData.getApparentTemperature(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgTemperature80mDaylight(calculateDaylightAverage(hourlyData.getTemperature80m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgTemperature120mDaylight(calculateDaylightAverage(hourlyData.getTemperature120m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgWindSpeed10mDaylight(calculateDaylightAverage(hourlyData.getWindSpeed10m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgWindSpeed80mDaylight(calculateDaylightAverage(hourlyData.getWindSpeed80m(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .avgVisibilityDaylight(calculateDaylightAverage(hourlyData.getVisibility(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .totalRainDaylight(calculateDaylightSum(hourlyData.getRain(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .totalShowersDaylight(calculateDaylightSum(hourlyData.getShowers(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .totalSnowfallDaylight(calculateDaylightSum(hourlyData.getSnowfall(), hourlyData.getTime(), sunriseTimestamp, sunsetTimestamp))
                    .sunriseIso(WeatherConverter.unixToIso(sunriseTimestamp))
                    .sunsetIso(WeatherConverter.unixToIso(sunsetTimestamp));
        }
        if (daylightDuration != null) {
            builder.daylightHours(WeatherConverter.round(
                    WeatherConverter.secondsToHours(daylightDuration), 2));
        }
        return builder.build();
    }

    @FunctionalInterface
    private interface ValueConverter {
        Double convert(Double value);
    }

    private Double convertValue(Double value, ValueConverter converter) {
        if (value == null) {
            return null;
        }
        try {
            Double converted = converter.convert(value);
            return converted != null ? WeatherConverter.round(converted, 2) : null;
        } catch (Exception e) {
            log.warn("Error converting value {}: {}", value, e.getMessage());
            return null;
        }
    }

    private List<LocalDate> extractDatesFromDailyData(DailyData dailyData) {
        List<LocalDate> dates = new ArrayList<>();
        if (dailyData.getTime() != null) {
            for (Long timestamp : dailyData.getTime()) {
                if (timestamp != null) {
                    LocalDate date = Instant.ofEpochSecond(timestamp)
                            .atZone(ZoneOffset.UTC)
                            .toLocalDate();
                    dates.add(date);
                }
            }
        }
        return dates;
    }

    private List<Integer> getHourlyIndicesForDay(List<Long> timestamps, LocalDate targetDate) {
        List<Integer> indices = new ArrayList<>();

        if (timestamps != null) {
            for (int i = 0; i < timestamps.size(); i++) {
                Long timestamp = timestamps.get(i);
                if (timestamp != null) {
                    LocalDate date = Instant.ofEpochSecond(timestamp)
                            .atZone(ZoneOffset.UTC)
                            .toLocalDate();
                    if (date.equals(targetDate)) {
                        indices.add(i);
                    }
                }
            }
        }
        return indices;
    }

    private <T extends Number> Double calculateDayAverage(List<T> values, List<Integer> indices) {
        if (values == null || indices == null || indices.isEmpty()) {
            return null;
        }
        double sum = 0.0;
        int count = 0;
        for (Integer index : indices) {
            if (index < values.size()) {
                T value = values.get(index);
                if (value != null) {
                    sum += value.doubleValue();
                    count++;
                }
            }
        }
        return count > 0 ? WeatherConverter.round(sum / count, 2) : null;
    }

    private <T extends Number> Double calculateDaySum(List<T> values, List<Integer> indices) {
        if (values == null || indices == null || indices.isEmpty()) {
            return null;
        }

        double sum = 0.0;

        for (Integer index : indices) {
            if (index < values.size()) {
                T value = values.get(index);
                if (value != null) {
                    sum += value.doubleValue();
                }
            }
        }
        return WeatherConverter.round(sum, 2);
    }

    private <T extends Number> Double calculateDaylightAverage(List<T> values,
                                                               List<Long> timestamps,
                                                               Long sunriseTimestamp,
                                                               Long sunsetTimestamp) {
        if (sunriseTimestamp == null || sunsetTimestamp == null) {
            return null;
        }
        Double result = WeatherConverter.calculateDaylightAverage(values, timestamps, sunriseTimestamp, sunsetTimestamp);
        return result != null ? WeatherConverter.round(result, 2) : null;
    }

    private <T extends Number> Double calculateDaylightSum(List<T> values,
                                                           List<Long> timestamps,
                                                           Long sunriseTimestamp,
                                                           Long sunsetTimestamp) {
        if (sunriseTimestamp == null || sunsetTimestamp == null) {
            return null;
        }
        Double result = WeatherConverter.calculateDaylightSum(values, timestamps, sunriseTimestamp, sunsetTimestamp);
        return result != null ? WeatherConverter.round(result, 2) : null;
    }

    private <T> T getSafeValue(List<T> list, int index) {
        if (list != null && index >= 0 && index < list.size()) {
            return list.get(index);
        }
        return null;
    }
}