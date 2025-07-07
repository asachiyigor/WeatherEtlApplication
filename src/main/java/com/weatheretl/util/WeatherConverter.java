package com.weatheretl.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

public class WeatherConverter {
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public static Double fahrenheitToCelsius(Double fahrenheit) {
        if (fahrenheit == null) {
            return null;
        }
        return (fahrenheit - 32.0) * 5.0 / 9.0;
    }

    public static Double knotsToMetersPerSecond(Double knots) {
        if (knots == null) {
            return null;
        }
        return knots * 0.514444;
    }

    public static Double inchesToMillimeters(Double inches) {
        if (inches == null) {
            return null;
        }
        return inches * 25.4;
    }

    public static Double feetToMeters(Double feet) {
        if (feet == null) {
            return null;
        }
        return feet * 0.3048;
    }

    public static String unixToIso(Long unixTimestamp) {
        if (unixTimestamp == null) {
            return null;
        }
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(unixTimestamp),
                ZoneOffset.UTC
        );
        return dateTime.format(ISO_FORMATTER);
    }

    public static Double secondsToHours(Integer seconds) {
        if (seconds == null) {
            return null;
        }
        return seconds / 3600.0;
    }

    public static Double calculateAverage(List<? extends Number> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        double sum = 0.0;
        int count = 0;
        for (Number value : values) {
            if (value != null) {
                sum += value.doubleValue();
                count++;
            }
        }
        return count > 0 ? sum / count : null;
    }

    public static Double calculateSum(List<? extends Number> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        double sum = 0.0;
        for (Number value : values) {
            if (value != null) {
                sum += value.doubleValue();
            }
        }
        return sum;
    }

    public static List<Integer> getDaylightIndices(List<Long> timestamps,
                                                   Long sunriseTimestamp, Long sunsetTimestamp) {
        if (timestamps == null || sunriseTimestamp == null || sunsetTimestamp == null) {
            return List.of();
        }
        return java.util.stream.IntStream.range(0, timestamps.size())
                .filter(i -> {
                    Long timestamp = timestamps.get(i);
                    return timestamp != null &&
                            timestamp >= sunriseTimestamp &&
                            timestamp <= sunsetTimestamp;
                })
                .boxed()
                .collect(java.util.stream.Collectors.toList());
    }

    public static Double calculateDaylightAverage(List<? extends Number> values,
                                                  List<Long> timestamps,
                                                  Long sunriseTimestamp,
                                                  Long sunsetTimestamp) {
        if (values == null || timestamps == null ||
                sunriseTimestamp == null || sunsetTimestamp == null ||
                values.size() != timestamps.size()) {
            return null;
        }
        List<Integer> daylightIndices = getDaylightIndices(timestamps, sunriseTimestamp, sunsetTimestamp);
        if (daylightIndices.isEmpty()) {
            return null;
        }
        double sum = 0.0;
        int count = 0;
        for (Integer index : daylightIndices) {
            Number value = values.get(index);
            if (value != null) {
                sum += value.doubleValue();
                count++;
            }
        }
        return count > 0 ? sum / count : null;
    }

    public static Double calculateDaylightSum(List<? extends Number> values,
                                              List<Long> timestamps,
                                              Long sunriseTimestamp,
                                              Long sunsetTimestamp) {
        if (values == null || timestamps == null ||
                sunriseTimestamp == null || sunsetTimestamp == null ||
                values.size() != timestamps.size()) {
            return null;
        }

        List<Integer> daylightIndices = getDaylightIndices(timestamps, sunriseTimestamp, sunsetTimestamp);

        if (daylightIndices.isEmpty()) {
            return null;
        }
        double sum = 0.0;
        for (Integer index : daylightIndices) {
            Number value = values.get(index);
            if (value != null) {
                sum += value.doubleValue();
            }
        }
        return sum;
    }

    public static Double round(Double value, int decimals) {
        if (value == null) {
            return null;
        }
        double multiplier = Math.pow(10, decimals);
        return Math.round(value * multiplier) / multiplier;
    }
}