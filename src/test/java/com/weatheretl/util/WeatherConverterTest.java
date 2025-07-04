package com.weatheretl.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit тесты для WeatherConverter
 */
@DisplayName("WeatherConverter Tests")
class WeatherConverterTest {

    @Test
    @DisplayName("Should convert Fahrenheit to Celsius correctly")
    void shouldConvertFahrenheitToCelsius() {
        // Given
        Double fahrenheit = 68.0;

        // When
        Double celsius = WeatherConverter.fahrenheitToCelsius(fahrenheit);

        // Then
        assertThat(celsius).isEqualTo(20.0);
    }

    @ParameterizedTest
    @CsvSource({
            "32.0, 0.0",      // Точка замерзания воды
            "68.0, 20.0",     // Комнатная температура
            "98.6, 37.0",     // Температура тела
            "212.0, 100.0",   // Точка кипения воды
            "-40.0, -40.0"    // Точка пересечения шкал
    })
    @DisplayName("Should convert various Fahrenheit values to Celsius")
    void shouldConvertFahrenheitToCelsiusParameterized(Double fahrenheit, Double expectedCelsius) {
        // When
        Double celsius = WeatherConverter.fahrenheitToCelsius(fahrenheit);

        // Then
        assertThat(celsius).isEqualTo(expectedCelsius);
    }

    @Test
    @DisplayName("Should return null when converting null Fahrenheit")
    void shouldReturnNullWhenConvertingNullFahrenheit() {
        // When
        Double celsius = WeatherConverter.fahrenheitToCelsius(null);

        // Then
        assertThat(celsius).isNull();
    }

    @Test
    @DisplayName("Should convert knots to meters per second correctly")
    void shouldConvertKnotsToMetersPerSecond() {
        // Given
        Double knots = 10.0;

        // When
        Double metersPerSecond = WeatherConverter.knotsToMetersPerSecond(knots);

        // Then
        assertThat(metersPerSecond).isCloseTo(5.14444, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("Should return null when converting null knots")
    void shouldReturnNullWhenConvertingNullKnots() {
        // When
        Double metersPerSecond = WeatherConverter.knotsToMetersPerSecond(null);

        // Then
        assertThat(metersPerSecond).isNull();
    }

    @Test
    @DisplayName("Should convert inches to millimeters correctly")
    void shouldConvertInchesToMillimeters() {
        // Given
        Double inches = 1.0;

        // When
        Double millimeters = WeatherConverter.inchesToMillimeters(inches);

        // Then
        assertThat(millimeters).isEqualTo(25.4);
    }

    @Test
    @DisplayName("Should convert feet to meters correctly")
    void shouldConvertFeetToMeters() {
        // Given
        Double feet = 100.0;

        // When
        Double meters = WeatherConverter.feetToMeters(feet);

        // Then
        assertThat(meters).isCloseTo(30.48, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("Should convert Unix timestamp to ISO string")
    void shouldConvertUnixTimestampToIso() {
        // Given
        Long unixTimestamp = 1716768000L; // 2024-05-27T00:00:00Z

        // When
        String iso = WeatherConverter.unixToIso(unixTimestamp);

        // Then
        assertThat(iso).isEqualTo("2024-05-27T00:00:00Z");
    }

    @Test
    @DisplayName("Should return null when converting null Unix timestamp")
    void shouldReturnNullWhenConvertingNullUnixTimestamp() {
        // When
        String iso = WeatherConverter.unixToIso(null);

        // Then
        assertThat(iso).isNull();
    }

    @Test
    @DisplayName("Should convert seconds to hours correctly")
    void shouldConvertSecondsToHours() {
        // Given
        Integer seconds = 3600;

        // When
        Double hours = WeatherConverter.secondsToHours(seconds);

        // Then
        assertThat(hours).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should calculate average from list of numbers")
    void shouldCalculateAverageFromListOfNumbers() {
        // Given
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0);

        // When
        Double average = WeatherConverter.calculateAverage(values);

        // Then
        assertThat(average).isEqualTo(25.0);
    }

    @Test
    @DisplayName("Should return null when calculating average from empty list")
    void shouldReturnNullWhenCalculatingAverageFromEmptyList() {
        // When
        Double average = WeatherConverter.calculateAverage(Collections.emptyList());

        // Then
        assertThat(average).isNull();
    }

    @Test
    @DisplayName("Should return null when calculating average from null list")
    void shouldReturnNullWhenCalculatingAverageFromNullList() {
        // When
        Double average = WeatherConverter.calculateAverage(null);

        // Then
        assertThat(average).isNull();
    }

    @Test
    @DisplayName("Should calculate average ignoring null values")
    void shouldCalculateAverageIgnoringNullValues() {
        // Given
        List<Double> values = Arrays.asList(10.0, null, 20.0, null, 30.0);

        // When
        Double average = WeatherConverter.calculateAverage(values);

        // Then
        assertThat(average).isEqualTo(20.0);
    }

    @Test
    @DisplayName("Should calculate sum from list of numbers")
    void shouldCalculateSumFromListOfNumbers() {
        // Given
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0);

        // When
        Double sum = WeatherConverter.calculateSum(values);

        // Then
        assertThat(sum).isEqualTo(100.0);
    }

    @Test
    @DisplayName("Should calculate sum ignoring null values")
    void shouldCalculateSumIgnoringNullValues() {
        // Given
        List<Double> values = Arrays.asList(10.0, null, 20.0, null, 30.0);

        // When
        Double sum = WeatherConverter.calculateSum(values);

        // Then
        assertThat(sum).isEqualTo(60.0);
    }

    @Test
    @DisplayName("Should get daylight indices correctly")
    void shouldGetDaylightIndicesCorrectly() {
        // Given
        List<Long> timestamps = Arrays.asList(
                1716768000L, // 2024-05-27T00:00:00Z
                1716789600L, // 2024-05-27T06:00:00Z
                1716811200L, // 2024-05-27T12:00:00Z
                1716832800L, // 2024-05-27T18:00:00Z
                1716854400L  // 2024-05-28T00:00:00Z
        );
        Long sunriseTimestamp = 1716786000L; // 2024-05-27T05:00:00Z
        Long sunsetTimestamp = 1716829200L;  // 2024-05-27T17:00:00Z

        // When
        List<Integer> daylightIndices = WeatherConverter.getDaylightIndices(
                timestamps, sunriseTimestamp, sunsetTimestamp);

        // Then
        assertThat(daylightIndices).containsExactly(1, 2); // Индексы 1 и 2 попадают в дневное время
    }

    @Test
    @DisplayName("Should calculate daylight average correctly")
    void shouldCalculateDaylightAverageCorrectly() {
        // Given
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0);
        List<Long> timestamps = Arrays.asList(
                1716768000L, // 2024-05-27T00:00:00Z
                1716789600L, // 2024-05-27T06:00:00Z
                1716811200L, // 2024-05-27T12:00:00Z
                1716832800L, // 2024-05-27T18:00:00Z
                1716854400L  // 2024-05-28T00:00:00Z
        );
        Long sunriseTimestamp = 1716786000L; // 2024-05-27T05:00:00Z
        Long sunsetTimestamp = 1716829200L;  // 2024-05-27T17:00:00Z

        // When
        Double average = WeatherConverter.calculateDaylightAverage(
                values, timestamps, sunriseTimestamp, sunsetTimestamp);

        // Then
        assertThat(average).isEqualTo(25.0); // Среднее из 20.0 и 30.0
    }

    @Test
    @DisplayName("Should round values correctly")
    void shouldRoundValuesCorrectly() {
        // Given
        Double value = 123.456789;

        // When
        Double rounded = WeatherConverter.round(value, 2);

        // Then
        assertThat(rounded).isEqualTo(123.46);
    }

    @Test
    @DisplayName("Should return null when rounding null value")
    void shouldReturnNullWhenRoundingNullValue() {
        // When
        Double rounded = WeatherConverter.round(null, 2);

        // Then
        assertThat(rounded).isNull();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5})
    @DisplayName("Should round to different decimal places")
    void shouldRoundToDifferentDecimalPlaces(int decimals) {
        Double value = 123.456789;

        Double rounded = WeatherConverter.round(value, decimals);

        assertThat(rounded).isNotNull();

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(value)
                .setScale(decimals, java.math.RoundingMode.HALF_UP);

        assertThat(rounded).isEqualTo(expected.doubleValue());
    }
}