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

@DisplayName("WeatherConverter Tests")
class WeatherConverterTest {

    @Test
    @DisplayName("Should convert Fahrenheit to Celsius correctly")
    void shouldConvertFahrenheitToCelsius() {
        Double fahrenheit = 68.0;
        Double celsius = WeatherConverter.fahrenheitToCelsius(fahrenheit);
        assertThat(celsius).isEqualTo(20.0);
    }

    @ParameterizedTest
    @CsvSource({
            "32.0, 0.0",
            "68.0, 20.0",
            "98.6, 37.0",
            "212.0, 100.0",
            "-40.0, -40.0"
    })
    @DisplayName("Should convert various Fahrenheit values to Celsius")
    void shouldConvertFahrenheitToCelsiusParameterized(Double fahrenheit, Double expectedCelsius) {
        Double celsius = WeatherConverter.fahrenheitToCelsius(fahrenheit);
        assertThat(celsius).isEqualTo(expectedCelsius);
    }

    @Test
    @DisplayName("Should return null when converting null Fahrenheit")
    void shouldReturnNullWhenConvertingNullFahrenheit() {
        Double celsius = WeatherConverter.fahrenheitToCelsius(null);
        assertThat(celsius).isNull();
    }

    @Test
    @DisplayName("Should convert knots to meters per second correctly")
    void shouldConvertKnotsToMetersPerSecond() {
        Double knots = 10.0;
        Double metersPerSecond = WeatherConverter.knotsToMetersPerSecond(knots);
        assertThat(metersPerSecond).isCloseTo(5.14444, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("Should return null when converting null knots")
    void shouldReturnNullWhenConvertingNullKnots() {
        Double metersPerSecond = WeatherConverter.knotsToMetersPerSecond(null);
        assertThat(metersPerSecond).isNull();
    }

    @Test
    @DisplayName("Should convert inches to millimeters correctly")
    void shouldConvertInchesToMillimeters() {
        Double inches = 1.0;
        Double millimeters = WeatherConverter.inchesToMillimeters(inches);
        assertThat(millimeters).isEqualTo(25.4);
    }

    @Test
    @DisplayName("Should convert feet to meters correctly")
    void shouldConvertFeetToMeters() {
        Double feet = 100.0;
        Double meters = WeatherConverter.feetToMeters(feet);
        assertThat(meters).isCloseTo(30.48, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("Should convert Unix timestamp to ISO string")
    void shouldConvertUnixTimestampToIso() {
        Long unixTimestamp = 1716768000L;
        String iso = WeatherConverter.unixToIso(unixTimestamp);
        assertThat(iso).isEqualTo("2024-05-27T00:00:00Z");
    }

    @Test
    @DisplayName("Should return null when converting null Unix timestamp")
    void shouldReturnNullWhenConvertingNullUnixTimestamp() {
        String iso = WeatherConverter.unixToIso(null);
        assertThat(iso).isNull();
    }

    @Test
    @DisplayName("Should convert seconds to hours correctly")
    void shouldConvertSecondsToHours() {
        Integer seconds = 3600;
        Double hours = WeatherConverter.secondsToHours(seconds);
        assertThat(hours).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Should calculate average from list of numbers")
    void shouldCalculateAverageFromListOfNumbers() {
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0);
        Double average = WeatherConverter.calculateAverage(values);
        assertThat(average).isEqualTo(25.0);
    }

    @Test
    @DisplayName("Should return null when calculating average from empty list")
    void shouldReturnNullWhenCalculatingAverageFromEmptyList() {
        Double average = WeatherConverter.calculateAverage(Collections.emptyList());
        assertThat(average).isNull();
    }

    @Test
    @DisplayName("Should return null when calculating average from null list")
    void shouldReturnNullWhenCalculatingAverageFromNullList() {
        Double average = WeatherConverter.calculateAverage(null);
        assertThat(average).isNull();
    }

    @Test
    @DisplayName("Should calculate average ignoring null values")
    void shouldCalculateAverageIgnoringNullValues() {
        List<Double> values = Arrays.asList(10.0, null, 20.0, null, 30.0);
        Double average = WeatherConverter.calculateAverage(values);
        assertThat(average).isEqualTo(20.0);
    }

    @Test
    @DisplayName("Should calculate sum from list of numbers")
    void shouldCalculateSumFromListOfNumbers() {
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0);
        Double sum = WeatherConverter.calculateSum(values);
        assertThat(sum).isEqualTo(100.0);
    }

    @Test
    @DisplayName("Should calculate sum ignoring null values")
    void shouldCalculateSumIgnoringNullValues() {
        List<Double> values = Arrays.asList(10.0, null, 20.0, null, 30.0);
        Double sum = WeatherConverter.calculateSum(values);
        assertThat(sum).isEqualTo(60.0);
    }

    @Test
    @DisplayName("Should get daylight indices correctly")
    void shouldGetDaylightIndicesCorrectly() {
        List<Long> timestamps = Arrays.asList(
                1716768000L,
                1716789600L,
                1716811200L,
                1716832800L,
                1716854400L
        );
        Long sunriseTimestamp = 1716786000L;
        Long sunsetTimestamp = 1716829200L;
        List<Integer> daylightIndices = WeatherConverter.getDaylightIndices(
                timestamps, sunriseTimestamp, sunsetTimestamp);
        assertThat(daylightIndices).containsExactly(1, 2);
    }

    @Test
    @DisplayName("Should calculate daylight average correctly")
    void shouldCalculateDaylightAverageCorrectly() {
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0);
        List<Long> timestamps = Arrays.asList(
                1716768000L,
                1716789600L,
                1716811200L,
                1716832800L,
                1716854400L
        );
        Long sunriseTimestamp = 1716786000L;
        Long sunsetTimestamp = 1716829200L;
        Double average = WeatherConverter.calculateDaylightAverage(
                values, timestamps, sunriseTimestamp, sunsetTimestamp);
        assertThat(average).isEqualTo(25.0);
    }

    @Test
    @DisplayName("Should round values correctly")
    void shouldRoundValuesCorrectly() {
        Double value = 123.456789;
        Double rounded = WeatherConverter.round(value, 2);
        assertThat(rounded).isEqualTo(123.46);
    }

    @Test
    @DisplayName("Should return null when rounding null value")
    void shouldReturnNullWhenRoundingNullValue() {
        Double rounded = WeatherConverter.round(null, 2);
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