package com.weatheretl;

import com.weatheretl.config.WeatherEtlConfig;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

@TestConfiguration
@Profile("test")
public class WeatherEtlTestConfiguration {

    @Bean
    @Primary
    public WeatherEtlConfig testWeatherEtlConfig() {
        WeatherEtlConfig config = new WeatherEtlConfig();

        WeatherEtlConfig.DefaultLocationConfig locationConfig = new WeatherEtlConfig.DefaultLocationConfig();
        locationConfig.setLatitude(40.7128);
        locationConfig.setLongitude(-74.0060);
        config.setDefaultLocation(locationConfig);

        WeatherEtlConfig.ApiConfig apiConfig = new WeatherEtlConfig.ApiConfig();
        apiConfig.setBaseUrl("https://api.open-meteo.com/v1/forecast");
        apiConfig.setTimeout(Duration.ofSeconds(30));
        config.setApi(apiConfig);

        WeatherEtlConfig.OutputConfig outputConfig = new WeatherEtlConfig.OutputConfig();
        outputConfig.setCsvPath("./output/test_weather_data.csv");
        outputConfig.setBatchSize(50);
        config.setOutput(outputConfig);

        return config;
    }

    @Bean
    @Primary
    public WebClient testWebClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(1024 * 1024))
                .build();
    }
}