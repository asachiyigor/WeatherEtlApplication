package com.weatheretl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableRetry
@EnableAsync
@EnableScheduling
public class WeatherEtlApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherEtlApplication.class, args);
    }
}