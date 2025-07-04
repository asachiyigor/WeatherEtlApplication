package com.weatheretl.config;

import com.weatheretl.service.WeatherApiClient;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Configuration
@RequiredArgsConstructor
public class WebClientConfig {

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10MB
                .filter(logRequest())
                .filter(logResponse())
                .build();
    }

    private ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            if (clientRequest.url().toString().contains("open-meteo")) {
                org.slf4j.LoggerFactory.getLogger(WeatherApiClient.class)
                        .debug("Request: {} {}", clientRequest.method(), clientRequest.url());
            }
            return Mono.just(clientRequest);
        });
    }

    private ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
            if (clientResponse.statusCode().isError()) {
                org.slf4j.LoggerFactory.getLogger(WeatherApiClient.class)
                        .error("Response error: {}", clientResponse.statusCode());
            } else {
                org.slf4j.LoggerFactory.getLogger(WeatherApiClient.class)
                        .debug("Response: {}", clientResponse.statusCode());
            }
            return Mono.just(clientResponse);
        });
    }
}