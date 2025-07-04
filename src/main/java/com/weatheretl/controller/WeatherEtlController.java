package com.weatheretl.controller;

import com.weatheretl.service.WeatherEtlService;
import com.weatheretl.service.WeatherEtlService.EtlResult;
import com.weatheretl.service.WeatherEtlService.EtlStats;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;

@Slf4j
@RestController
@RequestMapping("/api/v1/weather-etl")
@RequiredArgsConstructor
public class WeatherEtlController {

    private final WeatherEtlService weatherEtlService;

    @PostMapping("/execute/api-to-csv")
    public ResponseEntity<EtlResult> executeApiToCsv(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate,
            @RequestParam(required = false) String csvPath) {

        log.info("REST API request: Execute API to CSV for period {} to {}", startDate, endDate);

        try {
            EtlResult result = weatherEtlService.executeApiToCsv(startDate, endDate, csvPath);

            if (result.isSuccess()) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Failed to execute API to CSV", e);
            EtlResult errorResult = EtlResult.builder()
                    .startDate(startDate)
                    .endDate(endDate)
                    .success(false)
                    .errorMessage("Internal server error: " + e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResult);
        }
    }

    @PostMapping("/execute/api-to-database")
    public ResponseEntity<EtlResult> executeApiToDatabase(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {

        log.info("REST API request: Execute API to Database for period {} to {}", startDate, endDate);

        try {
            EtlResult result = weatherEtlService.executeApiToDatabase(startDate, endDate);

            if (result.isSuccess()) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Failed to execute API to Database", e);
            EtlResult errorResult = EtlResult.builder()
                    .startDate(startDate)
                    .endDate(endDate)
                    .success(false)
                    .errorMessage("Internal server error: " + e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResult);
        }
    }

    @PostMapping("/execute/api-to-all")
    public ResponseEntity<EtlResult> executeApiToCsvAndDatabase(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate,
            @RequestParam(required = false) String csvPath) {

        log.info("REST API request: Execute API to CSV + Database for period {} to {}", startDate, endDate);

        try {
            EtlResult result = weatherEtlService.executeApiToCsvAndDatabase(startDate, endDate, csvPath);

            if (result.isSuccess()) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Failed to execute API to CSV + Database", e);
            EtlResult errorResult = EtlResult.builder()
                    .startDate(startDate)
                    .endDate(endDate)
                    .success(false)
                    .errorMessage("Internal server error: " + e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResult);
        }
    }

    @GetMapping("/stats")
    public ResponseEntity<EtlStats> getEtlStats() {
        log.info("REST API request: Get ETL statistics");

        try {
            EtlStats stats = weatherEtlService.getEtlStats();
            return ResponseEntity.ok(stats);

        } catch (Exception e) {
            log.error("Failed to get ETL statistics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/health")
    public ResponseEntity<HealthStatus> health() {
        return ResponseEntity.ok(HealthStatus.builder()
                .status("UP")
                .timestamp(java.time.LocalDateTime.now())
                .build());
    }

    @lombok.Data
    @lombok.Builder
    public static class HealthStatus {
        private String status;
        private java.time.LocalDateTime timestamp;
    }
}