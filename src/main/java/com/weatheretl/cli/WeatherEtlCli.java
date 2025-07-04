package com.weatheretl.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weatheretl.model.api.WeatherApiModels.WeatherApiResponse;
import com.weatheretl.service.WeatherEtlService;
import com.weatheretl.service.WeatherEtlService.EtlResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class WeatherEtlCli implements CommandLineRunner {

    private final WeatherEtlService weatherEtlService;
    private final ObjectMapper objectMapper;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void run(String... args) throws Exception {
        if (args.length == 0) {
            log.info("Starting Weather ETL application as web service");
            printWelcomeMessage();
            return;
        }

        try {
            processCliArguments(args);
        } catch (Exception e) {
            log.error("CLI execution failed", e);
            System.err.println("Error: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private void processCliArguments(String[] args) throws Exception {
        List<String> arguments = Arrays.asList(args);

        if (arguments.contains("--help") || arguments.contains("-h")) {
            printUsage();
            return;
        }

        String source = getArgumentValue(arguments, "--source");
        String output = getArgumentValue(arguments, "--output");

        if (source == null) {
            throw new IllegalArgumentException("Source parameter is required. Use --source=api or --source=json");
        }

        if (output == null) {
            throw new IllegalArgumentException("Output parameter is required. Use --output=csv, --output=database, or --output=all");
        }

        EtlResult result;

        switch (source.toLowerCase()) {
            case "api":
                result = processApiSource(arguments, output);
                break;
            case "json":
                result = processJsonSource(arguments, output);
                break;
            default:
                throw new IllegalArgumentException("Invalid source: " + source + ". Use 'api' or 'json'");
        }

        printResult(result);
        System.exit(result.isSuccess() ? 0 : 1);
    }

    private EtlResult processApiSource(List<String> arguments, String output) {
        String startDateStr = getArgumentValue(arguments, "--start-date");
        String endDateStr = getArgumentValue(arguments, "--end-date");

        if (startDateStr == null || endDateStr == null) {
            throw new IllegalArgumentException("Start date and end date are required for API source. Use --start-date and --end-date");
        }

        LocalDate startDate;
        LocalDate endDate;

        try {
            startDate = LocalDate.parse(startDateStr, DATE_FORMATTER);
            endDate = LocalDate.parse(endDateStr, DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format. Use yyyy-MM-dd format");
        }

        if (startDate.isAfter(endDate)) {
            throw new IllegalArgumentException("Start date must be before or equal to end date");
        }

        String csvPath = getArgumentValue(arguments, "--csv-path");

        return switch (output.toLowerCase()) {
            case "csv" -> weatherEtlService.executeApiToCsv(startDate, endDate, csvPath);
            case "database" -> weatherEtlService.executeApiToDatabase(startDate, endDate);
            case "all" -> weatherEtlService.executeApiToCsvAndDatabase(startDate, endDate, csvPath);
            default -> throw new IllegalArgumentException("Invalid output: " + output + ". Use 'csv', 'database', or 'all'");
        };
    }

    private EtlResult processJsonSource(List<String> arguments, String output) throws Exception {
        String jsonPath = getArgumentValue(arguments, "--json-path");

        if (jsonPath == null) {
            throw new IllegalArgumentException("JSON path is required for JSON source. Use --json-path");
        }

        File jsonFile = new File(jsonPath);
        if (!jsonFile.exists()) {
            throw new IllegalArgumentException("JSON file not found: " + jsonPath);
        }

        WeatherApiResponse apiResponse = objectMapper.readValue(jsonFile, WeatherApiResponse.class);

        String csvPath = getArgumentValue(arguments, "--csv-path");

        return switch (output.toLowerCase()) {
            case "csv" -> weatherEtlService.processJsonData(apiResponse, true, false, csvPath);
            case "database" -> weatherEtlService.processJsonData(apiResponse, false, true, csvPath);
            case "all" -> weatherEtlService.processJsonData(apiResponse, true, true, csvPath);
            default -> throw new IllegalArgumentException("Invalid output: " + output + ". Use 'csv', 'database', or 'all'");
        };
    }

    private String getArgumentValue(List<String> arguments, String argName) {
        return arguments.stream()
                .filter(arg -> arg.startsWith(argName + "="))
                .map(arg -> arg.substring(argName.length() + 1))
                .findFirst()
                .orElse(null);
    }

    private void printResult(EtlResult result) {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("ETL PROCESS RESULT");
        System.out.println("=".repeat(50));

        if (result.isSuccess()) {
            System.out.println("✅ SUCCESS");
            System.out.println("Records processed: " + result.getRecordsTransformed());
            System.out.println("CSV exported: " + (result.isCsvExported() ? "✅" : "❌"));
            System.out.println("Database saved: " + (result.isDatabaseSaved() ? "✅" : "❌"));
        } else {
            System.out.println("❌ FAILED");
            System.out.println("Error: " + result.getErrorMessage());
        }

        if (result.getStartDate() != null && result.getEndDate() != null) {
            System.out.println("Period: " + result.getStartDate() + " to " + result.getEndDate());
        }

        System.out.println("=".repeat(50));
    }

    private void printWelcomeMessage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌤️  WEATHER ETL PIPELINE");
        System.out.println("=".repeat(60));
        System.out.println("Application started successfully!");
        System.out.println("Web interface available at: http://localhost:8080");
        System.out.println("API documentation: http://localhost:8080/swagger-ui.html");
        System.out.println("Health check: http://localhost:8080/actuator/health");
        System.out.println("\nFor CLI usage, restart with --help");
        System.out.println("=".repeat(60));
    }

    private void printUsage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌤️  WEATHER ETL PIPELINE - CLI USAGE");
        System.out.println("=".repeat(60));
        System.out.println();
        System.out.println("SYNOPSIS:");
        System.out.println("  java -jar weather-etl.jar [OPTIONS]");
        System.out.println();
        System.out.println("OPTIONS:");
        System.out.println("  --source=<api|json>          Data source (required)");
        System.out.println("  --output=<csv|database|all>  Output destination (required)");
        System.out.println();
        System.out.println("FOR API SOURCE:");
        System.out.println("  --start-date=<yyyy-MM-dd>    Start date (required)");
        System.out.println("  --end-date=<yyyy-MM-dd>      End date (required)");
        System.out.println("  --csv-path=<path>            Custom CSV file path (optional)");
        System.out.println();
        System.out.println("FOR JSON SOURCE:");
        System.out.println("  --json-path=<path>           Path to JSON file (required)");
        System.out.println("  --csv-path=<path>            Custom CSV file path (optional)");
        System.out.println();
        System.out.println("EXAMPLES:");
        System.out.println("  # Extract from API and save to CSV");
        System.out.println("  java -jar weather-etl.jar \\");
        System.out.println("    --source=api \\");
        System.out.println("    --output=csv \\");
        System.out.println("    --start-date=2025-05-16 \\");
        System.out.println("    --end-date=2025-05-30");
        System.out.println();
        System.out.println("  # Extract from API and save to database");
        System.out.println("  java -jar weather-etl.jar \\");
        System.out.println("    --source=api \\");
        System.out.println("    --output=database \\");
        System.out.println("    --start-date=2025-05-16 \\");
        System.out.println("    --end-date=2025-05-30");
        System.out.println();
        System.out.println("  # Extract from API and save to both CSV and database");
        System.out.println("  java -jar weather-etl.jar \\");
        System.out.println("    --source=api \\");
        System.out.println("    --output=all \\");
        System.out.println("    --start-date=2025-05-16 \\");
        System.out.println("    --end-date=2025-05-30 \\");
        System.out.println("    --csv-path=/custom/path/weather.csv");
        System.out.println();
        System.out.println("  # Process JSON file and save to CSV");
        System.out.println("  java -jar weather-etl.jar \\");
        System.out.println("    --source=json \\");
        System.out.println("    --output=csv \\");
        System.out.println("    --json-path=/path/to/weather-data.json");
        System.out.println();
        System.out.println("  # Process JSON file and save to database");
        System.out.println("  java -jar weather-etl.jar \\");
        System.out.println("    --source=json \\");
        System.out.println("    --output=database \\");
        System.out.println("    --json-path=/path/to/weather-data.json");
        System.out.println();
        System.out.println("DOCKER EXAMPLES:");
        System.out.println("  # Run in Docker container");
        System.out.println("  docker run --rm weather-etl:latest \\");
        System.out.println("    --source=api \\");
        System.out.println("    --output=csv \\");
        System.out.println("    --start-date=2025-05-16 \\");
        System.out.println("    --end-date=2025-05-30");
        System.out.println();
        System.out.println("  # Run with custom network for database access");
        System.out.println("  docker run --rm --network=weather-network weather-etl:latest \\");
        System.out.println("    --source=api \\");
        System.out.println("    --output=database \\");
        System.out.println("    --start-date=2025-05-16 \\");
        System.out.println("    --end-date=2025-05-30");
        System.out.println();
        System.out.println("NOTES:");
        System.out.println("  • Date format must be yyyy-MM-dd (ISO 8601)");
        System.out.println("  • CSV files are created in ./output/ directory by default");
        System.out.println("  • Database connection settings are configured in application.yml");
        System.out.println("  • For large date ranges, consider using smaller batches");
        System.out.println("  • The application supports duplicate handling for database inserts");
        System.out.println();
        System.out.println("For web interface, run without arguments:");
        System.out.println("  java -jar weather-etl.jar");
        System.out.println("=".repeat(60));
    }
}