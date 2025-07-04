# Multi-stage build for Weather ETL Pipeline

# Build stage
FROM openjdk:17.0.1-jdk-slim AS builder

WORKDIR /app

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Gradle wrapper and build files
COPY gradlew gradlew
COPY gradle gradle
COPY build.gradle settings.gradle ./

# Copy source code
COPY src src

# Make gradlew executable
RUN chmod +x ./gradlew

# Build the application
RUN ./gradlew clean bootJar --no-daemon

# Runtime stage
FROM openjdk:17.0.1-jdk-slim

WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r weather && useradd -r -g weather weather

# Create necessary directories
RUN mkdir -p /app/output /app/logs && \
    chown -R weather:weather /app

# Copy JAR file from builder stage
COPY --from=builder /app/build/libs/*.jar app.jar

# Copy configuration files
COPY --chown=weather:weather src/main/resources/application-docker.yaml /app/application-docker.yaml

# Set ownership
RUN chown weather:weather app.jar

# Switch to non-root user
USER weather

# Expose port
EXPOSE 8080

# Environment variables
ENV SPRING_PROFILES_ACTIVE=docker
ENV JAVA_OPTS="-Xms512m -Xmx1024m"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

# Entry point
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar \"$@\"", "--"]