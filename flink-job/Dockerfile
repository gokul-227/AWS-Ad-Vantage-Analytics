# ============================================================
# Stage 1: Build the Flink job fat JAR using Maven
# ============================================================
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app

# Copy POM first for dependency caching
COPY flink-job/pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY flink-job/src/ src/
RUN mvn clean package -DskipTests -B

# ============================================================
# Stage 2: Runtime image with Flink
# ============================================================
FROM flink:1.20-java17

# Copy the fat JAR to Flink's usrlib directory
# Flink automatically picks up JARs from this directory
COPY --from=builder /app/target/ad-analytics-flink-1.0.0.jar /opt/flink/usrlib/ad-analytics.jar

# Create checkpoints directory with correct permissions
USER root
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
USER flink

# The entrypoint is handled by the Flink base image
# JobManager and TaskManager modes are set via docker-compose command
