# First Stage - Build
FROM maven:3.9.9-eclipse-temurin-21 AS builder
ARG COMPILE_DIR=/compiledir
WORKDIR ${COMPILE_DIR}

# Copy Maven wrapper files first
COPY .mvn/ .mvn/
COPY mvnw mvnw.cmd ./

# Ensure proper line endings and executable permissions
RUN sed -i 's/\r$//' mvnw && \
    chmod +x mvnw

# Copy the rest of the project files
COPY pom.xml .
COPY src src

# Copy additional resources
COPY src/main/resources/data/director_movies_report.jasper /app/director_movies_report.jasper
COPY src/main/resources/data/movies_post_2010.zip /app/movies_post_2010.zip

# Download dependencies first (for better caching)
RUN ./mvnw dependency:go-offline

# Build the project
RUN ./mvnw clean package -DskipTests

# Second Stage - Runtime
FROM eclipse-temurin:21-jdk
ARG WORK_DIR=/app
WORKDIR ${WORK_DIR}

# Copy the application jar from the build stage
COPY --from=builder /compiledir/target/movies-0.0.1-SNAPSHOT.jar app.jar

# Copy necessary data files from the builder stage
COPY --from=builder /app/director_movies_report.jasper ${WORK_DIR}/director_movies_report.jasper
COPY --from=builder /app/movies_post_2010.zip ${WORK_DIR}/movies_post_2010.zip

ENV SERVER_PORT=3000
EXPOSE ${SERVER_PORT}

ENTRYPOINT ["java", "-jar", "app.jar"]