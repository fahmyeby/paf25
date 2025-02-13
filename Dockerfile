
FROM maven:3.9.9-eclipse-temurin-23 AS build

WORKDIR /movies

COPY movies/pom.xml .
COPY movies/src ./src

RUN mvn clean package -DskipTests

FROM openjdk:23-jdk-slim

WORKDIR /

COPY --from=build /movies/target/movies-0.0.1-SNAPSHOT.jar app.jar
COPY /data ./data

ENTRYPOINT ["java", "-jar", "app.jar"]