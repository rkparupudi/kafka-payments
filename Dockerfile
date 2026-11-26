# One multi-stage image definition for every service: docker build --build-arg MODULE=payment-api .
# Stage 1 compiles the whole multi-module project (identical for all services, so BuildKit caches it once).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package

# Stage 2: small runtime image with just the chosen service's Spring Boot jar.
FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
