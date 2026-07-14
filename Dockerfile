FROM --platform=$BUILDPLATFORM docker.io/maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

COPY pom.xml .
COPY src src

RUN mvn clean package -DskipTests

FROM scratch
COPY --from=build /build/target/kafka-connect-test-*.jar /kafka-connect-test.jar
