FROM maven:3.3.9 as builder

WORKDIR /app

COPY pom.xml pom.xml
RUN mvn dependency:resolve

COPY src/ src/
RUN mvn package

FROM scratch

COPY --from=builder /app/target/*-shaded.jar circulation.jar
