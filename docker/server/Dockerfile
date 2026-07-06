FROM eclipse-temurin:11-jre

# We need curl for healthcheck
RUN apt -y update; apt -y install curl

ENV IGNIFYR_HOME=/usr/local/ignifyr
RUN mkdir -p "$IGNIFYR_HOME"
WORKDIR $IGNIFYR_HOME

COPY ./ignifyr-server/target/ignifyr-server-standalone.jar .
COPY ./docker/server/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
