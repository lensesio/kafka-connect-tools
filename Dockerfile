FROM gradle:5.2.1 as builder

COPY --chown=gradle:gradle ./ /src
WORKDIR /src
RUN gradle fatJar

FROM openjdk:8-jre-alpine

RUN mkdir -p /usr/local/lib/kafka-connect-tools
COPY --from=builder /src/build/libs/*.jar /usr/local/lib/kafka-connect-tools
COPY bin/docker-cmd.sh /usr/local/bin/connect-cli
RUN chmod +x /usr/local/bin/connect-cli

CMD ["connect-cli", "--help"]
