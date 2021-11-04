ARG BASE_IMAGE=debian:10.10@sha256:e5cfab8012b17d80f93a7f567797b0c8a2839069d4f50e499152162152518663
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2021-10-12

LABEL Name="senzing/stream-logger" \
      Maintainer="support@senzing.com" \
      Version="1.1.3"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt update \
 && apt -y install \
      python3-dev \
      python3-pip \
      librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Install packages via PIP.

RUN pip3 install \
      boto3 \
      configparser \
      confluent-kafka \
      psutil \
      pika

# Copy files from repository.

COPY ./rootfs /
COPY ./stream-logger.py /app/

# Make non-root container.

USER 1001

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/stream-logger.py"]
