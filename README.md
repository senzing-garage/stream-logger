# stream-logger

## Overview

The [stream-logger.py](stream-logger.py) python script consumes data
from various stream sources (Kafka, RabbitMQ) and logs it.
This is mostly used for testing the contents of of a queue.
The `senzing/stream-logger` docker image is a wrapper for use in docker formations (e.g. docker-compose, kubernetes).

To see all of the subcommands, run:

```console
$ ./stream-logger.py --help
usage: stream-logger.py [-h]
                        {kafka,rabbitmq,sleep,version,docker-acceptance-test}
                        ...

Log contents from a stream. For more information, see
https://github.com/senzing/stream-logger

positional arguments:
  {kafka,rabbitmq,sleep,version,docker-acceptance-test}
                        Subcommands (SENZING_SUBCOMMAND):
    kafka               Read JSON Lines from Apache Kafka topic.
    rabbitmq            Read JSON Lines from RabbitMQ queue.
    sleep               Do nothing but sleep. For Docker testing.
    version             Print version of program.
    docker-acceptance-test
                        For Docker acceptance testing.

optional arguments:
  -h, --help            show this help message and exit
```

### Contents

1. [Expectations](#expectations)
1. [Demonstrate using Command Line](#demonstrate-using-command-line)
    1. [Install](#install)
    1. [Run from command line](#run-from-command-line)
1. [Demonstrate using Docker](#demonstrate-using-docker)
    1. [Configuration](#configuration)
    1. [Run docker container](#run-docker-container)
1. [References](#references)

### Legend

1. :thinking: - A "thinker" icon means that a little extra thinking may be required.
   Perhaps you'll need to make some choices.
   Perhaps it's an optional step.
1. :pencil2: - A "pencil" icon means that the instructions may need modification before performing.
1. :warning: - A "warning" icon means that something tricky is happening, so pay attention.

## Expectations

- **Space:** This repository and demonstration require 6 GB free disk space.
- **Time:** Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.
- **Background knowledge:** This repository assumes a working knowledge of:
  - [Docker](https://github.com/Senzing/knowledge-base/blob/main/WHATIS/docker.md)

## Demonstrate using Command Line

### Install

1. Install prerequisites:
    1. [Debian-based installation](docs/debian-based-installation.md) - For Ubuntu and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based)
    1. [RPM-based installation](docs/rpm-based-installation.md) - For Red Hat, CentOS, openSuse and [others](https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based).

### Run from command line

1. Run command.
    Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}
    ./stream-logger.py version
    ```

## Demonstrate using Docker

### Configuration

Configuration values specified by environment variable or command line parameter.

- **[SENZING_DEBUG](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_debug)**
- **[SENZING_KAFKA_BOOTSTRAP_SERVER](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_bootstrap_server)**
- **[SENZING_KAFKA_GROUP](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_group)**
- **[SENZING_KAFKA_TOPIC](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_topic)**
- **[SENZING_LOG_LEVEL](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_log_level)**
- **[SENZING_MONITORING_PERIOD_IN_SECONDS](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_monitoring_period_in_seconds)**
- **[SENZING_NETWORK](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_network)**
- **[SENZING_RABBITMQ_HOST](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_host)**
- **[SENZING_RABBITMQ_PASSWORD](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_password)**
- **[SENZING_RABBITMQ_PREFETCH_COUNT](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_prefetch_count)**
- **[SENZING_RABBITMQ_QUEUE](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_queue)**
- **[SENZING_RABBITMQ_USERNAME](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_username)**
- **[SENZING_RUNAS_USER](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_runas_user)**
- **[SENZING_SLEEP_TIME_IN_SECONDS](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_sleep_time_in_seconds)**
- **[SENZING_SUBCOMMAND](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_subcommand)**
- **[SENZING_THREADS_PER_PROCESS](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_threads_per_process)**

### Run docker container

1. :pencil2: Set environment variables.
   Example:

    ```console
    export SENZING_SUBCOMMAND=kafka
    export SENZING_KAFKA_BOOTSTRAP_SERVER=senzing-kafka:9092
    export SENZING_KAFKA_TOPIC=senzing-kafka-topic
    ```

1. Run docker container.
   Example:

    ```console
    sudo docker run \
      --env SENZING_SUBCOMMAND="${SENZING_SUBCOMMAND}" \
      --env SENZING_KAFKA_BOOTSTRAP_SERVER="${SENZING_KAFKA_BOOTSTRAP_SERVER}" \
      --env SENZING_KAFKA_TOPIC="${SENZING_KAFKA_TOPIC}" \
      --interactive \
      --rm \
      --tty \
      senzing/stream-logger
    ```

## References

1. [Development](docs/development.md)
1. [Errors](docs/errors.md)
1. [Examples](docs/examples.md)
1. Related artifacts:
    1. [DockerHub](https://hub.docker.com/r/senzing/stream-logger)
