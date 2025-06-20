# stream-logger

If you are beginning your journey with [Senzing],
please start with [Senzing Quick Start guides].

You are in the [Senzing Garage] where projects are "tinkered" on.
Although this GitHub repository may help you understand an approach to using Senzing,
it's not considered to be "production ready" and is not considered to be part of the Senzing product.
Heck, it may not even be appropriate for your application of Senzing!

## Overview

The [stream-logger.py] python script consumes data
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
https://github.com/senzing-garage/stream-logger

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

1. [Expectations]
1. [Demonstrate using Command Line]
   1. [Install]
   1. [Run from command line]
1. [Demonstrate using Docker]
   1. [Configuration]
   1. [Run docker container]
1. [References]

### Legend

1. :thinking: - A "thinker" icon means that a little extra thinking may be required.
   Perhaps there are some choices to be made.
   Perhaps it's an optional step.
1. :pencil2: - A "pencil" icon means that the instructions may need modification before performing.
1. :warning: - A "warning" icon means that something tricky is happening, so pay attention.

### Expectations

- **Space:** This repository and demonstration require 6 GB free disk space.
- **Time:** Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.
- **Background knowledge:** This repository assumes a working knowledge of:
  - [Docker]

## Demonstrate using Command Line

### Install

1. Install prerequisites:
   1. [Debian-based installation] - For [Ubuntu and others]
   1. [RPM-based installation] - For [Red Hat, CentOS, openSuse and others].

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

- **[SENZING_DEBUG]**
- **[SENZING_KAFKA_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_GROUP]**
- **[SENZING_KAFKA_TOPIC]**
- **[SENZING_LOG_LEVEL]**
- **[SENZING_MONITORING_PERIOD_IN_SECONDS]**
- **[SENZING_NETWORK]**
- **[SENZING_RABBITMQ_HOST]**
- **[SENZING_RABBITMQ_PASSWORD]**
- **[SENZING_RABBITMQ_PREFETCH_COUNT]**
- **[SENZING_RABBITMQ_QUEUE]**
- **[SENZING_RABBITMQ_USERNAME]**
- **[SENZING_RUNAS_USER]**
- **[SENZING_SLEEP_TIME_IN_SECONDS]**
- **[SENZING_SUBCOMMAND]**
- **[SENZING_THREADS_PER_PROCESS]**

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

1. [Development]
1. [Errors]
1. [Examples]
1. Related artifacts:
   1. [DockerHub]

[Development]: docs/development.md
[Errors]: docs/errors.md
[Examples]: docs/examples.md
[DockerHub]: https://hub.docker.com/r/senzing/stream-logger
[Senzing]: https://senzing.com/
[Senzing Quick Start guides]: https://docs.senzing.com/quickstart/
[Senzing Garage]: https://github.com/senzing-garage
[stream-logger.py]: stream-logger.py
[Expectations]: #expectations
[Demonstrate using Command Line]: #demonstrate-using-command-line
[Install]: #install
[Run from command line]: #run-from-command-line
[Demonstrate using Docker]: #demonstrate-using-docker
[Configuration]: #configuration
[Run docker container]: #run-docker-container
[References]: #references
[Docker]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/docker.md
[Debian-based installation]: docs/debian-based-installation.md
[Ubuntu and others]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based
[RPM-based installation]: docs/rpm-based-installation.md
[Red Hat, CentOS, openSuse and others]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based
[SENZING_DEBUG]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_debug
[SENZING_KAFKA_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_bootstrap_server
[SENZING_KAFKA_GROUP]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_group
[SENZING_KAFKA_TOPIC]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_topic
[SENZING_LOG_LEVEL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_log_level
[SENZING_MONITORING_PERIOD_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_monitoring_period_in_seconds
[SENZING_NETWORK]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_network
[SENZING_RABBITMQ_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_host
[SENZING_RABBITMQ_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_password
[SENZING_RABBITMQ_PREFETCH_COUNT]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_prefetch_count
[SENZING_RABBITMQ_QUEUE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_queue
[SENZING_RABBITMQ_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_username
[SENZING_RUNAS_USER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_runas_user
[SENZING_SLEEP_TIME_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_sleep_time_in_seconds
[SENZING_SUBCOMMAND]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_subcommand
[SENZING_THREADS_PER_PROCESS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_threads_per_process
