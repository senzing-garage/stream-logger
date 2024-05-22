# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.7] - 2024-05-22

### Changed in 1.1.7

- In `Dockerfile`, updated FROM instruction to `debian:11.9-slim@sha256:0e75382930ceb533e2f438071307708e79dc86d9b8e433cc6dd1a96872f2651d`
- In `requirements.txt`, updated:
  - boto3==1.34.110
  - confluent_kafka==2.4.0

## [1.1.6] - 2023-09-30

### Changed in 1.1.6

- In `Dockerfile`, updated FROM instruction to `debian:11.7-slim@sha256:c618be84fc82aa8ba203abbb07218410b0f5b3c7cb6b4e7248fda7785d4f9946`
- In `requirements.txt`, updated:
  - boto3==1.28.57
  - confluent_kafka==2.2.0
  - pika==1.3.2

## [1.1.5] - 2022-09-29

### Changed in 1.1.5

- In `Dockerfile`, updated FROM instruction to `debian:11.5-slim@sha256:5cf1d98cd0805951484f33b34c1ab25aac7007bb41c8b9901d97e4be3cf3ab04`
- In `requirements.txt`, updated:
  - boto3==1.24.82
  - confluent_kafka==1.9.2
  - pika==1.3.0

## [1.1.4] - 2021-12-08

### Added in 1.1.4

- Updated to debian:10.11

## [1.1.3] - 2021-10-12

### Added in 1.1.3

- Updated to debian:10.10

## [1.1.2] - 2020-09-10

### Added in 1.1.2

- Support for existing RabbitMQ entities.

### Fixed in 1.1.2

- Erroneous call to G2

## [1.1.1] - 2020-06-24

### Fixed in 1.1.1

- Adjust VisibilityTimeout

## [1.1.0] - 2020-06-22

### Added in 1.1.0

- Support for AWS SQS.

## [1.0.1] - 2020-03-02

### Fixed in 1.0.1

- Bug fix for `queued_records_total`.

## [1.0.0] - 2020-02-09

### Added in 1.0.0

- Logging of RabbitMQ and Kafka queues.
