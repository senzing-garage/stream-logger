#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# stream-logger.py Logger of streaming input.
# -----------------------------------------------------------------------------

from glob import glob
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen
import argparse
import boto3
import configparser
import confluent_kafka
import datetime
import json
import linecache
import logging
import math
import multiprocessing
import os
import pika
import queue
import signal
import string
import sys
import threading
import time

__all__ = []
__version__ = "1.1.1"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2020-02-06'
__updated__ = '2020-06-24'

SENZING_PRODUCT_ID = "5011"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
log_format = '%(asctime)s %(message)s'

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "debug": {
        "default": False,
        "env": "SENZING_DEBUG",
        "cli": "debug"
    },
    "delay_in_seconds": {
        "default": 0,
        "env": "SENZING_DELAY_IN_SECONDS",
        "cli": "delay-in-seconds"
    },
    "kafka_bootstrap_server": {
        "default": "localhost:9092",
        "env": "SENZING_KAFKA_BOOTSTRAP_SERVER",
        "cli": "kafka-bootstrap-server",
    },
    "kafka_group": {
        "default": "senzing-kafka-group",
        "env": "SENZING_KAFKA_GROUP",
        "cli": "kafka-group"
    },
    "kafka_topic": {
        "default": "senzing-kafka-topic",
        "env": "SENZING_KAFKA_TOPIC",
        "cli": "kafka-topic"
    },
    "monitoring_period_in_seconds": {
        "default": 60 * 10,
        "env": "SENZING_MONITORING_PERIOD_IN_SECONDS",
        "cli": "monitoring-period-in-seconds",
    },
    "rabbitmq_host": {
        "default": "localhost:5672",
        "env": "SENZING_RABBITMQ_HOST",
        "cli": "rabbitmq-host",
    },
    "rabbitmq_password": {
        "default": "bitnami",
        "env": "SENZING_RABBITMQ_PASSWORD",
        "cli": "rabbitmq-password",
    },
    "rabbitmq_prefetch_count": {
        "default": 50,
        "env": "SENZING_RABBITMQ_PREFETCH_COUNT",
        "cli": "rabbitmq-prefetch-count",
    },
    "rabbitmq_queue": {
        "default": "senzing-rabbitmq-queue",
        "env": "SENZING_RABBITMQ_QUEUE",
        "cli": "rabbitmq-queue",
    },
    "rabbitmq_use_existing_entities": {
        "default": True,
        "env": "SENZING_RABBITMQ_USE_EXISTING_ENTITIES",
        "cli": "rabbitmq-use-existing-entities",
    },
    "rabbitmq_username": {
        "default": "user",
        "env": "SENZING_RABBITMQ_USERNAME",
        "cli": "rabbitmq-username",
    },
    "sleep_time_in_seconds": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "sqs_queue_url": {
        "default": None,
        "env": "SENZING_SQS_QUEUE_URL",
        "cli": "sqs-queue-url"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    },
    "threads_per_process": {
        "default": 4,
        "env": "SENZING_THREADS_PER_PROCESS",
        "cli": "threads-per-process",
    },
}

# Enumerate keys in 'configuration_locator' that should not be printed to the log.

keys_to_redact = [
]

# -----------------------------------------------------------------------------
# Define argument parser
# -----------------------------------------------------------------------------


def get_parser():
    ''' Parse commandline arguments. '''

    subcommands = {
        'kafka': {
            "help": 'Read JSON Lines from Apache Kafka topic.',
            "arguments": {
                "--debug": {
                    "dest": "debug",
                    "action": "store_true",
                    "help": "Enable debugging. (SENZING_DEBUG) Default: False"
                },
                "--delay-in-seconds": {
                    "dest": "delay_in_seconds",
                    "metavar": "SENZING_DELAY_IN_SECONDS",
                    "help": "Delay before processing in seconds. DEFAULT: 0"
                },
                "--kafka-bootstrap-server": {
                    "dest": "kafka_bootstrap_server",
                    "metavar": "SENZING_KAFKA_BOOTSTRAP_SERVER",
                    "help": "Kafka bootstrap server. Default: localhost:9092"
                },
                "--kafka-group": {
                    "dest": "kafka_group",
                    "metavar": "SENZING_KAFKA_GROUP",
                    "help": "Kafka group. Default: senzing-kafka-group"
                },
                "--kafka-topic": {
                    "dest": "kafka_topic",
                    "metavar": "SENZING_KAFKA_TOPIC",
                    "help": "Kafka topic. Default: senzing-kafka-topic"
                },
                "--monitoring-period-in-seconds": {
                    "dest": "monitoring_period_in_seconds",
                    "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                    "help": "Period, in seconds, between monitoring reports. Default: 600"
                },
                "--threads-per-process": {
                    "dest": "threads_per_process",
                    "metavar": "SENZING_THREADS_PER_PROCESS",
                    "help": "Number of threads per process. Default: 4"
                },
            },
        },
        'rabbitmq': {
            "help": 'Read JSON Lines from RabbitMQ queue.',
            "arguments": {
                "--debug": {
                    "dest": "debug",
                    "action": "store_true",
                    "help": "Enable debugging. (SENZING_DEBUG) Default: False"
                },
                "--delay-in-seconds": {
                    "dest": "delay_in_seconds",
                    "metavar": "SENZING_DELAY_IN_SECONDS",
                    "help": "Delay before processing in seconds. DEFAULT: 0"
                },
                "--monitoring-period-in-seconds": {
                    "dest": "monitoring_period_in_seconds",
                    "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                    "help": "Period, in seconds, between monitoring reports. Default: 600"
                },
                "--rabbitmq-host": {
                    "dest": "rabbitmq_host",
                    "metavar": "SENZING_RABBITMQ_HOST",
                    "help": "RabbitMQ host. Default: localhost:5672"
                },
                "--rabbitmq-password": {
                    "dest": "rabbitmq_password",
                    "metavar": "SENZING_RABBITMQ_PASSWORD",
                    "help": "RabbitMQ password. Default: bitnami"
                },
                "--rabbitmq-prefetch-count": {
                    "dest": "rabbitmq_prefetch_count",
                    "metavar": "SENZING_RABBITMQ_PREFETCH_COUNT",
                    "help": "RabbitMQ prefetch-count. Default: 50"
                },
                "--rabbitmq-queue": {
                    "dest": "rabbitmq_queue",
                    "metavar": "SENZING_RABBITMQ_QUEUE",
                    "help": "RabbitMQ queue. Default: senzing-rabbitmq-queue"
                },
                "--rabbitmq-use-existing-entities": {
                    "dest": "rabbitmq_use_existing_entities",
                    "metavar": "SENZING_RABBITMQ_USE_EXISTING_ENTITIES",
                    "help": "Connect to an existing queue using its settings. An error is thrown if the queue does not exist. If False, it will create the queue if it does not exist. If it exists, then it will attempt to connect, checking the settings match. Default: True"
	    },
                "--rabbitmq-username": {
                    "dest": "rabbitmq_username",
                    "metavar": "SENZING_RABBITMQ_USERNAME",
                    "help": "RabbitMQ username. Default: user"
                },
                "--threads-per-process": {
                    "dest": "threads_per_process",
                    "metavar": "SENZING_THREADS_PER_PROCESS",
                    "help": "Number of threads per process. Default: 4"
                },
            },
        },
        'sqs': {
            "help": 'Read JSON Lines from AWS SQS queue.',
            "arguments": {
                "--debug": {
                    "dest": "debug",
                    "action": "store_true",
                    "help": "Enable debugging. (SENZING_DEBUG) Default: False"
                },
                "--delay-in-seconds": {
                    "dest": "delay_in_seconds",
                    "metavar": "SENZING_DELAY_IN_SECONDS",
                    "help": "Delay before processing in seconds. DEFAULT: 0"
                },
                "--monitoring-period-in-seconds": {
                    "dest": "monitoring_period_in_seconds",
                    "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                    "help": "Period, in seconds, between monitoring reports. Default: 600"
                },
                "--sqs-queue-url": {
                    "dest": "sqs_queue_url",
                    "metavar": "SENZING_SQS_QUEUE_URL",
                    "help": "AWS SQS URL. Default: none"
                },
                "--threads-per-process": {
                    "dest": "threads_per_process",
                    "metavar": "SENZING_THREADS_PER_PROCESS",
                    "help": "Number of threads per process. Default: 4"
                },
            },
        },
        'sleep': {
            "help": 'Do nothing but sleep. For Docker testing.',
            "arguments": {
                "--sleep-time-in-seconds": {
                    "dest": "sleep_time_in_seconds",
                    "metavar": "SENZING_SLEEP_TIME_IN_SECONDS",
                    "help": "Sleep time in seconds. DEFAULT: 0 (infinite)"
                },
            },
        },
        'version': {
            "help": 'Print version of program.',
        },
        'docker-acceptance-test': {
            "help": 'For Docker acceptance testing.',
        },
    }

    parser = argparse.ArgumentParser(prog="stream-logger.py", description="Log contents from a stream. For more information, see https://github.com/senzing/stream-logger")
    subparsers = parser.add_subparsers(dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    for subcommand_key, subcommand_values in subcommands.items():
        subcommand_help = subcommand_values.get('help', "")
        subcommand_arguments = subcommand_values.get('arguments', {})
        subparser = subparsers.add_parser(subcommand_key, help=subcommand_help)
        for argument_key, argument_values in subcommand_arguments.items():
            subparser.add_argument(argument_key, **argument_values)

    return parser

# -----------------------------------------------------------------------------
# Message handling
# -----------------------------------------------------------------------------

# 1xx Informational (i.e. logging.info())
# 3xx Warning (i.e. logging.warning())
# 5xx User configuration issues (either logging.warning() or logging.err() for Client errors)
# 7xx Internal error (i.e. logging.error for Server errors)
# 9xx Debugging (i.e. logging.debug())


MESSAGE_INFO = 100
MESSAGE_WARN = 300
MESSAGE_ERROR = 700
MESSAGE_DEBUG = 900

message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "101": "{0}",
    "120": "Sleeping for requested delay of {0} seconds.",
    "127": "Monitor: {0}",
    "129": "{0} is running.",
    "130": "RabbitMQ channel closed by the broker. Shutting down thread {0}.",
    "152": "Sleeping {0} seconds before deploying administrative threads.",
    "190": "AWS SQS Long-polling: No messages from {0}",
    "293": "For information on warnings and errors, see https://github.com/Senzing/stream-logger#errors",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "499": "{0}",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "561": "Unknown RabbitMQ error when connecting: {0}.",
    "562": "Could not connect to RabbitMQ host at {1}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details. Error: {0}",
    "696": "Bad SENZING_SUBCOMMAND: {0}.",
    "697": "No processing done.",
    "698": "Program terminated with error.",
    "699": "{0}",
    "700": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "721": "Running low on workers.  May need to restart",
    "722": "Kafka commit failed for {0}",
    "899": "{0}",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "904": "{0} processed: {1}",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    index_string = str(index)
    return "{0} {1}".format(message(generic_index, index), message(index, *args))


def message_info(index, *args):
    return message_generic(MESSAGE_INFO, index, *args)


def message_warning(index, *args):
    return message_generic(MESSAGE_WARN, index, *args)


def message_error(index, *args):
    return message_generic(MESSAGE_ERROR, index, *args)


def message_debug(index, *args):
    return message_generic(MESSAGE_DEBUG, index, *args)


def get_exception():
    ''' Get details about an exception. '''
    exception_type, exception_object, traceback = sys.exc_info()
    frame = traceback.tb_frame
    line_number = traceback.tb_lineno
    filename = frame.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_number, frame.f_globals)
    return {
        "filename": filename,
        "line_number": line_number,
        "line": line.strip(),
        "exception": exception_object,
        "type": exception_type,
        "traceback": traceback,
    }

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default. '''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        result[key] = value.get('default', None)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy OS environment variables into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = [
        'debug',
        'rabbitmq_use_existing_entities',
    ]
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = [
        "delay_in_seconds",
        "monitoring_period_in_seconds",
        "threads_per_process",
    ]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Initialize counters.

    result['counter_processed_messages'] = 0

    return result


def validate_configuration(config):
    ''' Check aggregate configuration from commandline options, environment variables, config files, and defaults. '''

    user_warning_messages = []
    user_error_messages = []

    # Perform subcommand specific checking.

    subcommand = config.get('subcommand')

    if subcommand in ['rabbitmq', 'kafka', 'sqs']:
        pass

    # Log warning messages.

    for user_warning_message in user_warning_messages:
        logging.warning(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(293))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(697)


def redact_configuration(config):
    ''' Return a shallow copy of config with certain keys removed. '''
    result = config.copy()
    for key in keys_to_redact:
        try:
            result.pop(key)
        except:
            pass
    return result

# -----------------------------------------------------------------------------
# Class: ReadThread
# -----------------------------------------------------------------------------


class ReadThread(threading.Thread):

    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config

# -----------------------------------------------------------------------------
# Class: ReadKafkaThread
# -----------------------------------------------------------------------------


class ReadKafkaThread(ReadThread):

    def __init__(self, config):
        super().__init__(config)

    def run(self):
        '''Process for reading lines from Kafka and feeding them to a process_function() function.'''

        logging.info(message_info(129, threading.current_thread().name))

        # Create Kafka client.

        consumer_configuration = {
            'bootstrap.servers': self.config.get('kafka_bootstrap_server'),
            'group.id': self.config.get("kafka_group"),
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
            }
        consumer = confluent_kafka.Consumer(consumer_configuration)
        consumer.subscribe([self.config.get("kafka_topic")])

        # In a loop, get messages from Kafka.

        while True:

            # Get message from Kafka queue.
            # Timeout quickly to allow other co-routines to process.

            kafka_message = consumer.poll(1.0)

            # Handle non-standard Kafka output.

            if kafka_message is None:
                continue
            if kafka_message.error():
                if kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(message_error(722, kafka_message.error()))
                    continue

            # Construct and verify Kafka message.

            kafka_message_string = kafka_message.value().strip()
            if not kafka_message_string:
                continue
            if isinstance(kafka_message_string, bytes):
                kafka_message_string = kafka_message_string.decode()
            logging.debug(message_debug(904, threading.current_thread().name, kafka_message_string))
            self.config['counter_processed_messages'] += 1

            # Write message to log.

            logging.info(message_info(101, kafka_message_string))
            consumer.commit()

        consumer.close()

# -----------------------------------------------------------------------------
# Class: ReadRabbitMQWriteG2Thread
# -----------------------------------------------------------------------------


class ReadRabbitMQThread(ReadThread):

    def __init__(self, config):
        super().__init__(config)

    def callback(self, channel, method, header, body):
        ''' Called by Pika whenever a message is received. '''
        jsonline = body.decode()
        logging.debug(message_debug(904, threading.current_thread().name, jsonline))
        self.config['counter_processed_messages'] += 1
        logging.info(message_info(101, jsonline))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        '''Process for reading lines from RabbitMQ and feeding them to a process_function() function.'''

        logging.info(message_info(129, threading.current_thread().name))

        # Get config parameters.

        rabbitmq_queue = self.config.get("rabbitmq_queue")
        rabbitmq_passive_declare = self.config.get("rabbitmq_use_existing_entities")
        rabbitmq_username = self.config.get("rabbitmq_username")
        rabbitmq_password = self.config.get("rabbitmq_password")
        rabbitmq_host = self.config.get("rabbitmq_host")
        rabbitmq_prefetch_count = self.config.get("rabbitmq_prefetch_count")

        # Connect to RabbitMQ queue.

        try:
            credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()
            channel.queue_declare(queue=rabbitmq_queue, passive=rabbitmq_passive_declare)
            channel.basic_qos(prefetch_count=rabbitmq_prefetch_count)
            channel.basic_consume(on_message_callback=self.callback, queue=rabbitmq_queue)
        except pika.exceptions.AMQPConnectionError as err:
            exit_error(562, err, rabbitmq_host)
        except BaseException as err:
            exit_error(561, err)

        # Start consuming.

        try:
            channel.start_consuming()
        except pika.exceptions.ChannelClosed:
            logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: ReadSqsThread
# -----------------------------------------------------------------------------


class ReadSqsThread(ReadThread):

    def __init__(self, config):
        super().__init__(config)
        self.queue_url = config.get("sqs_queue_url")
        self.sqs = boto3.client("sqs")

    def run(self):
        '''Process for reading lines from SQS and feeding them to a process_function() function.'''

        logging.info(message_info(129, threading.current_thread().name))

        # In a loop, get messages from AWS SQS.

        while True:

            # Get message from AWS SQS queue.

            sqs_response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=[],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[],
                VisibilityTimeout=30,
                WaitTimeSeconds=20
            )

            # If non-standard SQS output or empty messages, just loop.

            if sqs_response is None:
                continue
            sqs_messages = sqs_response.get("Messages", [])
            if not sqs_messages:
                logging.info(message_info(190, self.queue_url))
                continue

            # Construct and verify SQS message.

            sqs_message = sqs_messages[0]
            sqs_message_body = sqs_message.get("Body")
            sqs_message_receipt_handle = sqs_message.get("ReceiptHandle")

            # Write message to log.

            logging.info(message_info(101, sqs_message_body))

            # After successful import into Senzing, tell AWS SQS we're done with message.

            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=sqs_message_receipt_handle
            )

            # Count the processing

            self.config['counter_processed_messages'] += 1

# -----------------------------------------------------------------------------
# Class: MonitorThread
# -----------------------------------------------------------------------------


class MonitorThread(threading.Thread):

    def __init__(self, config, workers):
        threading.Thread.__init__(self)
        self.config = config
        self.workers = workers

    def run(self):
        '''Periodically monitor what is happening.'''

        last_processed_records = 0
        last_time = time.time()

        # Define monitoring report interval.

        sleep_time_in_seconds = self.config.get('monitoring_period_in_seconds')

        # Sleep-monitor loop.

        active_workers = len(self.workers)
        for worker in self.workers:
            if not worker.is_alive():
                active_workers -= 1

        while active_workers > 0:

            time.sleep(sleep_time_in_seconds)

            # Calculate active Threads.

            active_workers = len(self.workers)
            for worker in self.workers:
                if not worker.is_alive():
                    active_workers -= 1

            # Determine if we're running out of workers.

            if (active_workers / float(len(self.workers))) < 0.5:
                logging.warning(message_warning(721))

            # Calculate times.

            now = time.time()
            uptime = now - self.config.get('start_time', now)
            elapsed_time = now - last_time

            # Calculate rates.

            processed_messages_total = self.config['counter_processed_messages']
            processed_messages_interval = processed_messages_total - last_processed_records
            rate_processed_total = int(processed_messages_total / uptime)
            rate_processed_interval = int(processed_messages_interval / elapsed_time)

            # Construct and log monitor statistics.

            stats = {
                "processed_messages_interval": processed_messages_interval,
                "processed_messages_total": processed_messages_total,
                "rate_processed_interval": rate_processed_interval,
                "rate_processed_total": rate_processed_total,
                "uptime": int(uptime),
                "workers_total": len(self.workers),
                "workers_active": active_workers,
            }
            logging.info(message_info(127, json.dumps(stats, sort_keys=True)))

            # Store values for next iteration of loop.

            last_processed_records = processed_messages_total
            last_time = now

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)


def create_signal_handler_function(args):
    ''' Tricky code.  Uses currying technique. Create a function for signal handling.
        that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(298, args))
        sys.exit(0)

    return result_function


def delay(config):
    delay_in_seconds = config.get('delay_in_seconds')
    if delay_in_seconds > 0:
        logging.info(message_info(120, delay_in_seconds))
        time.sleep(delay_in_seconds)


def entry_template(config):
    ''' Format of entry message. '''
    debug = config.get("debug", False)
    config['start_time'] = time.time()
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(297, config_json)


def exit_template(config):
    ''' Format of exit message. '''
    debug = config.get("debug", False)
    stop_time = time.time()
    config['stop_time'] = stop_time
    config['elapsed_time'] = stop_time - config.get('start_time', stop_time)
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(298, config_json)


def exit_error(index, *args):
    ''' Log error message and exit program. '''
    logging.error(message_error(index, *args))
    logging.error(message_error(698))
    sys.exit(1)


def exit_silently():
    ''' Exit program. '''
    sys.exit(0)

# -----------------------------------------------------------------------------
# dohelper_* functions
# -----------------------------------------------------------------------------


def dohelper_thread_runner(args, threadClass):
    ''' Performs threadClass. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # If requested, delay start.

    delay(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')

    # Create queue reader threads for master process.

    threads = []
    for i in range(0, threads_per_process):
        thread = threadClass(config)
        thread.name = "{0}-0-thread-{1}".format(threadClass.__name__, i)
        threads.append(thread)

    # Create monitor thread for master process.

    adminThreads = []
    thread = MonitorThread(config, threads)
    thread.name = "{0}-0-thread-monitor".format(threadClass.__name__)
    adminThreads.append(thread)

    # Start threads for master process.

    for thread in threads:
        thread.start()

    # Sleep, if requested.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')
    if sleep_time_in_seconds > 0:
        logging.info(message_info(152, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


def do_docker_acceptance_test(args):
    ''' For use with Docker acceptance testing. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Epilog.

    logging.info(exit_template(config))


def do_kafka(args):
    ''' Read from Kafka. '''

    dohelper_thread_runner(args, ReadKafkaThread)


def do_rabbitmq(args):
    ''' Read from rabbitmq. '''

    dohelper_thread_runner(args, ReadRabbitMQThread)


def do_sleep(args):
    ''' Sleep.  Used for debugging. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')

    # Sleep

    if sleep_time_in_seconds > 0:
        logging.info(message_info(296, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    else:
        sleep_time_in_seconds = 3600
        while True:
            logging.info(message_info(295))
            time.sleep(sleep_time_in_seconds)

    # Epilog.

    logging.info(exit_template(config))


def do_sqs(args):
    ''' Read from AWS SQS. '''

    dohelper_thread_runner(args, ReadSqsThread)


def do_version(args):
    ''' Log version information. '''

    logging.info(message_info(294, __version__, __updated__))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)

    # Trap signals temporarily until args are parsed.

    signal.signal(signal.SIGTERM, bootstrap_signal_handler)
    signal.signal(signal.SIGINT, bootstrap_signal_handler)

    # Parse the command line arguments.

    subcommand = os.getenv("SENZING_SUBCOMMAND", None)
    parser = get_parser()
    if len(sys.argv) > 1:
        args = parser.parse_args()
        subcommand = args.subcommand
    elif subcommand:
        args = argparse.Namespace(subcommand=subcommand)
    else:
        parser.print_help()
        if len(os.getenv("SENZING_DOCKER_LAUNCHED", "")):
            subcommand = "sleep"
            args = argparse.Namespace(subcommand=subcommand)
            do_sleep(args)
        exit_silently()

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warning(message_warning(696, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)
