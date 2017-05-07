import os
import sys
import uuid
import base64
import kafka
import logging
import binascii
import subprocess
import tempfile
import json
import urllib.request

log = logging.getLogger(__name__)

KAFKA_STATUS_STARTING = { "status": "CLASSIFICATION_STARTING" }
KAFKA_STATUS_FAILED_INVALID_MESSAGE = { "status": "CLASSIFICATION_FAILED_INVALID_INPUT" }
KAFKA_STATUS_FAILED_DARKNET_FAILED = { "status": "CLASSIFICATION_FAILED_DARKNET_FAILED" }
KAFKA_STATUS_FAILED_UNKNOWN = { "status": "CLASSIFICATION_FAILED_REASON_UNKNOWN" }

def setup_logging():
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)


def get_conf_from_env():
    defaults = {
        'KAFKA_BOOTSTRAP_SERVER': 'broker.kafka.svc.cluster.local', 
        'DARKNET_CMD_TEMPLATE': 'detector test cfg/yolo.cfg yolo.weights {file} -tagger-output', 
        'DARKNET_EXECUTABLE': '/darknet/darknet', 
        'KAFKA_INCOMING_TOPIC': 'incoming-pics', 
        'KAFKA_DESTINATION_TOPIC': 'predictions', 
        'KAFKA_CONSUMER_GROUP': 'tagger-workers', 
        'KAFKA_STATUS_TOPIC' : 'classification-status',
        'DARKNET_WORKING_DIR': '/darknet'
    }
    for key in defaults.keys():
        envvar = os.environ.get(key)
        if envvar:
            log.debug('Overriding Configuration: %s = %s', key, envvar)
            defaults.update({key: envvar})

    return defaults


def decode_incoming_message(raw_kafka_msg):
    r = json.loads(raw_kafka_msg)
    log.debug("Got Json From Kafka: %s", r)
    return r


def get_image(parsed_kafka_msg):
    cache = tempfile.NamedTemporaryFile(
        delete=False, suffix=".jpg", prefix="classify")
    req = urllib.request.urlopen(parsed_kafka_msg["url"])
    cache.write(req.read())
    cache.close()
    return cache


def classify(message, darknet_cwd, cmd, darknet_cmd):
    obj = decode_incoming_message(message.value.decode("utf-8"))
    filecache = get_image(obj)

    command = [darknet_cmd] + \
        cmd.format(file=os.path.abspath(filecache.name)).split()

    log.info('Classifying %s via Command: %s', filecache.name, command)

    process = subprocess.Popen(
        command, cwd=darknet_cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = process.communicate()

    log.info("Classification completed, deleting temporary file %s", filecache.name)
    filecache.close()

    log.debug('Stderr is %s', err)
    log.debug('Output is %s', out)
    if type(out) is bytes:
        out = out.decode('utf-8')
    log.debug('parsing command output into json')
    
    try:
        return json.loads(out)
    except ValueError:
        log.info(
            'Darknet returned invalid json, ignoring this for now.')
        message = KAFKA_STATUS_FAILED_DARKNET_FAILED.copy()
        message.update({ "stderr": err, "stdout": out })
        send_to_kafka(producer, status_topic, message.key, KAFKA_STATUS_FAILED_DARKNET_FAILED)
        raise


def send_to_kafka(producer, out_topic, key, results):
    log.info('Writing response with key %s into %s', key, out_topic)
    producer.send(out_topic, results, key=key)
    producer.flush()


if __name__ == '__main__':
    setup_logging()
    conf = get_conf_from_env()

    (server, in_topic, out_topic, status_topic, group, cmd, darknet_cwd, darknet_cmd) = (
        conf['KAFKA_BOOTSTRAP_SERVER'],
        conf['KAFKA_INCOMING_TOPIC'],
        conf['KAFKA_DESTINATION_TOPIC'],
        conf['KAFKA_STATUS_TOPIC'],
        conf['KAFKA_CONSUMER_GROUP'],
        conf['DARKNET_CMD_TEMPLATE'],
        conf['DARKNET_WORKING_DIR'],
        conf['DARKNET_EXECUTABLE'])

    log.info('Connecting to %s (topic: %s, group: %s)',
             server, in_topic, group)

    consumer = kafka.KafkaConsumer(
        in_topic, group_id=group, request_timeout_ms=120000, 
        session_timeout_ms=100000, bootstrap_servers=server, enable_auto_commit=False)

    producer = kafka.KafkaProducer(
        bootstrap_servers=server,
        value_serializer=lambda m: json.dumps(m).encode('ascii'))

    for topic_partition in consumer.assignment():
        offset = consumer.committed(kafka.TopicPartition(0, "incoming-pics"))
        position = consumer.position(kafka.TopicPartition(0, "incoming-pics"))
        log.info('Last commited offset for partition 0: %s; next position: %s', offset, position)

    log.info('Connected successfully.')

    for message in consumer:
        for topic_partition in consumer.assignment():
            offset = consumer.committed(topic_partition)
            position = consumer.position(topic_partition)
            log.info('Last commited offset for partition %s: %s; next position: %s',topic_partition, offset, position)

        log.info('Incoming Message from %s, offset %d, partition %d, checksum %s, key %s',
                 message.topic, message.offset, message.partition, message.checksum, message.key)
        log.debug('Partitions for topic: %s. Partitions assigned to me: %s',
                  repr(consumer.partitions_for_topic(in_topic)), repr(consumer.assignment()))
        if message.key is None:
            log.warn('Rejecting Message without key.')
            continue

        send_to_kafka(producer, status_topic, message.key, KAFKA_STATUS_STARTING)

        log.debug('Decoding Message')
        try:
            results = classify(message, darknet_cwd, cmd, darknet_cmd)
            send_to_kafka(producer, out_topic, message.key, results)
            consumer.commit()

        except binascii.Error:
            log.info('Received Invalid Message. Sending error to %s (key: %s)', status_topic, message.key)
            send_to_kafka(producer, status_topic, message.key, KAFKA_STATUS_FAILED_INVALID_MESSAGE)
        except ValueError:
            pass
        except Exception as e:
            send_to_kafka(producer, status_topic, message.key, KAFKA_STATUS_FAILED_UNKNOWN)
            log.info('Unhandled Exception: %s', e)
            consumer.close()
            sys.exit(1)
