import os
import sys
import uuid
import base64
import kafka
import logging
import binascii
import subprocess
import json

#export DARKNET_CMD_TEMPLATE="detector test /home/svt/code/webeng-darknet/cfg/coco.data /home/svt/code/webeng-darknet/cfg/yolo.cfg /home/svt/code/webeng-darknet/yolo.weights {file} -tagger-output"
#export DARKNET_EXECUTABLE=/home/svt/code/webeng-darknet/darknet

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

log.addHandler(ch)

def get_conf_from_env():
    defaults = {
        'KAFKA_BOOTSTRAP_SERVER': 'broker.kafka.svc.cluster.local'
      , 'DARKNET_CMD_TEMPLATE' : 'detector test cfg/yolo.cfg yolo.weights {file} -tagger-output'
      , 'DARKNET_EXECUTABLE' : '/darknet/darknet'
      , 'KAFKA_INCOMING_TOPIC' : 'incoming-pics'
      , 'KAFKA_DESTINATION_TOPIC' : 'predictions'
      , 'KAFKA_CONSUMER_GROUP': 'tagger-workers'
      , 'DARKNET_WORKING_DIR' : '/home/svt/code/webeng-darknet'
    }
    for key in defaults.keys():
        envvar = os.environ.get(key)
        if envvar:
            log.debug('Overriding Configuration: %s = %s', key, envvar)
            defaults.update({key: envvar})
        
    return defaults

if __name__ == '__main__':
    conf = get_conf_from_env()
    (server, in_topic, out_topic, group, cmd, darknet_cwd, darknet_cmd) = (
        conf['KAFKA_BOOTSTRAP_SERVER'],
        conf['KAFKA_INCOMING_TOPIC'],
        conf['KAFKA_DESTINATION_TOPIC'],
        conf['KAFKA_CONSUMER_GROUP'],
        conf['DARKNET_CMD_TEMPLATE'],
        conf['DARKNET_WORKING_DIR'],
        conf['DARKNET_EXECUTABLE'])

    log.info('Connecting to %s (topic: %s, group: %s)', server, in_topic, group)
    consumer = kafka.KafkaConsumer(in_topic, group_id=group, bootstrap_servers=server)
    log.info('Connected.')


    for message in consumer:
        log.info('Incoming Message from %s, partition %d, checksum %s, key %s',
                 message.topic, message.partition, message.checksum, message.key)
        if message.key is None:
            log.warn('Rejecting Message without key.')
            continue

        log.debug('Decoding Message')
        try:
            blob = base64.b64decode(message.value, validate=True)
            filename = '{}.jpg'.format(uuid.uuid4())
            log.debug('Writing temporary file to %s', filename)
            filecache = open(filename, 'wb')
            filecache.write(blob)
            filecache.close()

            command = [darknet_cmd] + cmd.format(file=os.path.abspath(filecache.name)).split()

            log.info('Classifying %s via Command: %s', filename, command)

            process = subprocess.Popen(command, cwd=darknet_cwd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            
            out, _ = process.communicate()
            
            log.info("Classification completed, deleting temporary file %s", filename)
            os.unlink(filename)

            log.debug('Output is %s', out)
            if type(out) is bytes:
                out = out.decode('utf-8')
            log.info('Attempting to parse received output')
            parsed = json.loads(out)


            log.info('Setting up producer')
            producer = kafka.KafkaProducer(
                bootstrap_servers=server,
                value_serializer=lambda m: json.dumps(m).encode('ascii'))
            
            log.info('Writing response with key %s into %s', message.key, out_topic)
            producer.send(out_topic, parsed, key=message.key)
            producer.flush()

            log.info('All done.')


            
        except binascii.Error:
            log.info('Received Invalid Message, ignoring this for now.')
        except json.decoder.JSONDecodeError:
            log.info('Process supplied invalid return shit, ignoreing this for now.')
        except Exception as e:
            log.info('Unhandled Exception: %s', e)
            sys.exit(1)
