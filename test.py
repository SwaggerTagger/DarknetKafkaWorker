import kafka
import sys
import base64
import logging
logging.basicConfig(level=logging.INFO)
print('connecting')
prod = kafka.KafkaProducer(bootstrap_servers="broker.kafka.svc.cluster.local")
print('connected')

f = sys.argv[1]

msg = base64.b64encode(open(f, 'rb').read())

future = prod.send('incoming-pics', msg, key=b"fuck-you")

try:
    metadata = future.get(timeout=10)
except kafka.errors.KafkaError:
    log.exception()


prod.flush()