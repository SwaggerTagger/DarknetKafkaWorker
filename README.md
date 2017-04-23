# Tagger Worker
This python script will connect to Kafka, digest incoming darknet classification tasks from an input topic, and put the results into an output topic.

## Configuration
Configuration works via the following Environment Variables:

Environment Variable | Default Value 
--- | ---
KAFKA_BOOTSTRAP_SERVER | broker.kafka.svc.cluster.local
DARKNET_CMD_TEMPLATE | detector test cfg/yolo.cfg yolo.weights {file} -tagger-output
DARKNET_EXECUTABLE | /darknet/darknet
KAFKA_INCOMING_TOPIC | incoming-pics
KAFKA_DESTINATION_TOPIC | predictions
KAFKA_CONSUMER_GROUP | tagger-workers
DARKNET_WORKING_DIR | /home/svt/code/webeng-darknet

## Build Container
```bash
$ git clone https://github.com/SwaggerTagger/DarknetKafkaWorker
$ cd DarknetKafkaWorker
$ git clone https://github.com/SwaggerTagger/darknet
$ wget http://pjreddie.com/media/files/yolo.weights -O darknet/yolo.weights
$ docker build -t tagger-worker:latest .
```
