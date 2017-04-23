# Tagger Worker
This python script will connect to Kafka, digest incoming darknet classification tasks from an input topic, and put the results into an output topic.

## Build Container
```bash
$ git clone https://github.com/SwaggerTagger/DarknetKafkaWorker
$ cd DarknetKafkaWorker
$ git clone https://github.com/SwaggerTagger/darknet
$ wget http://pjreddie.com/media/files/yolo.weights -O darknet/yolo.weights
$ docker build -t tagger-worker:latest .
```
