# Tagger Worker
This python script will connect to Kafka, digest incoming darknet classification tasks from an input topic, and put the results into an output topic.

## Configuration
Configuration works via the following Environment Variables:

Environment Variable | Default Value 
--- | ---
`KAFKA_BOOTSTRAP_SERVER` | `broker.kafka.svc.cluster.local`
`DARKNET_CMD_TEMPLATE` | `detector test cfg/yolo.cfg yolo.weights {file} -tagger-output`
`DARKNET_EXECUTABLE` | `/darknet/darknet`
`KAFKA_INCOMING_TOPIC` | `incoming-pics`
`KAFKA_DESTINATION_TOPIC` | `predictions`
`KAFKA_CONSUMER_GROUP` | `tagger-workers`
`DARKNET_WORKING_DIR` | `/darknet`

## Interface
### Incoming Messages
The Worker expects incoming messages in `KAFKA_INCOMING_TOPIC` to have a non-null keyId and have the following format:

```json
{
  "url": "https://..../.../.jpg"
}
```
Every additional Json Key Value Pair is ignored.

### Outgoing Messages
The Worker will put classification results into `KAFKA_DESTINATION_TOPIC` with the same keyId as the incoming message and the following json format:
```json
{
  "count": 7,
  "input": "/tmp/classifyq2yg7ey1.jpg",
  "matches": [
    {
      "left": 17,
      "right": 32,
      "top": 301,
      "class": "person",
      "probability": 0.346982,
      "bottom": 333
    },
    {
      "left": 215,
      "right": 229,
      "top": 288,
      "class": "person",
      "probability": 0.288302,
      "bottom": 334
    },
    {
      "left": 276,
      "right": 290,
      "top": 289,
      "class": "person",
      "probability": 0.342672,
      "bottom": 341
    },
    {
      "left": 238,
      "right": 267,
      "top": 291,
      "class": "person",
      "probability": 0.292135,
      "bottom": 353
    }
  ],
  "time": 38.847912
}

```

## Build Container
```bash
$ git clone https://github.com/SwaggerTagger/DarknetKafkaWorker
$ cd DarknetKafkaWorker
$ git clone https://github.com/SwaggerTagger/darknet
$ wget http://pjreddie.com/media/files/yolo.weights -O darknet/yolo.weights
$ docker build -t tagger-worker:latest .
```
