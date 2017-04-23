FROM jfloff/alpine-python:3.4-onbuild

COPY darknet /darknet

RUN cd /darknet && make

ENV DARKNET_CMD_TEMPLATE="detector test /darknet/cfg/coco.data /darknet/cfg/yolo.cfg /darknet/yolo.weights {file} -tagger-output"
ENV DARKNET_EXECUTABLE=/darknet/darknet
ENV DARKNET_WORKING_DIR=/darknet

COPY worker.py /worker.py
COPY requirements.txt /requirements.txt

CMD python worker.py
