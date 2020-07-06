#!/bin/sh

KAFKA_PATH=~/kafka_2.12-2.5.0
TOP_TAGS_PORT=9998
TREND_TAGS_PORT=9999


nc -lk $TOP_TAGS_PORT &     # for top tags stream
nc -lk $TREND_TAGS_PORT &   # for trend tags stream

$KAFKA_PATH/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic posts --from-beginning | nc 127.0.0.1 $TOP_TAGS_PORT &

$KAFKA_PATH/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic posts --from-beginning | nc 127.0.0.1 $TREND_TAGS_PORT &
