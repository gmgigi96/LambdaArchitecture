#!/bin/sh -x

if [ $# -ne 3 ]; then
    echo "Usage: start-simulation <sleep time posts> <n posts> <batch time>"
    exit -1
fi

export HADOOP_CONF_DIR=/etc/hadoop/conf

KAFKA_PATH=~/kafka_2.12-2.5.0
IP=`ip a show dev eth0 | awk '/inet /{print $2}' | sed 's/\/[0-9]*//'`

SLEEP_TIME=$1
POSTS=$2
BATCH=$3

#7za t -so stackoverflow.com-Posts.7z | python src/take.py $SLEEP_TIME $POSTS | bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic posts &
cat posts.xml | /usr/bin/python3 src/take.py $SLEEP_TIME $POSTS | $KAFKA_PATH/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic posts &
sleep 2

# append to hdfs
hdfs dfs -mkdir -p /user/batch

$KAFKA_PATH/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic posts --from-beginning | nc -lk 9990 &
sleep 2
nc 127.0.0.1 9990 | /usr/bin/python3 src/fill_hdfs.py /user/batch/posts.xml $POSTS &
sleep 10


# start batch layer
echo "*/$BATCH * * * * /bin/sh /home/hadoop/src/start_batch.sh" | sudo tee /var/spool/cron/hadoop &> /dev/null


# start speed layer
tmux new -s trend_tags -d "spark-submit src/trend_tags.py localhost 9999" &

tmux new -s top_tags -d "spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.2 src/top_tags.py localhost 9998 $IP" &