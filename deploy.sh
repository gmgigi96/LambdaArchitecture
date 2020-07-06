#!/bin/sh

### Download kafka
wget https://downloads.apache.org/kafka/2.5.0/kafka_2.12-2.5.0.tgz
tar -xzf kafka_2.12-2.5.0.tgz
rm -rf kafka_2.12-2.5.0.tgz

### Download dataset
wget https://ia800107.us.archive.org/27/items/stackexchange/stackoverflow.com-Posts.7z

### Preparazione python spark
/usr/bin/pip3 install --user pypandoc
/usr/bin/pip3 install --user pyspark
/usr/bin/pip3 install --user pymongo

### Installazione mongodb
sudo mv config/mongodb-org-4.2.repo /etc/yum.repos.d/mongodb-org-4.2.repo
sudo yum install -y mongodb-org
sudo mv config/mongodb.conf /etc/mongodb.conf

### Installazione tmux
sudo yum install -y tmux

### Installazione pydoop
sudo yum install -y python3-devel
export JAVA_HOME=/etc/alternatives/java_sdk
/usr/bin/pip3 install --user pydoop
#export HADOOP_CONF_DIR=/etc/hadoop/conf

### Creazione topic posts
cd kafka_2.12-2.5.0
nohup bin/kafka-server-start.sh config/server.properties &
sleep 10
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic posts --partitions 2 --replication-factor 1
cd ..

### start mongo db server
sudo systemctl start mongod

### Crea gli stream per lo speed layer
src/create_streams.sh &
