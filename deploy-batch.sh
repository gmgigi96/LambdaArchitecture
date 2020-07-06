#!/bin/sh

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

### start mongo db server
sudo systemctl start mongod


sudo amazon-linux-extras install epel
sudo yum install p7zip -y

hdfs dfs -mkdir /user/batch
hdfs dfs -touch /user/batch/posts.xml
7za x -so stackoverflow.com-Posts.7z | hdfs dfs -appendToFile - /user/batch/posts.xml