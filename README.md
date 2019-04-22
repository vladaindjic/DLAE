
```shell
# ################spark-master>


apt install python3
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py
rm get-pip.py
pip3 install requests
pip3 install requests_oauthlib
pip3 install parglare
pip3 install python-dateutil
export PYSPARK_PYTHON=python3

#pip3 install dateutil 
# ################# kafka

# create topic logs
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic logs
# list topics
kafka-topics.sh --list --zookeeper zookeeper:2181

# ################## Localhost
# start producer 
python producer.py

# start log generator
python log_generator



# ##############kafka
# to check if everything works fine, start kafka consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logs --from-beginning


# ####################### spark-master>
# run spark consumer
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 spark-direct.py kafka:9092 logs


# Maybe we should consider how to use dataframes in streaming.
# If we do that, than watermarking could be apply, so we don't lose data that comes lately.
```