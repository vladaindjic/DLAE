# System for realtime logs and alarms analytics

## Realtime logs and alarms analytics
An average informational system is usually built of lots of workstations and couple of servers.
If the machines are connected to the internet, then they represent the potential threat for hacker attacks.
In order to prevent and detect security breaches in informational system,
 each workstation and server generate tons of logs.
These logs should be processed in realtime, so that reaction to threats could be made on time.

This project propose a system that can be used for realtime logs monitoring and alarms detection.
Two main parts of this system represents:
- `Apache Kafka` - collects logs from workstations and servers. For testing purpose, 
workstations and servers are mocked by `log_generator.py` script, which periodically
sends logs to `Kafka` instance.  
- `Apache Spark` - subscribes to `Kafka` broker and receives logs in order they have been stored in `Kafka`.
The purpose of `Spark` is to select logs of interests, that represent tries of security breaches.


Two main problems with systems for realtime logs and alarms analytics are:
- how to easily write parsers for different types of log's formats
- how to easily specify the criteria when a log or a few of them should indicate
the threat and fire alarm to system administrator.

This project gives the answer to the above questions by introducing two
domain-specific languages.

## DSL for log format definition
The example below shows the usage of a DSL that is used for writing log formats 
and automatically generating parser for the specified format:

```
## log format specification that corresponds to this log line
## <11>1 2019-04-08T01:08:12+02:00 12.12.12.1 FakeWebApp - msg77 - from:192.52.223.99 "GET /recipe HTTP/1.0" 200 4923 "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_12_6) AppleWebKit/5361 (KHTML, like Gecko) Chrome/53.0.892.0 Safari/5361 "

# declaration section (optional)
priority        := int;
version         := int(/\d/);
timestamp       := datetime(/\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}\+\d{2}\:\d{2}/);
_ws             := /\s+/;
server_id       := string(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/);
app_name        := string(/\w+/);
_dash           := /\s+\-\s+/;
msg_id          := string(/msg\d+/);
workstation_id  := string(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/);

# log format definition section (mandatory)        
/</ priority />/ version _ws timestamp _ws server_id _ws app_name _dash msg_id _dash _from:=/from:/ workstation_id _ws msg:=string(/.*/)

```

Each log format specification contains these sections:
- optional declaration section (lines 5-13) - can contain declarations of log elements and separators
- mandatory log format definition section (line 16) - can contain regular expressions, log element and separator 
identifiers, log element and separator declarations. Separator identifiers can be used multiple times, but log element 
identifiers and log element declarations must be unique.

By using `parglare` `python` library, format specification will be parsed and the parser for specified log format 
will be generated. For the example above, 
generated parser will parse log line and create python object of `Log class`:

```python
class Log:
    def __init__(self):
        self.priority = 11
        self.version = 1
        self.timestamp = datetime.datetime(2019, 4, 8, 1, 8, 12)
        self.server_id = "12.12.12.1"
        self.app_name = "FakeWebApp"
        self.msg_id = "msg77"
        self.workstation_id = "192.52.223.99"
        self.msg = '""GET /recipe HTTP/1.0" 200 4923 "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_12_6) AppleWebKit/5361 (KHTML, like Gecko) Chrome/53.0.892.0 Safari/5361 ""'


```
Note that all separators are ignored.

## DSL for alarm criteria definition
Assume that we have logs which format are specified in previous section. Then, we can easily specify when received
logs should fire alarms by using DSL for alarm criteria definition.

```
version == 1 and (priority > 10 and priority <= 14) or not timestamp@#2018#; count(10, groupBy=[server_id, workstation_id], last=1m30s)
```

The alarm criteria definition is made of two parts:
- mandatory condition part - says which characteristics each log has to have in order to be selected by `Spark`.
- optional count part - says how many logs with specified characteristics should be accumulated in order to fire log.
Optionally, logs can be grouped by one or more attributes and accumulated only if they appear in specific time offset.

In the example above, alarm will be fired only if 10 logs with the same combination of values of attributes `server_id`
 and `workstation_id` are generated in (last) minute and half. Each log should have one of these characteristic:
- attribute `version` is equal to one and the value of `priority` attribute must be in range `(10, 14]`
- date attribute `timestamp` should not have happened in 2018 year.  

## Example of using the whole system

```shell
# On your local machine install python3, pip33 and virtualenv. 
# Activare virtualenv and then run
pip install -r requirements.txt
# On your local machine Run Kafka and Spark containers
docker-compose up

# attach to kafka container by runnig this command
docker exec -it kafka /bin/bash
# create topic for logs
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic logs
# to check if topic is successfully created run
kafka-topics.sh --list --zookeeper zookeeper:2181
# exit from kafka container
exit

# attach to spark-master container
docker exec -it spark-master /bin/bash
# create directory logs and step into it
mkdir logs && cd logs
# run following commangs to install unecessery dependencies for running spark-streming
apt install -y python3
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py
rm get-pip.py
pip3 install requests
pip3 install requests_oauthlib
pip3 install parglare
pip3 install python-dateutil
export PYSPARK_PYTHON=python3

# On your local machine open generate_spark_code.py
# Write log format definition in log_format_str variable.
# Write alarm definition in alarm_str variable.
# (You could copy log format and alarm definitions from previous two sections.
# Note that lines which start with # must be removed.)
# Run next command to generate pyspark module
python generate_spark_code.py
# copy generated module to spark-master container by running following command
./copy-data.sh

# ################## Localhost
# On your local machine, run kafka producer and log_generator.py script
python producer.py
python log_generator.py


# return back to spark-master container and run next command to start spark streaming
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 py_cond_count_last_group_by_generated.py kafka:9092 logs
# Alarms would be print in terminal.

```

