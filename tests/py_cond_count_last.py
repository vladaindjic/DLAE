#!/usr/bin/python3

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from log_formatter import build_log_parser

log_format = """

        brojka:=int;
        </</> <brojka> </>/> </.*/>

    """
log_parser = build_log_parser(log_format)


def process_rdd(time, rdd):
    print("SISI KARINUUUUUUUUUUUUUUUUUUUUUUUU %s" % rdd.count())
    # we could send message to server to store this specific log
    if rdd.count() >= 11:
        log_list = rdd.collect()
        print("Alarm fired at {0} by these {1} logs: {2}".format(time, len(log_list), log_list))


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaAlarmLog")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint_PythonStreamingDirectKafkaAlarmLog")
    brokers, topic = sys.argv[1:]
    print("********** brokers: {0} \t topic: {1} **********".format(brokers, topic))
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])

    # new version
    logs = lines.map(lambda log_line: log_parser.parse_log(log_line.strip()))
    filtered_logs = logs.filter(lambda l:

                                l.brojka == 11 or l.brojka == 13

                                )
    window_logs = logs.window(10, 1)
    window_logs.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
