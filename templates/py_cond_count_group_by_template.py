#!/usr/bin/python3

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from log_formatter import build_log_parser
from functools import reduce
import dateutil.parser as date_parser

log_format = """

        %s

    """
log_parser = build_log_parser(log_format)


def extract_log_list_from_result_iterable(log_res_it):
    return [l for l in log_res_it]


def extract_log_list_from_result_iterable_list(log_res_it_list):
    log_list = []
    for log_res_it in log_res_it_list:
        log_list.extend(extract_log_list_from_result_iterable(log_res_it))
    return log_list


def update_function(new_values, current):
    if current is None:
        current = []
    if len(current) >= %s:
        current.clear()
    log_list = extract_log_list_from_result_iterable_list(new_values)
    current.extend(log_list)
    return current


def process_rdd_element(time, rdd_element):
    log_group_key = rdd_element[0]
    log_list = rdd_element[1]
    # check if we have more then specified number of logs
    if len(log_list) >= %s:
        print("Alarm fired at {0} by these group: {1} which counts {2} logs: {3}".format(time, log_group_key,
                                                                                         len(log_list), log_list))


def process_rdd(time, rdd):
    # we could send message to server to store this specific log
    rdd.foreach(lambda el: process_rdd_element(time, el))


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

                                %s

                                )

    pairs = filtered_logs.map(lambda l: ("%s".format(%s), l))
    groups = pairs.groupByKey()
    aggregated = groups.updateStateByKey(update_function)
    aggregated.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
