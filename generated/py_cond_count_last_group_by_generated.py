#!/usr/bin/python3

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from log_formatter import build_log_parser
import dateutil.parser as date_parser

log_format = """

        
    priority        := int;
    version         := int(/\d/);
    timestamp       := datetime(/\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}\+\d{2}\:\d{2}/);
    _ws             := /\s+/;
    server_id       := string(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/);
    app_name        := string(/\w+/);
    _dash           := /\s+\-\s+/;
    msg_id          := string(/msg\d+/);
    workstation_id  := string(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/);
    
    /</ priority />/ version _ws timestamp _ws server_id _ws app_name _dash msg_id _dash _from:=/from:/ workstation_id _ws msg:=string(/.*/)



    """
log_parser = build_log_parser(log_format)


def extract_log_list_from_result_iterable(log_res_it):
    return [l for l in log_res_it]


def process_rdd_element(rdd_el, time):
    window_group_key, log_res_it = rdd_el
    log_list = extract_log_list_from_result_iterable(log_res_it)
    if len(log_list) >= 10:
        print("Alarm fired at {0} by these group {1} which counts {2} logs: {3}".format(time, window_group_key,
                                                                                        len(log_list), log_list))


def process_rdd(time, rdd):
    rdd.foreach(lambda rdd_el: process_rdd_element(rdd_el, time))


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

                                ((l.version == 1 and (l.priority > 10 and l.priority <= 14)) or (l.timestamp < date_parser.parse("2018-01-01 00:00:00") or l.timestamp >= date_parser.parse("2019-01-01 00:00:00")))

                                )

    pairs = filtered_logs.map(lambda l: ("{0}_{1}".format(l.server_id, l.workstation_id), l))
    window_groups = pairs.groupByKeyAndWindow(90, 1)
    window_groups.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
