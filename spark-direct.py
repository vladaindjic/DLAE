#!/usr/bin/python

import sys

import requests
from pyspark import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# # create spark configuration
# conf = SparkConf()
# conf.setAppName("TwitterStreamApp")
# # create spark instance with the above configuration
# sc = SparkContext(conf=conf)
# sc.setLogLevel("ERROR")
# # create the Streaming Context from the above spark context with window size 2 seconds
# ssc = StreamingContext(sc, 2)
# # setting a checkpoint to allow RDD recovery
# ssc.checkpoint("checkpoint_TwitterApp")
# # read data from port 9009
# dataStream = ssc.socketTextStream("localhost", 9009)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag.encode('utf-8')) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    print(top_tags)
    print(tags_count)
    # initialize and send the data through REST API
    url = 'http://hashtags_dash_app:50001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        if row_rdd.count() == 0:
            return
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = \
            sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        # send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("checkpoint_PythonStreamingDirectKafkaWordCount")
    brokers, topic = sys.argv[1:]
    print("*********************")
    print(brokers)
    print(topic)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    # counts = lines.map(lambda x: x)
    # counts.pprint()
    # ssc.start()
    # ssc.awaitTermination()

    # split each tweet into words
    words = lines.flatMap(lambda line: line.split(" "))
    # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
    hashtags = words.filter(lambda w: '<' in w and '>' in w).map(lambda x: (x, 1))
    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
    # tags_totals.pprint()
    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()

# pokretanje spark streaminga
# $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 spark-direct.py kafka:9092 test
# srediti spark-master dockerfile da ima odgovarajuce instalacije
