from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, min, max, first, last, from_json, expr, to_timestamp, concat
from pyspark.sql.types import StructType, StringType, DoubleType

import os
import logging
from datetime import timedelta, datetime
import pytz

@task(name="Create Spark Session", cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def create_spark_session(spark_url):
    return SparkSession \
        .builder \
        .appName("tick_to_min") \
        .master(spark_url) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3') \
        .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint/tick_to_min') \
        .getOrCreate()

@task(name="Read Stream")
def read_stream(spark, kafka_url, tick_topic, schema):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", tick_topic) \
        .load()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    df = df.withColumn("timestamp", to_timestamp(concat(col("날짜"), col("현재시간")), "yyyyMMddHHmmss")) \
        .withColumn("price", col("현재가").cast(DoubleType()))

    df = df.withColumn("candle", '5m')

    return df

@task(name="Aggregate OHLC")
def aggregate_ohlc(df):
    return df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("종목코드")) \
        .agg(first("price").alias("open"),
             max("price").alias("high"),
             min("price").alias("low"),
             last("price").alias("close"))

@task(name="Stream to Console")
def stream_to_console(ohlc_df):
    return ohlc_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

@task(name="Stream to Kafka")
def stream_to_kafka(ohlc_df, kafka_url, min_topic):
    return ohlc_df \
        .selectExpr("CAST(window.start AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("topic", min_topic) \
        .start()

@task(name="Calculate Termination Time")
def calculate_termination_time():
    kr_tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(kr_tz)
    end_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
    if now >= end_time:
        end_time += timedelta(days=1)
    return (end_time - now).total_seconds()

@task(name="Await Termination")
def await_termination(spark, termination_time):
    spark.streams.awaitAnyTermination(timeout=int(termination_time))
    logging.info("Termination time reached or streaming stopped.")

@task(name="Stop Streaming")
def stop_streaming(spark):
    for query in spark.streams.active:
        query.stop()
    logging.info("All streaming queries stopped.")

@task(name="Stop Spark Session")
def stop_spark_session(spark):
    spark.stop()
    logging.info("Spark session stopped.")

@flow
def hun_tick2min_flow():

    #log_level = os.getenv('LOG_LEVEL', 'INFO')
    #logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = get_run_logger()

    spark_url = os.getenv('SPARK_URL', 'default_url')
    kafka_url = os.getenv('KAFKA_URL', 'default_url')
    tick_topic = os.getenv('TICK_TOPIC', 'default_tick')
    min_topic = os.getenv('MIN_TOPIC', 'default_min')


    logger.info(spark_url)
    logger.info(kafka_url)
    logger.info(tick_topic)
    logger.info(min_topic)

    spark = create_spark_session(spark_url)
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

    df_stream = read_stream(spark, kafka_url, tick_topic, schema)
    ohlc_df = aggregate_ohlc(df_stream)

    kafka_query = stream_to_kafka(ohlc_df, kafka_url, min_topic)
    console_query = stream_to_console(ohlc_df)

    termination_time = calculate_termination_time()
    
    # 종료 시간까지 대기
    await_termination(spark, termination_time)

    # 스트리밍 쿼리 종료
    stop_streaming(spark)

    # Spark 세션 종료
    stop_spark_session(spark)

    logging.info("Flow completed.")

if __name__ == "__main__":

    hun_tick2min_flow()