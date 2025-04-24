# src/logic.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import window, col, min, max, first, last, from_json, expr, to_timestamp, concat, lit
from pyspark.sql.types import StructType, StringType, DoubleType
from datetime import timedelta, datetime
import pytz

def create_spark_session_logic(spark_url: str) -> SparkSession:
    """
    Spark 세션을 생성하는 순수 함수.
    
    Args:
        spark_url (str): Spark 클러스터의 URL.
    
    Returns:
        SparkSession: 생성된 Spark 세션 객체.
    """
    return SparkSession \
        .builder \
        .appName("tick_to_min") \
        .master(spark_url) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3') \
        .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint/tick_to_min') \
        .getOrCreate()

def read_stream_logic(spark: SparkSession, kafka_url: str, tick_topic: str, schema: StructType) -> DataFrame:
    """
    Kafka로부터 스트림을 읽어오는 순수 함수.
    
    Args:
        spark (SparkSession): Spark 세션 객체.
        kafka_url (str): Kafka 클러스터의 URL.
        tick_topic (str): 구독할 Kafka 토픽.
        schema (StructType): Kafka 메시지의 스키마.
    
    Returns:
        DataFrame: 변환된 Spark 데이터프레임.
    """
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", tick_topic) \
        .load()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    df = df.withColumn("timestamp", to_timestamp(col("체결일시"), "yyyyMMddHHmmss")) \
        .withColumn("price", col("현재가").cast(DoubleType()))

    return df

def add_candle_info_logic(df: DataFrame) -> DataFrame:
    """
    데이터프레임에 'candle' 컬럼을 추가하는 순수 함수.
    
    Args:
        df (DataFrame): 입력 데이터프레임.
    
    Returns:
        DataFrame: 'candle' 컬럼이 추가된 데이터프레임.
    """
    return df.withColumn("candle", lit("5m"))

def aggregate_ohlc_logic(df: DataFrame) -> DataFrame:
    """
    OHLC (Open, High, Low, Close)를 집계하는 순수 함수.
    
    Args:
        df (DataFrame): 입력 데이터프레임.
    
    Returns:
        DataFrame: OHLC 집계 결과가 포함된 데이터프레임.
    """
    return df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("종목코드")) \
        .agg(
            first("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            last("price").alias("close")
        )

def calculate_termination_time_logic(current_time: datetime = None, tz_str: str = 'Asia/Seoul') -> float:
    """
    종료 시간을 계산하는 순수 함수.
    
    Args:
        current_time (datetime, optional): 현재 시간. 지정하지 않으면 현재 시스템 시간을 사용.
        tz_str (str, optional): 시간대 문자열. 기본값은 'Asia/Seoul'.
    
    Returns:
        float: 종료 시간까지의 초 단위 남은 시간.
    """
    kr_tz = pytz.timezone(tz_str)
    if current_time is None:
        now = datetime.now(kr_tz)
    else:
        now = current_time.astimezone(kr_tz)
    end_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
    if now >= end_time:
        end_time += timedelta(days=1)
    return (end_time - now).total_seconds()
