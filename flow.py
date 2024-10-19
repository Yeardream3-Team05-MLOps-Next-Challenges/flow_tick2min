from prefect import flow, task, get_run_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import os
import logging

# src 폴더의 logic.py에서 함수 임포트
from src.logic import (
    create_spark_session_logic,
    read_stream_logic,
    add_candle_info_logic,
    aggregate_ohlc_logic,
    calculate_termination_time_logic
)

@task(name="Create Spark Session")
def create_spark_session(spark_url: str) -> SparkSession:
    """
    Spark 세션을 생성하는 Prefect 태스크.
    
    Args:
        spark_url (str): Spark 클러스터의 URL.
    
    Returns:
        SparkSession: 생성된 Spark 세션 객체.
    """
    return create_spark_session_logic(spark_url)

@task(name="Read Stream")
def read_stream(spark: SparkSession, kafka_url: str, tick_topic: str, schema: StructType) -> DataFrame:
    """
    Kafka로부터 스트림을 읽어오는 Prefect 태스크.
    
    Args:
        spark (SparkSession): Spark 세션 객체.
        kafka_url (str): Kafka 클러스터의 URL.
        tick_topic (str): 구독할 Kafka 토픽.
        schema (StructType): Kafka 메시지의 스키마.
    
    Returns:
        DataFrame: 변환된 Spark 데이터프레임.
    """
    return read_stream_logic(spark, kafka_url, tick_topic, schema)

@task(name="Add Candle Info")
def add_candle_info(df: DataFrame) -> DataFrame:
    """
    데이터프레임에 'candle' 컬럼을 추가하는 Prefect 태스크.
    
    Args:
        df (DataFrame): 입력 데이터프레임.
    
    Returns:
        DataFrame: 'candle' 컬럼이 추가된 데이터프레임.
    """
    return add_candle_info_logic(df)

@task(name="Aggregate OHLC")
def aggregate_ohlc(df: DataFrame) -> DataFrame:
    """
    OHLC를 집계하는 Prefect 태스크.
    
    Args:
        df (DataFrame): 입력 데이터프레임.
    
    Returns:
        DataFrame: OHLC 집계 결과가 포함된 데이터프레임.
    """
    return aggregate_ohlc_logic(df)

@task(name="Calculate Termination Time")
def calculate_termination_time() -> float:
    """
    종료 시간을 계산하는 Prefect 태스크.
    
    Returns:
        float: 종료 시간까지의 초 단위 남은 시간.
    """
    return calculate_termination_time_logic()

@task(name="Stream to Console")
def stream_to_console(ohlc_df: DataFrame):
    """
    OHLC 데이터를 콘솔로 출력하는 Prefect 태스크.
    
    Args:
        ohlc_df (DataFrame): OHLC 집계 데이터프레임.
    """
    ohlc_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

@task(name="Stream to Kafka")
def stream_to_kafka(ohlc_df: DataFrame, kafka_url: str, min_topic: str):
    """
    OHLC 데이터를 Kafka로 스트리밍하는 Prefect 태스크.
    
    Args:
        ohlc_df (DataFrame): OHLC 집계 데이터프레임.
        kafka_url (str): Kafka 클러스터의 URL.
        min_topic (str): 출력할 Kafka 토픽.
    """
    ohlc_df \
        .selectExpr("CAST(window.start AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("topic", min_topic) \
        .start()

@task(name="Await Termination")
def await_termination(spark: SparkSession, termination_time: float):
    """
    스트리밍 종료를 대기하는 Prefect 태스크.
    
    Args:
        spark (SparkSession): Spark 세션 객체.
        termination_time (float): 종료 시간까지의 초 단위 남은 시간.
    """
    spark.streams.awaitAnyTermination(timeout=int(termination_time))
    logging.info("Termination time reached or streaming stopped.")

@task(name="Stop Streaming")
def stop_streaming(spark: SparkSession):
    """
    모든 스트리밍 쿼리를 중지하는 Prefect 태스크.
    
    Args:
        spark (SparkSession): Spark 세션 객체.
    """
    for query in spark.streams.active:
        query.stop()
    logging.info("All streaming queries stopped.")

@task(name="Stop Spark Session")
def stop_spark_session(spark: SparkSession):
    """
    Spark 세션을 종료하는 Prefect 태스크.
    
    Args:
        spark (SparkSession): Spark 세션 객체.
    """
    spark.stop()
    logging.info("Spark session stopped.")

@flow
def hun_tick2min_flow():
    """
    전체 데이터 처리 플로우를 정의하는 Prefect 플로우.
    """
    logger = get_run_logger()

    spark_url = os.getenv('SPARK_URL', 'default_url')
    kafka_url = os.getenv('KAFKA_URL', 'default_url')
    tick_topic = os.getenv('TICK_TOPIC', 'default_tick')
    min_topic = os.getenv('MIN_TOPIC', 'default_min')

    logger.info(f"Spark URL: {spark_url}")
    logger.info(f"Kafka URL: {kafka_url}")
    logger.info(f"Tick Topic: {tick_topic}")
    logger.info(f"Min Topic: {min_topic}")

    spark = create_spark_session(spark_url)

    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

    df_stream = read_stream(spark, kafka_url, tick_topic, schema)
    ohlc_df = aggregate_ohlc(df_stream)
    ohlc_df_with_candle = add_candle_info(ohlc_df)

    kafka_query = stream_to_kafka(ohlc_df_with_candle, kafka_url, min_topic)
    console_query = stream_to_console(ohlc_df_with_candle)

    termination_time = calculate_termination_time()

    # 종료 시간까지 대기
    await_termination(spark, termination_time)

    # 스트리밍 쿼리 종료
    stop_streaming(spark)

    # Spark 세션 종료
    stop_spark_session(spark)

    logger.info("Flow completed.")

if __name__ == "__main__":
    hun_tick2min_flow()