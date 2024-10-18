import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flow import create_spark_session, read_stream, aggregate_ohlc, hun_tick2min_flow

# Spark 세션 테스트
@patch('flow.SparkSession.builder.getOrCreate')
def test_create_spark_session(mock_spark):
    mock_spark.return_value = MagicMock(SparkSession)
    spark_url = "spark://localhost:7077"
    
    spark = create_spark_session(spark_url)
    mock_spark.assert_called_once()
    assert spark is not None

# Kafka 스트리밍 테스트
@patch('flow.SparkSession.readStream')
def test_read_stream(mock_read_stream):
    mock_df = MagicMock()
    mock_read_stream.return_value = mock_df

    spark = MagicMock(SparkSession)
    kafka_url = "localhost:9092"
    tick_topic = "tick_topic"

    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

    df_stream = read_stream(spark, kafka_url, tick_topic, schema)
    assert df_stream is not None
    mock_read_stream.assert_called_once()

# Flow 테스트
@patch('flow.create_spark_session')
@patch('flow.read_stream')
@patch('flow.aggregate_ohlc')
@patch('flow.add_candle_info')
@patch('flow.stream_to_kafka')
@patch('flow.stream_to_console')
@patch('flow.calculate_termination_time')
@patch('flow.await_termination')
@patch('flow.stop_streaming')
@patch('flow.stop_spark_session')
def test_hun_tick2min_flow(mock_create_spark, mock_read_stream, mock_aggregate_ohlc, 
                           mock_add_candle, mock_stream_kafka, mock_stream_console, 
                           mock_calculate_termination, mock_await_termination,
                           mock_stop_streaming, mock_stop_spark):

    mock_create_spark.return_value = MagicMock(SparkSession)
    mock_read_stream.return_value = MagicMock()
    mock_aggregate_ohlc.return_value = MagicMock()
    mock_add_candle.return_value = MagicMock()
    mock_stream_kafka.return_value = MagicMock()
    mock_stream_console.return_value = MagicMock()
    mock_calculate_termination.return_value = 1000

    hun_tick2min_flow()

    mock_create_spark.assert_called_once()
    mock_read_stream.assert_called_once()
    mock_aggregate_ohlc.assert_called_once()
    mock_add_candle.assert_called_once()
    mock_stream_kafka.assert_called_once()
    mock_stream_console.assert_called_once()
    mock_calculate_termination.assert_called_once()
    mock_await_termination.assert_called_once()
    mock_stop_streaming.assert_called_once()
    mock_stop_spark.assert_called_once()
