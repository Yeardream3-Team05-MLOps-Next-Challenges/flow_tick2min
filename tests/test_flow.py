import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType
from datetime import datetime, timedelta
import pytz
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.logic import (
    create_spark_session_logic,
    read_stream_logic,
    add_candle_info_logic,
    aggregate_ohlc_logic,
    calculate_termination_time_logic
)

# Schema fixture
@pytest.fixture
def schema():
    return StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

# Mock SparkSession fixture
@pytest.fixture
def mock_spark():
    mock_session = MagicMock(spec=SparkSession)
    
    # Mock DataStreamReader
    mock_stream_reader = MagicMock()
    mock_session.readStream.return_value = mock_stream_reader
    
    # Mock the format, option, and load chain
    mock_stream_reader_format = MagicMock()
    mock_stream_reader_option1 = MagicMock()
    mock_stream_reader_option2 = MagicMock()
    mock_stream_reader_load = MagicMock()
    
    mock_stream_reader.format.return_value = mock_stream_reader_format
    mock_stream_reader_format.option.return_value = mock_stream_reader_option1
    mock_stream_reader_option1.option.return_value = mock_stream_reader_option2
    mock_stream_reader_option2.load.return_value = MagicMock(spec=DataFrame)
    
    return mock_session

class TestLogicFunctions:
    
    @patch('src.logic.SparkSession.builder')
    def test_create_spark_session_logic(self, mock_builder):
        """Spark 세션 생성 로직 테스트"""
        spark_url = "spark://test:7077"
        mock_session = MagicMock(spec=SparkSession)
        # Set up the builder chain
        mock_builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_session
        
        spark = create_spark_session_logic(spark_url)
        
        assert spark == mock_session
        mock_builder.appName.assert_called_with("tick_to_min")
        mock_builder.appName.return_value.master.assert_called_with(spark_url)
        mock_builder.appName.return_value.master.return_value.config.assert_any_call('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3')
        mock_builder.appName.return_value.master.return_value.config.assert_any_call('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint/tick_to_min')
    
    @patch('src.logic.lit')
    def test_add_candle_info_logic(self, mock_lit):
        """candle 컬럼 추가 로직 테스트"""
        # Set up the lit mock to return a mock value
        mock_lit.return_value = "mock_lit_value"
        
        mock_df = MagicMock(spec=DataFrame)
        mock_df.withColumn.return_value = mock_df
        
        result = add_candle_info_logic(mock_df)
        
        # 'lit' 함수 is called with "5m"
        mock_lit.assert_called_with("5m")
        mock_df.withColumn.assert_called_with("candle", "mock_lit_value")
        assert result == mock_df
    
    @patch('src.logic.window')
    @patch('src.logic.col')
    def test_aggregate_ohlc_logic(self, mock_col, mock_window):
        """OHLC 집계 로직 테스트"""
        # Set up the window and col mocks
        mock_window.return_value = "mock_window"
        mock_col.return_value = "mock_col"
        
        mock_df = MagicMock(spec=DataFrame)
        mock_df.withWatermark.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        
        result = aggregate_ohlc_logic(mock_df)
        
        mock_df.withWatermark.assert_called_once_with("timestamp", "10 seconds")
        mock_window.assert_called_with(col("timestamp"), "5 minutes")
        mock_col.assert_any_call("종목코드")
        mock_df.groupBy.assert_called_once_with("mock_window", "mock_col")
        mock_df.agg.assert_called_once()
        assert result == mock_df
    
    @patch('src.logic.datetime')
    def test_calculate_termination_time_logic_before_end(self, mock_datetime):
        """종료 시간 계산 로직 테스트 (현재 시간이 오후 8시 이전)"""
        kr_tz = pytz.timezone('Asia/Seoul')
        mock_now = kr_tz.localize(datetime(2024, 10, 19, 19, 30, 0))
        
        # Mock datetime.now() to return mock_now
        mock_datetime.now.return_value = mock_now
        
        termination_seconds = calculate_termination_time_logic(current_time=mock_now)
        
        expected_seconds = (mock_now.replace(hour=20, minute=0, second=0, microsecond=0) - mock_now).total_seconds()
        assert termination_seconds == expected_seconds
    
    @patch('src.logic.datetime')
    def test_calculate_termination_time_logic_after_end(self, mock_datetime):
        """종료 시간 계산 로직 테스트 (현재 시간이 오후 8시 이후)"""
        kr_tz = pytz.timezone('Asia/Seoul')
        mock_now = kr_tz.localize(datetime(2024, 10, 19, 20, 30, 0))
        
        # Mock datetime.now() to return mock_now
        mock_datetime.now.return_value = mock_now
        
        termination_seconds = calculate_termination_time_logic(current_time=mock_now)
        
        expected_end_time = mock_now.replace(hour=20, minute=0, second=0, microsecond=0) + timedelta(days=1)
        expected_seconds = (expected_end_time - mock_now).total_seconds()
        assert termination_seconds == expected_seconds
    
    @patch('src.logic.to_timestamp')
    @patch('src.logic.concat')
    @patch('src.logic.from_json')
    def test_read_stream_logic(self, mock_from_json, mock_concat, mock_to_timestamp, mock_spark, schema):
        """Kafka 스트림 읽기 로직 테스트"""
        # Mock the functions to return mock objects or simple values
        mock_concat.return_value = "mock_concat"
        mock_to_timestamp.return_value = "mock_timestamp"
        mock_from_json.return_value = "mock_from_json"
        
        # Mock the DataFrame returned by load()
        mock_stream_reader = mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.return_value
        mock_stream_reader.selectExpr.return_value = MagicMock(spec=DataFrame)
        mock_stream_reader.selectExpr.return_value.select.return_value = MagicMock(spec=DataFrame)
        mock_stream_reader.selectExpr.return_value.select.return_value.withColumn.return_value = MagicMock(spec=DataFrame)
        mock_stream_reader.selectExpr.return_value.select.return_value.withColumn.return_value.withColumn.return_value = MagicMock(spec=DataFrame)
        
        # Call the function
        result = read_stream_logic(mock_spark, "kafka:9092", "test_topic", schema)
        
        # Assertions
        mock_spark.readStream.format.assert_called_with("kafka")
        mock_spark.readStream.format.return_value.option.assert_any_call("kafka.bootstrap.servers", "kafka:9092")
        mock_spark.readStream.format.return_value.option.return_value.option.assert_any_call("subscribe", "test_topic")
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.assert_called_once()
        
        mock_from_json.assert_called_with(col("value"), schema)
        mock_concat.assert_called_with(col("날짜"), col("현재시간"))
        mock_to_timestamp.assert_called_with("mock_concat", "yyyyMMddHHmmss")
        
        # Check withColumn calls
        mock_stream_reader.selectExpr.return_value.select.return_value.withColumn.assert_any_call("timestamp", "mock_timestamp")
        mock_stream_reader.selectExpr.return_value.select.return_value.withColumn.return_value.withColumn.assert_any_call("price", ANY)
        
        assert result == mock_stream_reader.selectExpr.return_value.select.return_value.withColumn.return_value.withColumn.return_value