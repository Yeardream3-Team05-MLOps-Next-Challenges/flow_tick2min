import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.streaming import StreamingQuery
import os
import sys
import asyncio

from prefect import flow, task, get_run_logger

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flow import create_spark_session, read_stream, aggregate_ohlc, hun_tick2min_flow

# Mocking prefect logger
@pytest.fixture
def mock_logger():
    with patch('prefect.get_run_logger') as mock:
        yield mock

# Schema fixture
@pytest.fixture
def schema():
    return StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

# Mock SparkSession with proper streaming context
@pytest.fixture
def mock_spark():
    mock_session = MagicMock(spec=SparkSession)
    
    # Mock streaming context
    mock_streaming = MagicMock()
    mock_session.streams = mock_streaming
    mock_streaming.active = []
    mock_streaming.awaitAnyTermination = AsyncMock()
    
    # Mock readStream
    mock_read_stream = MagicMock()
    mock_session.readStream = mock_read_stream
    mock_read_stream.format.return_value.option.return_value.option.return_value.load.return_value = MagicMock()
    
    return mock_session

# Environment variables fixture
@pytest.fixture
def env_vars():
    test_env = {
        'SPARK_URL': 'spark://test:7077',
        'KAFKA_URL': 'kafka:9092',
        'TICK_TOPIC': 'test_tick',
        'MIN_TOPIC': 'test_min'
    }
    with patch.dict(os.environ, test_env):
        yield test_env

class TestSparkFlow:
    
    def test_create_spark_session(self):
        """Spark 세션 생성 테스트"""
        with patch('pyspark.sql.SparkSession.builder') as mock_builder:
            mock_session = MagicMock(spec=SparkSession)
            mock_builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_session
            
            from flow import create_spark_session
            spark = create_spark_session("spark://test:7077")
            
            assert isinstance(spark, SparkSession)
            mock_builder.appName.assert_called_with("tick_to_min")

    def test_read_stream(self, mock_spark, schema):
        """Kafka 스트림 읽기 테스트"""
        from flow import read_stream
        
        # Mock the DataFrame operations
        mock_df = MagicMock()
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.return_value = mock_df
        mock_df.selectExpr.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        
        result = read_stream(mock_spark, "kafka:9092", "test_topic", schema)
        
        assert result is not None
        mock_spark.readStream.format.assert_called_with("kafka")

    def test_aggregate_ohlc(self, mock_spark):
        """OHLC 집계 테스트"""
        from flow import aggregate_ohlc
        
        mock_df = MagicMock()
        mock_df.withWatermark.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        
        result = aggregate_ohlc(mock_df)
        
        assert result is not None
        mock_df.withWatermark.assert_called_once_with("timestamp", "10 seconds")

    @pytest.mark.asyncio
    async def test_hun_tick2min_flow(self, mock_spark, mock_logger, env_vars):
        """전체 Flow 테스트"""
        from flow import hun_tick2min_flow
        
        # Mock all the required components
        with patch('flow.create_spark_session', return_value=mock_spark), \
             patch('flow.read_stream', return_value=MagicMock()), \
             patch('flow.aggregate_ohlc', return_value=MagicMock()), \
             patch('flow.stream_to_kafka', return_value=MagicMock()), \
             patch('flow.stream_to_console', return_value=MagicMock()), \
             patch('flow.calculate_termination_time', return_value=1), \
             patch('flow.await_termination', new_callable=AsyncMock), \
             patch('flow.stop_streaming'), \
             patch('flow.stop_spark_session'):
            
            # Execute the flow
            await hun_tick2min_flow()
            
            # Verify logger calls
            mock_logger.return_value.info.assert_called()
            
            # Verify that streams.awaitAnyTermination was called
            mock_spark.streams.awaitAnyTermination.assert_called_once()

    def test_stream_to_kafka(self, mock_spark):
        """Kafka 스트리밍 출력 테스트"""
        from flow import stream_to_kafka
        
        mock_df = MagicMock()
        mock_df.selectExpr.return_value = mock_df
        mock_df.writeStream.return_value = mock_df
        mock_df.outputMode.return_value = mock_df
        mock_df.format.return_value = mock_df
        mock_df.option.return_value = mock_df
        mock_df.start.return_value = MagicMock(spec=StreamingQuery)
        
        result = stream_to_kafka(mock_df, "kafka:9092", "test_topic")
        
        assert result is not None
        mock_df.format.assert_called_with("kafka")

    def test_stream_to_console(self, mock_spark):
        """콘솔 스트리밍 출력 테스트"""
        from flow import stream_to_console
        
        mock_df = MagicMock()
        mock_df.writeStream.return_value = mock_df
        mock_df.outputMode.return_value = mock_df
        mock_df.format.return_value = mock_df
        mock_df.start.return_value = MagicMock(spec=StreamingQuery)
        
        result = stream_to_console(mock_df)
        
        assert result is not None
        mock_df.format.assert_called_with("console")