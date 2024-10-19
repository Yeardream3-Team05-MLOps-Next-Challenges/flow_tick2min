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

# Prefect test harness fixture
@pytest.fixture(autouse=True)
def prefect_test():
    with prefect_test_harness():
        yield

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
    mock_streams = MagicMock()
    mock_session.streams = mock_streams
    mock_streams.active = []
    
    # Mock readStream
    mock_read_stream = MagicMock()
    mock_session.readStream = mock_read_stream
    mock_df = MagicMock()
    mock_read_stream.format.return_value.option.return_value.option.return_value.load.return_value = mock_df
    mock_df.selectExpr.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    
    return mock_session

# Mock Logger fixture
@pytest.fixture
def mock_logger():
    with patch('prefect.get_run_logger') as mock:
        mock_logger = MagicMock()
        mock.return_value = mock_logger
        yield mock_logger

@pytest.mark.asyncio
class TestSparkFlow:
    
    async def test_create_spark_session(self):
        """Spark 세션 생성 테스트"""
        with patch('pyspark.sql.SparkSession.builder') as mock_builder:
            mock_session = MagicMock(spec=SparkSession)
            mock_builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_session
            
            from flow import create_spark_session
            spark = await create_spark_session("spark://test:7077")
            
            assert isinstance(spark, SparkSession)
            mock_builder.appName.assert_called_with("tick_to_min")

    async def test_read_stream(self, mock_spark, schema):
        """Kafka 스트림 읽기 테스트"""
        from flow import read_stream
        
        mock_df = MagicMock()
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.return_value = mock_df
        mock_df.selectExpr.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        
        result = await read_stream.fn(mock_spark, "kafka:9092", "test_topic", schema)
        
        assert result is not None
        mock_spark.readStream.format.assert_called_with("kafka")

    async def test_aggregate_ohlc(self, mock_spark):
        """OHLC 집계 테스트"""
        from flow import aggregate_ohlc
        
        mock_df = MagicMock()
        mock_df.withWatermark.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        
        result = await aggregate_ohlc.fn(mock_df)
        
        assert result is not None
        mock_df.withWatermark.assert_called_once_with("timestamp", "10 seconds")

    async def test_hun_tick2min_flow(self, mock_spark, mock_logger):
        """전체 Flow 테스트"""
        from flow import hun_tick2min_flow
        
        test_env = {
            'SPARK_URL': 'spark://test:7077',
            'KAFKA_URL': 'kafka:9092',
            'TICK_TOPIC': 'test_tick',
            'MIN_TOPIC': 'test_min'
        }
        
        # Mock all dependencies
        with patch.dict(os.environ, test_env), \
             patch('flow.create_spark_session', return_value=mock_spark), \
             patch('flow.read_stream.fn', new_callable=AsyncMock, return_value=MagicMock()), \
             patch('flow.aggregate_ohlc.fn', new_callable=AsyncMock, return_value=MagicMock()), \
             patch('flow.stream_to_kafka.fn', new_callable=AsyncMock, return_value=MagicMock()), \
             patch('flow.stream_to_console.fn', new_callable=AsyncMock, return_value=MagicMock()), \
             patch('flow.calculate_termination_time.fn', new_callable=AsyncMock, return_value=1), \
             patch('flow.await_termination.fn', new_callable=AsyncMock), \
             patch('flow.stop_streaming.fn', new_callable=AsyncMock), \
             patch('flow.stop_spark_session.fn', new_callable=AsyncMock):
            
            # Execute flow
            await hun_tick2min_flow()
            
            # Verify logger calls
            mock_logger.info.assert_called()

    async def test_stream_to_kafka(self, mock_spark):
        """Kafka 스트리밍 출력 테스트"""
        from flow import stream_to_kafka
        
        mock_df = MagicMock()
        mock_df.selectExpr.return_value = mock_df
        mock_df.writeStream.return_value = mock_df
        mock_df.outputMode.return_value = mock_df
        mock_df.format.return_value = mock_df
        mock_df.option.return_value = mock_df
        mock_df.start.return_value = MagicMock(spec=StreamingQuery)
        
        result = await stream_to_kafka.fn(mock_df, "kafka:9092", "test_topic")
        
        assert result is not None
        mock_df.format.assert_called_with("kafka")

    async def test_stream_to_console(self, mock_spark):
        """콘솔 스트리밍 출력 테스트"""
        from flow import stream_to_console
        
        mock_df = MagicMock()
        mock_df.writeStream.return_value = mock_df
        mock_df.outputMode.return_value = mock_df
        mock_df.format.return_value = mock_df
        mock_df.start.return_value = MagicMock(spec=StreamingQuery)
        
        result = await stream_to_console.fn(mock_df)
        
        assert result is not None
        mock_df.format.assert_called_with("console")

# conftest.py에 추가할 내용
def pytest_configure(config):
    """Optional: configure pytest."""
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )