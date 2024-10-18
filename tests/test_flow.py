import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flow import create_spark_session, read_stream, aggregate_ohlc, hun_tick2min_flow

# Import your flow module
from flow import (
    create_spark_session,
    read_stream,
    aggregate_ohlc,
    stream_to_kafka,
    stream_to_console,
    hun_tick2min_flow
)

@pytest.fixture
def schema():
    return StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

@pytest.fixture
def mock_spark():
    with patch('pyspark.sql.SparkSession') as mock:
        mock_session = MagicMock(spec=SparkSession)
        mock.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_session
        yield mock_session

class TestSparkFlow:
    def test_create_spark_session(self, mock_spark):
        """Test creation of Spark session"""
        spark_url = "spark://localhost:7077"
        
        with patch('pyspark.sql.SparkSession.builder') as mock_builder:
            mock_builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
            
            spark = create_spark_session(spark_url)
            assert spark is not None
            assert isinstance(spark, MagicMock)

    def test_read_stream(self, mock_spark, schema):
        """Test Kafka stream reading"""
        # Mock the stream reading chain
        mock_read_stream = MagicMock()
        mock_spark.readStream.return_value = mock_read_stream
        mock_read_stream.format.return_value.option.return_value.option.return_value.load.return_value = mock_read_stream
        mock_read_stream.selectExpr.return_value = mock_read_stream
        mock_read_stream.select.return_value = mock_read_stream
        mock_read_stream.withColumn.return_value = mock_read_stream

        kafka_url = "localhost:9092"
        tick_topic = "tick_topic"
        
        df = read_stream(mock_spark, kafka_url, tick_topic, schema)
        
        assert df is not None
        mock_spark.readStream.assert_called_once()
        
    def test_aggregate_ohlc(self):
        """Test OHLC aggregation"""
        mock_df = MagicMock()
        mock_df.withWatermark.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        
        result = aggregate_ohlc(mock_df)
        
        assert result is not None
        mock_df.withWatermark.assert_called_once_with("timestamp", "10 seconds")

    @patch('flow.create_spark_session')
    @patch('flow.read_stream')
    @patch('flow.aggregate_ohlc')
    @patch('flow.stream_to_kafka')
    @patch('flow.stream_to_console')
    @patch('flow.calculate_termination_time')
    @patch('flow.await_termination')
    @patch('flow.stop_streaming')
    @patch('flow.stop_spark_session')
    def test_hun_tick2min_flow(
        self,
        mock_create_spark,
        mock_read_stream,
        mock_aggregate_ohlc,
        mock_stream_kafka,
        mock_stream_console,
        mock_calculate_termination,
        mock_await_termination,
        mock_stop_streaming,
        mock_stop_spark
    ):
        """Test the entire flow"""
        # Setup mocks
        mock_spark = MagicMock(spec=SparkSession)
        mock_create_spark.return_value = mock_spark
        mock_df = MagicMock()
        mock_read_stream.return_value = mock_df
        mock_aggregate_ohlc.return_value = mock_df
        mock_stream_query = MagicMock(spec=StreamingQuery)
        mock_stream_kafka.return_value = mock_stream_query
        mock_stream_console.return_value = mock_stream_query
        mock_calculate_termination.return_value = 1000

        # Set environment variables
        test_env = {
            'SPARK_URL': 'spark://test:7077',
            'KAFKA_URL': 'kafka:9092',
            'TICK_TOPIC': 'test_tick',
            'MIN_TOPIC': 'test_min'
        }
        
        with patch.dict(os.environ, test_env):
            hun_tick2min_flow()

        # Verify all main components were called
        mock_create_spark.assert_called_once()
        mock_read_stream.assert_called_once()
        mock_aggregate_ohlc.assert_called_once()
        mock_stream_kafka.assert_called_once()
        mock_stream_console.assert_called_once()
        mock_calculate_termination.assert_called_once()
        mock_await_termination.assert_called_once()
        mock_stop_streaming.assert_called_once()
        mock_stop_spark.assert_called_once()

# Optionally add integration tests if needed
@pytest.mark.integration
class TestSparkFlowIntegration:
    def test_end_to_end(self):
        """Integration test for the entire flow"""
        # This would be implemented if you want to test with actual Spark/Kafka
        pass