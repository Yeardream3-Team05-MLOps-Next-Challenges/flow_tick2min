import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flow import create_spark_session, read_stream, aggregate_ohlc, hun_tick2min_flow


@pytest.fixture
def mock_spark():
    with patch('pyspark.sql.SparkSession') as mock_spark:
        mock_session = MagicMock(spec=SparkSession)
        mock_spark.builder.getOrCreate.return_value = mock_session
        yield mock_session

@pytest.fixture
def schema():
    return StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

class TestSparkFlow:
    def test_create_spark_session(self, mock_spark):
        """Spark 세션 생성 테스트"""
        spark_url = "spark://localhost:7077"
        spark = create_spark_session(spark_url)
        assert spark is not None
        assert isinstance(spark, MagicMock)  # SparkSession이 mock인지 확인

    def test_read_stream(self, mock_spark, schema):
        """Kafka 스트림 읽기 테스트"""
        mock_read_stream = MagicMock()
        mock_spark.readStream.return_value = mock_read_stream
        df = read_stream(mock_spark, "localhost:9092", "tick_topic", schema)
        assert df is not None
        mock_spark.readStream.assert_called_once()  # mock 호출 확인

    def test_aggregate_ohlc(self):
        """OHLC 집계 테스트"""
        mock_df = MagicMock()
        mock_df.withWatermark.return_value = mock_df
        result = aggregate_ohlc(mock_df)
        assert result is not None
        mock_df.withWatermark.assert_called_once_with("timestamp", "10 seconds")

    @pytest.mark.integration
    def test_end_to_end(self):
        """전체 흐름에 대한 통합 테스트"""
        pass  # 실제 Spark/Kafka 테스트를 원할 때 구현