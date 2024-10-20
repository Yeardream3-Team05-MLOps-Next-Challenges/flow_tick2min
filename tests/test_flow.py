import pytest
from unittest import mock
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
import pytz
from src.logic import (
    create_spark_session_logic,
    read_stream_logic,
    add_candle_info_logic,
    aggregate_ohlc_logic,
    calculate_termination_time_logic
)

@pytest.fixture(scope="session")
def spark_session():
    return create_spark_session_logic("local[*]")

def test_create_spark_session_logic():
    spark = create_spark_session_logic("local[*]")
    assert isinstance(spark, SparkSession)

def test_read_stream_logic(spark_session):
    # 스키마 정의
    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

    # 테스트 데이터와 설정
    kafka_url = "mock_kafka_url"
    tick_topic = "mock_tick_topic"

    # 중간 DataFrame들 생성
    raw_kafka_df = spark_session.createDataFrame(
        [("{'종목코드':'005930','현재가':'50000','현재시간':'093000','날짜':'20231018'}",)],
        ["value"]
    )
    
    parsed_df = spark_session.createDataFrame(
        [("005930", "50000", "093000", "20231018")],
        ["종목코드", "현재가", "현재시간", "날짜"]
    )

    final_df = parsed_df \
        .withColumn("timestamp", to_timestamp(concat(col("날짜"), col("현재시간")), "yyyyMMddHHmmss")) \
        .withColumn("price", col("현재가").cast(DoubleType()))

    # Mock the stream reader
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = raw_kafka_df

    # Patch both readStream and the necessary transformations
    with mock.patch.object(spark_session, 'readStream', new_callable=mock.PropertyMock) as mock_read_stream:
        mock_read_stream.return_value = mock_reader
        
        # Execute the function
        result_df = read_stream_logic(spark_session, kafka_url, tick_topic, schema)

        # Verify the basic structure
        assert result_df is not None
        assert "종목코드" in result_df.columns
        assert "timestamp" in result_df.columns
        assert "price" in result_df.columns

        # Verify the Kafka configuration
        mock_reader.format.assert_called_once_with("kafka")
        mock_reader.option.assert_any_call("kafka.bootstrap.servers", kafka_url)
        mock_reader.option.assert_any_call("subscribe", tick_topic)
        mock_reader.load.assert_called_once()

        # Check the first row values (if needed)
        first_row = result_df.first()
        assert first_row is not None
        assert first_row["종목코드"] == "005930"
        assert first_row["price"] == 50000.0

def test_add_candle_info_logic(spark_session):
    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("price", DoubleType()) \
        .add("timestamp", TimestampType())

    data = [Row(종목코드="005930", price=50000.0, timestamp=datetime.now())]
    df = spark_session.createDataFrame(data, schema)

    df_with_candle = add_candle_info_logic(df)

    assert "candle" in df_with_candle.columns
    assert df_with_candle.select("candle").first()["candle"] == "5m"

def test_aggregate_ohlc_logic(spark_session):
    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("price", DoubleType()) \
        .add("timestamp", TimestampType())

    data = [
        Row(종목코드="005930", price=50000.0, timestamp=datetime.now() - timedelta(minutes=1)),
        Row(종목코드="005930", price=50500.0, timestamp=datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)

    ohlc_df = aggregate_ohlc_logic(df)

    assert "open" in ohlc_df.columns
    assert "high" in ohlc_df.columns
    assert "low" in ohlc_df.columns
    assert "close" in ohlc_df.columns
    assert ohlc_df.count() == 1

def test_calculate_termination_time_logic():
    kr_tz = pytz.timezone('Asia/Seoul')
    
    # 테스트 시간을 19:00로 고정
    fixed_time = datetime(2024, 10, 19, 19, 0, 0, tzinfo=kr_tz)
    
    # `datetime.now`를 mock 처리하여 고정된 시간을 반환하도록 설정
    with mock.patch('src.logic.datetime') as mock_datetime:
        mock_datetime.now.return_value = fixed_time
        mock_datetime.timezone = pytz.timezone
        
        termination_time = calculate_termination_time_logic()
        
        # 1시간 이내에 종료가 계산되는지 확인 (19시에서 20시까지 남은 시간)
        assert termination_time > 0
        assert termination_time <= 3600  # 최대 1시간