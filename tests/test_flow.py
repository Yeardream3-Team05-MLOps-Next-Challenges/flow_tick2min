import pytest
from unittest import mock
from unittest.mock import MagicMock, PropertyMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_json, col, to_timestamp, concat, struct
from datetime import datetime, timedelta
import pytz
from src.logic import (
    create_spark_session_logic,
    read_stream_logic,
    add_candle_info_logic,
    aggregate_ohlc_logic,
    calculate_termination_time_logic
)

# Pytest 픽스처: 테스트 세션 동안 단일 Spark 세션을 생성하여 재사용
@pytest.fixture(scope="session")
def spark_session():
    """
    Spark 세션을 생성하여 모든 테스트에서 공유합니다.
    
    Returns:
        SparkSession: 생성된 Spark 세션 객체.
    """
    return create_spark_session_logic("local[*]")

def test_create_spark_session_logic():
    """
    create_spark_session_logic 함수가 올바르게 SparkSession을 생성하는지 테스트합니다.
    
    Assertions:
        - 반환된 객체가 SparkSession의 인스턴스인지 확인.
    """
    spark = create_spark_session_logic("local[*]")
    assert isinstance(spark, SparkSession)

def test_read_stream_logic(spark_session):
    """
    read_stream_logic 함수가 Kafka 스트림을 올바르게 읽고 처리하는지 테스트합니다.
    
    Args:
        spark_session (SparkSession): 테스트에 사용될 Spark 세션.
    
    Assertions:
        - 결과 DataFrame이 None이 아님을 확인.
        - 예상된 컬럼들이 DataFrame에 존재하는지 확인.
        - 데이터 값이 예상과 일치하는지 확인.
        - Kafka 설정이 올바르게 호출되었는지 확인.
    """
    # Kafka 메시지의 스키마 정의
    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("현재가", StringType()) \
        .add("현재시간", StringType()) \
        .add("날짜", StringType())

    kafka_url = "mock_kafka_url"  # 모의 Kafka URL
    tick_topic = "mock_tick_topic"  # 모의 Kafka 토픽

    # 기대하는 결과를 가지는 모의 DataFrame 생성
    mock_df = spark_session.createDataFrame(
        [("005930", "50000", "093000", "20231018")],
        ["종목코드", "현재가", "현재시간", "날짜"]
    )
    # timestamp와 price 컬럼 추가
    mock_df = mock_df \
        .withColumn("timestamp", to_timestamp(concat(col("날짜"), col("현재시간")), "yyyyMMddHHmmss")) \
        .withColumn("price", col("현재가").cast(DoubleType()))

    # DataStreamReader를 모킹하여 Kafka 스트림 대신 모의 DataFrame을 반환하도록 설정
    mock_stream_reader = MagicMock()
    mock_stream_reader.format.return_value = mock_stream_reader
    mock_stream_reader.option.return_value = mock_stream_reader
    mock_stream_reader.load.return_value = mock_df.select(to_json(struct(*mock_df.columns)).alias("value"))  # Kafka 형식으로 변환

    # SparkSession.readStream 프로퍼티를 모킹하여 모의 DataStreamReader를 반환하도록 설정
    with mock.patch('pyspark.sql.SparkSession.readStream', new_callable=PropertyMock) as mock_read_stream:
        mock_read_stream.return_value = mock_stream_reader
        
        # read_stream_logic 함수 실행
        result_df = read_stream_logic(spark_session, kafka_url, tick_topic, schema)
        
        # 결과 검증
        assert result_df is not None  # DataFrame이 None이 아님을 확인
        assert "종목코드" in result_df.columns  # '종목코드' 컬럼 존재 확인
        assert "timestamp" in result_df.columns  # 'timestamp' 컬럼 존재 확인
        assert "price" in result_df.columns  # 'price' 컬럼 존재 확인
        
        # DataFrame의 첫 번째 행 검증
        first_row = result_df.first()
        assert first_row["종목코드"] == "005930"  # '종목코드' 값 확인
        assert first_row["price"] == 50000.0  # 'price' 값 확인
        
        # Kafka 설정이 올바르게 호출되었는지 검증
        mock_stream_reader.format.assert_called_once_with("kafka")  # format("kafka")가 한 번 호출되었는지 확인
        mock_stream_reader.option.assert_any_call("kafka.bootstrap.servers", kafka_url)  # Kafka 서버 설정이 호출되었는지 확인
        mock_stream_reader.option.assert_any_call("subscribe", tick_topic)  # 구독 토픽 설정이 호출되었는지 확인
        mock_stream_reader.load.assert_called_once()  # load()가 한 번 호출되었는지 확인

def test_add_candle_info_logic(spark_session):
    """
    add_candle_info_logic 함수가 DataFrame에 'candle' 컬럼을 올바르게 추가하는지 테스트합니다.
    
    Args:
        spark_session (SparkSession): 테스트에 사용될 Spark 세션.
    
    Assertions:
        - 'candle' 컬럼이 DataFrame에 존재하는지 확인.
        - 'candle' 컬럼의 값이 "5m"으로 설정되었는지 확인.
    """
    # 입력 데이터 스키마 정의
    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("price", DoubleType()) \
        .add("timestamp", TimestampType())

    # 테스트 데이터 생성
    data = [Row(종목코드="005930", price=50000.0, timestamp=datetime.now())]
    df = spark_session.createDataFrame(data, schema)

    # add_candle_info_logic 함수 실행
    df_with_candle = add_candle_info_logic(df)

    # 'candle' 컬럼 검증
    assert "candle" in df_with_candle.columns  # 'candle' 컬럼 존재 확인
    # 'candle' 컬럼의 첫 번째 값이 "5m"인지 확인
    assert df_with_candle.select("candle").first()["candle"] == "5m"

def test_aggregate_ohlc_logic(spark_session):
    """
    aggregate_ohlc_logic 함수가 데이터를 OHLC(Open, High, Low, Close) 형식으로 올바르게 집계하는지 테스트합니다.
    
    Args:
        spark_session (SparkSession): 테스트에 사용될 Spark 세션.
    
    Assertions:
        - 'open', 'high', 'low', 'close' 컬럼이 집계 결과에 존재하는지 확인.
        - 집계된 결과가 예상한 행 수(여기서는 1개)인지 확인.
    """
    # 입력 데이터 스키마 정의
    schema = StructType() \
        .add("종목코드", StringType()) \
        .add("price", DoubleType()) \
        .add("timestamp", TimestampType())

    # 테스트 데이터 생성: 동일한 5분 윈도우 내의 두 가격 데이터
    data = [
        Row(종목코드="005930", price=50000.0, timestamp=datetime.now() - timedelta(minutes=1)),
        Row(종목코드="005930", price=50500.0, timestamp=datetime.now())
    ]
    df = spark_session.createDataFrame(data, schema)

    # aggregate_ohlc_logic 함수 실행
    ohlc_df = aggregate_ohlc_logic(df)

    # OHLC 컬럼 검증
    assert "open" in ohlc_df.columns  # 'open' 컬럼 존재 확인
    assert "high" in ohlc_df.columns  # 'high' 컬럼 존재 확인
    assert "low" in ohlc_df.columns  # 'low' 컬럼 존재 확인
    assert "close" in ohlc_df.columns  # 'close' 컬럼 존재 확인
    assert ohlc_df.count() == 1  # 집계 결과가 1개 행인지 확인

def test_calculate_termination_time_logic():
    """
    calculate_termination_time_logic 함수가 종료 시간까지 남은 시간을 올바르게 계산하는지 테스트합니다.
    
    Scenario:
        - 현재 시간을 19:00으로 고정하여 테스트.
        - 종료 시간이 20:00으로 설정되어 있으므로, 남은 시간이 3600초 이내인지 확인.
    
    Assertions:
        - termination_time이 0보다 큰지 확인.
        - termination_time이 3600초(1시간) 이하인지 확인.
    """
    kr_tz = pytz.timezone('Asia/Seoul')
    
    # 테스트 시간을 19:00으로 고정
    fixed_time = datetime(2024, 10, 19, 19, 0, 0, tzinfo=kr_tz)
    
    # `datetime.now`를 모킹하여 고정된 시간을 반환하도록 설정
    with mock.patch('src.logic.datetime') as mock_datetime:
        mock_datetime.now.return_value = fixed_time  # 현재 시간을 19:00으로 설정
        mock_datetime.timezone = pytz.timezone  # timezone 속성 모킹
        
        # calculate_termination_time_logic 함수 실행
        termination_time = calculate_termination_time_logic()
        
        # termination_time 검증: 0보다 크고 3600초(1시간) 이하인지 확인
        assert termination_time > 0  # 종료 시간이 미래임을 확인
        assert termination_time <= 3600  # 종료 시간이 1시간 이내임을 확인