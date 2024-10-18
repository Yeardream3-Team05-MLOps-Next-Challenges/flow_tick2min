FROM prefecthq/prefect:2.18.3-python3.10


ARG KAFKA_URL
ARG SPARK_URL
ARG TICK_TOPIC
ARG MIN_TOPIC

ENV KAFKA_URL ${KAFKA_URL}
ENV SPARK_URL ${SPARK_URL}
ENV TICK_TOPIC ${TICK_TOPIC}
ENV MIN_TOPIC ${MIN_TOPIC}

COPY pyproject.toml poetry.lock* ./

RUN apt-get update && apt-get install -y openjdk-17-jdk procps \
    && python -m pip install --upgrade pip\
    && pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "./flow.py"]