FROM prefecthq/prefect:2.18.3-python3.10

COPY requirements.txt .

RUN apt-get update && apt-get install -y openjdk-17-jdk procps \
    && python -m pip install --upgrade pip\
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

ENV PATH $JAVA_HOME/bin:$PATH

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "./flow.py"]