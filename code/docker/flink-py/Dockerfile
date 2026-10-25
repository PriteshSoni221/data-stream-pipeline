FROM flink:1.19.2-scala_2.12-java17

USER root

# Install Python tooling inside the Flink image so `flink run -py` works.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-dev python3-pip openjdk-17-jdk-headless build-essential curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && ln -sf /usr/bin/gcc /usr/bin/aarch64-linux-gnu-gcc \
    && JDK_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")" \
    && rm -rf /opt/java/openjdk \
    && ln -sf "$JDK_HOME" /opt/java/openjdk \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.19.2

# Kafka connector so PyFlink jobs can read from / write to Kafka topics.
# Version must match Flink 1.19 (artifact: flink-sql-connector-kafka-3.2.0-1.19).
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

USER flink
