FROM python:3.11-slim

# Install system dependencies: procps (for ps command needed by Spark), JRE, and curl
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
        procps \
        default-jre-headless \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install PySpark 3.5.1, Delta Lake 3.2.0, and data engineering ecosystem
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    boto3==1.34.131 \
    pyarrow==15.0.2 \
    pandas==2.2.2 \
    requests==2.32.3 \
    pytest==8.2.2

# PySpark install location
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Pre-download Delta Lake and Hadoop AWS S3A connector jars directly into PySpark's jars folder.
# Spark 3.5.1 runs Hadoop 3.3.4; aws-java-sdk-bundle 1.12.262 is the companion SDK.
ADD https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars/

RUN chmod 644 ${SPARK_HOME}/jars/delta-spark_2.12-3.2.0.jar \
    ${SPARK_HOME}/jars/delta-storage-3.2.0.jar \
    ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
    ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/spark/work-dir
