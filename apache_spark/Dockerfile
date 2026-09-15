FROM apache/spark:3.5.6-python3

USER root

# Python dependencies used by the pipeline
RUN pip install --no-cache-dir \
    boto3 \
    py4j

# Add Hadoop AWS support for Amazon S3
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/

# Install AWS CLI v2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
        -o "/tmp/awscliv2.zip" && \
    unzip -q /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /tmp/aws /tmp/awscliv2.zip && \
    apt-get remove -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV SPARK_HOME=/opt/spark
ENV PATH="/usr/local/bin:${SPARK_HOME}/bin:${PATH}"

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-*.zip:${PYTHONPATH}"

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["bash"]