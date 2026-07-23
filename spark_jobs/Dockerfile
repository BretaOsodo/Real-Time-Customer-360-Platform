FROM apache/spark:4.1.1

USER root

RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install \
    boto3 \
    psycopg2-binary \
    python-dotenv \
    requests \
    pytest

ENV SPARK_JARS_DIR=/opt/spark/jars

RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o ${SPARK_JARS_DIR}/postgresql.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o ${SPARK_JARS_DIR}/hadoop-aws.jar

RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_JARS_DIR}/aws-java-sdk.jar

USER spark

WORKDIR /opt/project

COPY . .