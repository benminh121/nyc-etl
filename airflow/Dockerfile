FROM apache/airflow:2.10.5

USER root

# Install Java
RUN apt-get update && apt-get install -y openjdk-17-jdk

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
ENV MINIO_ENDPOINT  host.docker.internal:9000
ENV MINIO_ACCESS_KEY benminh1201
ENV MINIO_SECRET_KEY benminh1201

RUN export JAVA_HOME

RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y libgomp1 

USER airflow

COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache -r /requirements.txt