FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest

# DVT

RUN /bin/bash -c 'set -x \
    && cd /opt \
    && apt-get update \
    && apt-get install -y git python3 python3-venv \
    && mkdir -p ~/.ssh \
    && ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts \
    && git clone https://github.com/GoogleCloudPlatform/professional-services-data-validator.git \
    && cd professional-services-data-validator \
    && python3 -m venv env \
    && source env/bin/activate \
    && python3 -m pip install .'

# BQ JDBC

COPY bigquery-jdbc /tmp/bigquery-jdbc

RUN set -x \
    && apt-get install wget unzip \
    && cd /tmp/bigquery-jdbc \
    && wget https://storage.googleapis.com/simba-bq-release/jdbc/SimbaBigQueryJDBC42-1.3.2.1003.zip \
    && unzip SimbaBigQueryJDBC42-1.3.2.1003.zip -d bigquery-jdbc \
    && ./gradlew build \
    && mkdir -p /template/data-validation/libs \
    && mv build/libs/bigquery-jdbc.jar /template/data-validation/libs
