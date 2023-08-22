FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:flex_templates_base_image_release_20230213_RC00

COPY bigquery-jdbc /tmp/bigquery-jdbc

RUN set -x \
    && apt-get update \
    && apt-get install -y wget unzip \
    && cd /tmp/bigquery-jdbc \
    && wget https://storage.googleapis.com/simba-bq-release/jdbc/SimbaBigQueryJDBC42-1.3.2.1003.zip \
    && unzip SimbaBigQueryJDBC42-1.3.2.1003.zip -d bigquery-jdbc \
    && ./gradlew build \
    && mkdir -p /template/data-validation/libs \
    && mv build/libs/bigquery-jdbc.jar /template/data-validation/libs
