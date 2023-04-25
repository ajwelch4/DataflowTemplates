# JDBC to BigQuery +

- [Auth](#Auth)
- [Infrastructure](#Infrastructure)
- [Stage](#Stage)
- [Execute](#Execute)
- [Load Test](#Load-Test)
- [TODO](#TODO)

## Auth

```shell
gcloud auth login
gcloud auth application-default login
gcloud config set project pipelines-377521
```

## Infrastructure

The infrastructure required by this template can be deployed via
[this](https://github.com/ajwelch4/DataflowTemplatesOps/tree/v2-jdbc-plus/v2/jdbc-to-bigquery-plus)
Terraform repo.

## Stage

Adjust the parameters to the commands below according to your own environment
and run from the root of the repo:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am

mvn clean package -pl v2/jdbc-to-bigquery-plus -am \
    -PtemplatesStage \
    -DskipShade \
    -DskipTests \
    -DprojectId="pipelines-377521" \
    -Dregion="us-east1" \
    -Dartifactregion="us-east1" \
    -DbucketName="pipelines-377521-dataflow-flex-template" \
    -DstagePrefix="jdbc-to-bigquery-plus" \
    -DtemplateName="JDBC_To_BigQuery_Plus"
```

## Config


```shell
gcloud sql connect source-postgres-instance --user=admin_user --database=foo # Password: 49adf11bb6cdcc954cb4bab0dc03726957b6791e
```

Postgres:

```sql
CREATE SCHEMA bar;
SET search_path TO bar,public;

GRANT USAGE ON SCHEMA bar to "read_only_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA bar
   GRANT SELECT ON TABLES TO "read_only_user";

CREATE TABLE customers
(
    customer_id integer                  NOT NULL PRIMARY KEY,
    first_name  varchar(30)              NOT NULL,
    last_name   varchar(30)              NOT NULL,
    created_ts  timestamp with time zone NOT NULL,
    updated_ts  timestamp with time zone,
    deleted_ts  timestamp with time zone
);
CREATE
INDEX customers_updated ON customers  (updated_ts);

CREATE TABLE orders
(
    order_id    integer                  NOT NULL PRIMARY KEY,
    customer_id integer                  NOT NULL,
    status      varchar(12)              NOT NULL,
    created_ts  timestamp with time zone NOT NULL,
    updated_ts  timestamp with time zone,
    deleted_ts  timestamp with time zone,
    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_id)
            REFERENCES customers (customer_id)
);
CREATE
INDEX orders_updated ON orders  (updated_ts);


-- First batch of data.
INSERT INTO customers (customer_id, first_name, last_name, created_ts, updated_ts, deleted_ts)
VALUES
(1, 'Lilyana', 'Browning','2023-01-09 21:14:29','2023-01-09 21:14:29', NULL),
(2, 'Carter', 'Holt','2023-01-09 22:33:47','2023-01-09 22:33:47', NULL),
(3, 'Darien', 'Becker','2023-01-10 00:07:17','2023-01-10 00:07:17', NULL);

INSERT INTO orders (order_id, customer_id, status, created_ts, updated_ts, deleted_ts)
VALUES
(1, 1, 'SHIPPED','2023-01-09 22:13:19','2023-01-09 22:13:19', NULL),
(2, 1, 'ORDERED','2023-01-09 23:54:39','2023-01-09 23:54:39', NULL),
(3, 2, 'SHIPPED','2023-01-09 23:37:15','2023-01-09 23:37:15', NULL),
(4, 3, 'SHIPPED','2023-01-10 00:08:16','2023-01-10 00:08:16', NULL),
(5, 3, 'ORDERED','2023-01-10 00:09:47','2023-01-10 00:09:47', NULL),
(6, 3, 'ORDERED','2023-01-10 00:10:40','2023-01-10 00:10:40', NULL);
```

```jsonl
{"source_schema": "bar", "source_table": "customers", "source_columns": ["first_name", "customer_id", "last_name", "created_ts", "deleted_ts"], "source_watermark_column": null, "source_partition_column": "customer_id", "source_partition_count": 3, "target_project": "pipelines-377521", "target_dataset": "foo_bar", "target_create_disposition": "CREATE_NEVER", "target_write_disposition": "WRITE_APPEND"}
{"source_schema": "bar", "source_table": "orders", "source_columns": null, "source_watermark_column": null, "source_partition_column": null, "source_partition_count": null, "target_project": "pipelines-377521", "target_dataset": "foo_bar", "target_create_disposition": "CREATE_NEVER", "target_write_disposition": "WRITE_APPEND"}
```

## Execute

Adjust the parameters to the command below according to your own environment:

```shell
gcloud dataflow flex-template run "jdbc-to-bigquery-plus-$(date +%s)" \
  --region="us-east1" \
  --service-account-email="dataflow-worker-sa@pipelines-377521.iam.gserviceaccount.com" \
  --temp-location="gs://pipelines-377521-dataflow-gcp-temp-location/tmp" \
  --additional-experiments="use_runner_v2" \
  --template-file-gcs-location="gs://pipelines-377521-dataflow-flex-template/jdbc-to-bigquery-plus/flex/JDBC_To_BigQuery_Plus" \
  --parameters="sourceDatabaseJdbcDriverClassName=org.postgresql.Driver" \
  --parameters="sourceDatabaseConnectionURLSecretId=projects/484214517971/secrets/source-postgres-jdbc-url/versions/1" \
  --parameters="jdbcToBigQueryPlusConfigurationGcsLocation=gs://pipelines-377521-dataflow-pipeline-config/jdbctobigqueryplus/config.json" \
  --parameters="jdbcToBigQueryPlusStagingGcsLocation=gs://pipelines-377521-dataflow-pipeline-data/jdbctobigqueryplus"
```

## Load Test


## TODO

- Error handling.
- Unit/integration/load tests.
- Comments/docs.
