# Data Validation Template

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
gcloud config set project data-validation-project
gcloud auth configure-docker us-east1-docker.pkg.dev
```

## Infrastructure

The infrastructure required by this template can be deployed via
[this](https://github.com/ajwelch4/DataflowTemplatesOps/tree/v2-data-validation/v2/data-validation)
Terraform repo.

## Build Custom Launcher and Worker Images

```shell
cd v2/data-validation/docker/launcher \
    && gcloud builds submit \
        --tag us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-launcher:latest . \
    && cd ../worker \
    && gcloud builds submit \
        --tag us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-worker:latest . \
    && cd ../../../..
```

If needed, inspect/debug images:

```shell
docker pull us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-launcher:latest
docker run -it --entrypoint /bin/bash us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-launcher:latest

docker pull us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-worker:latest
docker run -it --entrypoint /bin/bash us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-worker:latest

```

## Stage

Adjust the parameters to the commands below according to your own environment
and run from the root of the repo:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am

mvn clean package -pl v2/data-validation -am \
    -PtemplatesStage \
    -DskipShade \
    -DskipTests \
    -DprojectId="data-validation-project" \
    -Dregion="us-east1" \
    -Dartifactregion="us-east1" \
    -DbaseContainerImage="us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-launcher:latest" \
    -DbucketName="data-validation-project-dataflow-flex-template" \
    -DstagePrefix="data-validation" \
    -DtemplateName="Data_Validation"
```

## Execute

Adjust the parameters to the command below according to your own environment:

```shell
PSO_DV_CONFIG_HOME="gs://data-validation-project-dataflow-pipeline-data/config" \
    data-validation connections add \
        --connection-name my_bq_conn BigQuery \
        --project-id data-validation-project

PSO_DV_CONFIG_HOME="gs://data-validation-project-dataflow-pipeline-data/config" \
    data-validation generate-table-partitions \
        -sc my_bq_conn \
        -tc my_bq_conn \
        -tbls bigquery-public-data.new_york_citibike.citibike_stations \
        --primary-keys station_id,name,short_name \
        --hash '*' \
        --config-dir "gs://data-validation-project-dataflow-pipeline-data/config/validations" \
        --partition-num 10 \
        --labels tag=test-run,owner=name \
        --format table \
        --filter-status fail

PSO_DV_CONFIG_HOME="gs://data-validation-project-dataflow-pipeline-data/config" \
    data-validation generate-table-partitions \
        -sc my_bq_conn \
        -tc my_bq_conn \
        -tbls bigquery-public-data.chicago_taxi_trips.taxi_trips \
        --primary-keys unique_key \
        --hash '*' \
        --config-dir "gs://data-validation-project-dataflow-pipeline-data/config/validations" \
        --partition-num 10 \
        --labels tag=test-run,owner=name \
        --format table \
        --filter-status fail

gcloud dataflow flex-template run "data-validation-$(date +%s)" \
  --region="us-east1" \
  --service-account-email="dataflow-worker-sa@data-validation-project.iam.gserviceaccount.com" \
  --temp-location="gs://data-validation-project-dataflow-gcp-temp-location/tmp" \
  --additional-experiments="use_runner_v2" \
  --template-file-gcs-location="gs://data-validation-project-dataflow-flex-template/data-validation/flex/Data_Validation" \
  --parameters="sdkContainerImage=us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-worker:latest" \
  --parameters="connectionsGcsLocation=gs://data-validation-project-dataflow-pipeline-data/config/connections" \
  --parameters="configurationGcsLocationsPattern=gs://data-validation-project-dataflow-pipeline-data/config/**/*.yaml" \
  --parameters="resultsStagingGcsLocation=gs://data-validation-project-dataflow-pipeline-data/data" \
  --parameters="fullyQualifiedResultsTableName=data-validation-project.load_test.results" \
  --worker-machine-type="n2-highmem-2" \
  --num-workers=30 \
  --max-workers=100
```

## Load Test

Ensure you have staged the template as described above, adjust the parameters to
the command below according to your own environment and run from the root of the
repo:

```shell
source /home/user/code/professional-services-data-validator/venv/bin/activate

mvn clean verify -pl v2/data-validation -am -fae \
    -Dmdep.analyze.skip -Dcheckstyle.skip -Dspotless.check.skip \
    -Denforcer.skip -DskipShade -Djib.skip -Djacoco.skip \
    -DfailIfNoTests=false \
    -Dproject="data-validation-project" \
    -Dregion="us-east1" \
    -DserviceAccountEmail="dataflow-worker-sa@data-validation-project.iam.gserviceaccount.com" \
    -DsdkContainerImage="us-east1-docker.pkg.dev/data-validation-project/data-validation/dataflow/data-validation-worker:latest" \
    -DspecPath="gs://data-validation-project-dataflow-flex-template/data-validation/flex/Data_Validation" \
    -DartifactBucket="data-validation-project-dataflow-pipeline-data" \
    -DadditionalExperiments="use_runner_v2" \
    -Dtest=DataValidationLT#testDataflow \
    -DexportProject="data-validation-project" \
    -DexportDataset="load_test" \
    -DexportTable="metrics" \
    -DfullyQualifiedResultsTableName="data-validation-project.load_test.results" \
    -DmachineType="n2-highmem-2" \
    -DnumWorkers="30" \
    -DmaxWorkers="100"
```

## Contributing

```shell
mvn spotless:apply -pl v2/data-validation -am
mvn checkstyle:check -pl v2/data-validation -am
mvn verify -pl v2/data-validation -am
```

## TODO

- Update load test.

- Create results table as part of test
    - Delete/Truncate results table after load tests.
    - What about export table??
- Support multiple load test tables
- Comments/docs.
- Move ObjectMapper out of ProcessElement in GenerateSourceAndTargetQueries

- Package additional JDBC drivers and account for them in `getDataSourceConfiguration`.
    - Use enum instead of case?
    - https://github.com/ajwelch4/DataflowTemplates/pull/2
- Parameterize load tests.
- Unit/integration tests.

- repin snakeyaml version/check if beam 2.55 uses a more recent version of jackson
