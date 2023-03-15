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
```

## Infrastructure

The infrastructure required by this template can be deployed via
[this](https://github.com/ajwelch4/DataflowTemplatesOps/tree/v2-data-validation/v2/data-validation)
Terraform repo.

## Stage

Adjust the parameters to the commands below according to your own environment
and run from the root of the repo:

```shell
cd v2/data-validation \
    && docker build -t data-validation:latest . \
    && cd ../..

mvn clean install -pl plugins/templates-maven-plugin -am

mvn clean package -pl v2/data-validation -am \
    -PtemplatesStage \
    -DskipShade \
    -DskipTests \
    -DprojectId="data-validation-project" \
    -Dregion="us-east1" \
    -Dartifactregion="us-east1" \
    -DbaseContainerImage="docker://data-validation:latest" \
    -DbucketName="data-validation-project-dataflow-flex-template" \
    -DstagePrefix="data-validation" \
    -DtemplateName="Data_Validation"
```

If needed, inspect/debug template container:

```shell
docker pull gcr.io/data-validation-project/data-validation/data-validation:latest
docker run -it --entrypoint /bin/bash gcr.io/data-validation-project/data-validation/data-validation:latest
```

## Execute

Adjust the parameters to the command below according to your own environment:

```shell
gcloud dataflow flex-template run "data-validation-$(date +%s)" \
  --region="us-east1" \
  --service-account-email="dataflow-worker-sa@data-validation-project.iam.gserviceaccount.com" \
  --temp-location="gs://data-validation-project-dataflow-gcp-temp-location/tmp" \
  --additional-experiments="use_runner_v2" \
  --template-file-gcs-location="gs://data-validation-project-dataflow-flex-template/data-validation/flex/Data_Validation" \
  --parameters="configGcsLocation=gs://data-validation-project-dataflow-pipeline-data/config" \
  --parameters="sourceConnection=my_bq_conn" \
  --parameters="fullyQualifiedSourceTableName=data-validation-project.dataset_id.source_table" \
  --parameters="targetConnection=my_bq_conn" \
  --parameters="fullyQualifiedTargetTableName=data-validation-project.dataset_id.target_table" \
  --parameters="primaryKeys=primary_key" \
  --parameters="partitionKey=partition_key" \
  --parameters="partitionCount=60" \
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
mvn clean verify -pl v2/data-validation -am -fae \
    -Dmdep.analyze.skip -Dcheckstyle.skip -Dspotless.check.skip \
    -Denforcer.skip -DskipShade -Djib.skip -Djacoco.skip \
    -DfailIfNoTests=false \
    -Dproject="data-validation-project" \
    -Dregion="us-east1" \
    -DexportProject="data-validation-project" \
    -DexportDataset="load_test" \
    -DexportTable="metrics" \
    -DspecPath="gs://data-validation-project-dataflow-flex-template/data-validation/flex/Data_Validation" \
    -DartifactBucket="data-validation-project-dataflow-pipeline-data" \
    -DtempLocation="gs://data-validation-project-dataflow-gcp-temp-location/tmp" \
    -DserviceAccountEmail="dataflow-worker-sa@data-validation-project.iam.gserviceaccount.com" \
    -DadditionalExperiments="use_runner_v2" \
    -Dtest=DataValidationLT#testDataflow \
    -DconfigGcsLocation="gs://data-validation-project-dataflow-pipeline-data/config" \
    -DsourceConnection="my_bq_conn" \
    -DtargetConnection="my_bq_conn" \
    -DpartitionCount="60" \
    -DresultsStagingGcsLocation="gs://data-validation-project-dataflow-pipeline-data/data" \
    -DfullyQualifiedResultsTableName="data-validation-project.load_test.results" \
    -DmachineType="n2-highmem-2" \
    -DnumWorkers="30" \
    -DmaxWorkers="100"
```

## TODO

- Truncate results table after load tests.
- Parameterize load tests.
- Update Dockerfile once [dry-run PR](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/778) is merged.
- Add `validation_name` as pipeline option.
- Fix `end_time`.
- Properly quote partition key.
- Package additional JDBC drivers and account for them in `getDataSourceConfiguration`.
- Filters.
- Hash column list.
- Error handling.
- Unit/integration tests.
- Comments/docs.
