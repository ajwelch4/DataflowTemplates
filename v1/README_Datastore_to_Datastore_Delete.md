Bulk Delete Entities in Datastore [Deprecated] Template
---
A pipeline which reads in Entities (via a GQL query) from Datastore, optionally passes in the JSON encoded Entities to a JavaScript UDF, and then deletes all matching Entities in the selected target project.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/datastore-bulk-delete)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **datastoreReadGqlQuery** (GQL Query): Specifies which Datastore entities to read. Ex: ‘SELECT * FROM MyKind’.
* **datastoreReadProjectId** (Project ID): The Google Cloud project ID of the Datastore instance to read from.
* **datastoreDeleteProjectId** (Delete all matching entities from the GQL Query present in this Datastore Project Id of): Google Cloud Project Id of where to delete the datastore entities.

### Optional Parameters

* **datastoreReadNamespace** (Namespace): Namespace of requested Datastore entities. Leave blank to use default namespace.
* **javascriptTextTransformGcsPath** (JavaScript UDF path in Cloud Storage): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions.
* **javascriptTextTransformFunctionName** (JavaScript UDF name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: transform_udf1).
* **datastoreHintNumWorkers** (Expected number of workers): Hint for the expected number of workers in the Datastore ramp-up throttling step. Defaults to: 500.

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Classic Template, meaning that the pipeline code will be
executed only once and the pipeline will be saved to Google Cloud Storage for
further reuse. Please check [Creating classic Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
and [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Datastore_to_Datastore_Delete" \
-pl v1 \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Datastore_to_Datastore_Delete
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Datastore_to_Datastore_Delete"

### Mandatory
export DATASTORE_READ_GQL_QUERY=<datastoreReadGqlQuery>
export DATASTORE_READ_PROJECT_ID=<datastoreReadProjectId>
export DATASTORE_DELETE_PROJECT_ID=<datastoreDeleteProjectId>

### Optional
export DATASTORE_READ_NAMESPACE=<datastoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export DATASTORE_HINT_NUM_WORKERS=500

gcloud dataflow jobs run "datastore-to-datastore-delete-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "datastoreReadGqlQuery=$DATASTORE_READ_GQL_QUERY" \
  --parameters "datastoreReadProjectId=$DATASTORE_READ_PROJECT_ID" \
  --parameters "datastoreReadNamespace=$DATASTORE_READ_NAMESPACE" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "datastoreDeleteProjectId=$DATASTORE_DELETE_PROJECT_ID" \
  --parameters "datastoreHintNumWorkers=$DATASTORE_HINT_NUM_WORKERS"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
export DATASTORE_READ_GQL_QUERY=<datastoreReadGqlQuery>
export DATASTORE_READ_PROJECT_ID=<datastoreReadProjectId>
export DATASTORE_DELETE_PROJECT_ID=<datastoreDeleteProjectId>

### Optional
export DATASTORE_READ_NAMESPACE=<datastoreReadNamespace>
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export DATASTORE_HINT_NUM_WORKERS=500

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="datastore-to-datastore-delete-job" \
-DtemplateName="Datastore_to_Datastore_Delete" \
-Dparameters="datastoreReadGqlQuery=$DATASTORE_READ_GQL_QUERY,datastoreReadProjectId=$DATASTORE_READ_PROJECT_ID,datastoreReadNamespace=$DATASTORE_READ_NAMESPACE,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,datastoreDeleteProjectId=$DATASTORE_DELETE_PROJECT_ID,datastoreHintNumWorkers=$DATASTORE_HINT_NUM_WORKERS" \
-pl v1 \
-am
```