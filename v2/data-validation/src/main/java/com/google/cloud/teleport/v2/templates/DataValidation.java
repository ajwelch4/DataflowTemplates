/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.coders.JsonCoder;
import com.google.cloud.teleport.v2.transforms.CompareSourceAndTargetHashes;
import com.google.cloud.teleport.v2.transforms.DeserializeConnectionConfigurations;
import com.google.cloud.teleport.v2.transforms.DeserializeValidationConfigurations;
import com.google.cloud.teleport.v2.transforms.ExecuteQueries;
import com.google.cloud.teleport.v2.transforms.GenerateSourceAndTargetQueries;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Data Validation. */
@Template(
    name = "Data_Validation",
    category = TemplateCategory.BATCH,
    displayName = "Data Validation",
    description = "Perform a row hash comparison of database tables.",
    optionsClass = DataValidation.Options.class,
    flexContainerName = "data-validation")
public class DataValidation {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(DataValidation.class);

  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "GCS location of JSON connection files.",
        helpText = "GCS location of JSON connection files.",
        example = "gs://bucket/connections")
    @Description("GCS location of JSON connection files.")
    @Required
    String getConnectionsGcsLocation();

    void setConnectionsGcsLocation(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        description =
            "Glob matching GCS locations of YAML configuration files. "
                + "YAML configuration files can be optionally compressed.",
        helpText =
            "Glob matching GCS locations of YAML configuration files. "
                + "YAML configuration files can be optionally compressed.",
        example = "gs://bucket/configs/**/*.yaml.gz")
    @Description(
        "Glob matching GCS locations of YAML configuration files. "
            + "YAML configuration files can be optionally compressed.")
    @Required
    String getConfigurationGcsLocationsPattern();

    void setConfigurationGcsLocationsPattern(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        description = "GCS location where results will be staged before loading into BigQuery.",
        helpText = "GCS location where results will be staged before loading into BigQuery.",
        example = "gs://project-374206-staging")
    @Description("GCS location where results will be staged before loading into BigQuery.")
    @Required
    ValueProvider<String> getResultsStagingGcsLocation();

    void setResultsStagingGcsLocation(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 4,
        optional = false,
        description = "Fully qualified results table name.",
        helpText = "Fully qualified results table name.",
        example = "project_id.dataset_id.table_name")
    @Description("Fully qualified results table name.")
    @Required
    String getFullyQualifiedResultsTableName();

    void setFullyQualifiedResultsTableName(String value);
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    DataValidation.Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataValidation.Options.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .getCoderRegistry()
        .registerCoderForClass(JsonNode.class, new JsonCoder<JsonNode>(JsonNode.class));

    buildPipeline(
        pipeline,
        options.getConnectionsGcsLocation(),
        options.getConfigurationGcsLocationsPattern(),
        options.getResultsStagingGcsLocation(),
        options.getFullyQualifiedResultsTableName());

    pipeline.run();
  }

  private static void buildPipeline(
      Pipeline pipeline,
      String connectionsGcsLocation,
      String configurationGcsLocationsPattern,
      ValueProvider<String> resultsStagingGcsLocation,
      String fullyQualifiedResultsTableName) {

    PCollectionView<Map<String, JsonNode>> connectionConfigurationsView =
        pipeline
            .apply(
                "Find JSON connection configuration files",
                FileIO.match().filepattern(connectionsGcsLocation + "/*.connection.json"))
            .apply("Read JSON connection configuration files", FileIO.readMatches())
            .apply(
                "Deserialize JSON connection configuration files",
                DeserializeConnectionConfigurations.create())
            .apply("Transform connection configurations to PCollectionView as Map", View.asMap());

    PCollection<KV<String, JsonNode>> validationConfigurations =
        pipeline
            .apply(
                "Find YAML validation configuration files",
                FileIO.match().filepattern(configurationGcsLocationsPattern))
            .apply("Read YAML validation configuration files", FileIO.readMatches())
            .apply(
                "Deserialize YAML validation configuration files",
                DeserializeValidationConfigurations.create());

    PCollectionView<Map<String, JsonNode>> validationConfigurationsView =
        validationConfigurations.apply(
            "Transform validation configurations to PCollectionView as Map", View.asMap());

    PCollection<KV<String, JsonNode>> sourceAndTargetQueries =
        validationConfigurations.apply(
            "Generate source and target queries",
            GenerateSourceAndTargetQueries.create(connectionsGcsLocation));

    PCollection<KV<List<String>, String>> sourceHashes =
        sourceAndTargetQueries.apply(
            "Execute source queries",
            ExecuteQueries.create(
                ExecuteQueries.SystemType.SOURCE,
                connectionConfigurationsView,
                validationConfigurationsView));

    PCollection<KV<List<String>, String>> targetHashes =
        sourceAndTargetQueries.apply(
            "Execute target queries",
            ExecuteQueries.create(
                ExecuteQueries.SystemType.TARGET,
                connectionConfigurationsView,
                validationConfigurationsView));

    final TupleTag<String> sourceTupleTag = new TupleTag<>();
    final TupleTag<String> targetTupleTag = new TupleTag<>();
    PCollection<KV<List<String>, CoGbkResult>> joinedHashes =
        KeyedPCollectionTuple.of(sourceTupleTag, sourceHashes)
            .and(targetTupleTag, targetHashes)
            .apply("Join source and target hashes", CoGroupByKey.<List<String>>create());

    PCollection<Map<String, String>> validationResults =
        joinedHashes.apply(
            "Compare source and target hashes",
            CompareSourceAndTargetHashes.create(
                sourceTupleTag, targetTupleTag, validationConfigurationsView));

    validationResults.apply(
        "Write results",
        BigQueryIO.<Map<String, String>>write()
            .to(fullyQualifiedResultsTableName)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withCustomGcsTempLocation(resultsStagingGcsLocation)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withFormatFunction(
                validationResult -> {
                  TableRow row = new TableRow();
                  validationResult.forEach(
                      (columnName, columnValue) -> {
                        row.set(columnName, columnValue);
                      });
                  return row;
                }));
  }
}
