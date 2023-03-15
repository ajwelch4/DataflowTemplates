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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Data Validation. */
@Template(
    name = "Data_Validation",
    category = TemplateCategory.BATCH,
    displayName = "Data Validation",
    description = "Perform a row hash comparison of two database tables.",
    optionsClass = DataValidation.Options.class,
    flexContainerName = "data-validation")
public class DataValidation {

  private static final Logger LOG = LoggerFactory.getLogger(DataValidation.class);
  private static final String PARTITION_KEY_ALIAS = "__dv_partition_key__";

  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "Config GCS location.",
        helpText = "Config GCS location.",
        example = "gs://bucket/config")
    @Description("Config GCS location.")
    @Required
    String getConfigGcsLocation();

    void setConfigGcsLocation(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        description = "Source connection name.",
        helpText = "Source connection name.",
        example = "my_bq_conn")
    @Description("Source connection name.")
    @Required
    String getSourceConnection();

    void setSourceConnection(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        description = "Fully qualified source table name.",
        helpText = "Fully qualified source table name.",
        example = "project_id.dataset_id.table_name")
    @Description("Fully qualified source table name.")
    @Required
    String getFullyQualifiedSourceTableName();

    void setFullyQualifiedSourceTableName(String value);

    @TemplateParameter.Text(
        order = 4,
        optional = false,
        description = "Target connection name.",
        helpText = "Target connection name.",
        example = "my_bq_conn")
    @Description("Target connection name.")
    @Required
    String getTargetConnection();

    void setTargetConnection(String value);

    @TemplateParameter.Text(
        order = 5,
        optional = false,
        description = "Fully qualified target table name.",
        helpText = "Fully qualified target table name.",
        example = "project_id.dataset_id.table_name")
    @Description("Fully qualified target table name.")
    @Required
    String getFullyQualifiedTargetTableName();

    void setFullyQualifiedTargetTableName(String value);

    @TemplateParameter.Text(
        order = 6,
        optional = false,
        description = "Primary key columns used to join source and target rows.",
        helpText = "Primary key columns used to join source and target rows.",
        example = "col0,col1")
    @Description("Primary key columns used to join source and target rows.")
    @Required
    List<String> getPrimaryKeys();

    void setPrimaryKeys(List<String> value);

    @TemplateParameter.Text(
        order = 7,
        optional = false,
        description = "Column to partition on.",
        helpText = "Column to partition on.",
        example = "col2")
    @Description("Column to partition on.")
    @Required
    String getPartitionKey();

    void setPartitionKey(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = false,
        description = "Number of partitions.",
        helpText = "Number of partitions.",
        example = "5")
    @Description("Number of partitions.")
    @Required
    Integer getPartitionCount();

    void setPartitionCount(Integer value);

    @TemplateParameter.Text(
        order = 9,
        optional = false,
        description = "GCS location where results will be staged before loading into BigQuery.",
        helpText = "GCS location where results will be staged before loading into BigQuery.",
        example = "gs://project-374206-staging")
    @Description("GCS location where results will be staged before loading into BigQuery.")
    @Required
    ValueProvider<String> getResultsStagingGcsLocation();

    void setResultsStagingGcsLocation(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 10,
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

    Map<String, String> queries =
        getSourceAndTargetQueries(
            options.getConfigGcsLocation(),
            options.getSourceConnection(),
            options.getFullyQualifiedSourceTableName(),
            options.getTargetConnection(),
            options.getFullyQualifiedTargetTableName(),
            options.getPrimaryKeys(),
            options.getPartitionKey());

    DataSourceConfiguration sourceDataSourceConfiguration =
        getDataSourceConfiguration(options.getConfigGcsLocation(), options.getSourceConnection());

    DataSourceConfiguration targetDataSourceConfiguration =
        getDataSourceConfiguration(options.getConfigGcsLocation(), options.getTargetConnection());

    Pipeline pipeline = Pipeline.create(options);

    buildPipeline(
        pipeline,
        sourceDataSourceConfiguration,
        options.getFullyQualifiedSourceTableName(),
        queries.get("source_query"),
        targetDataSourceConfiguration,
        options.getFullyQualifiedTargetTableName(),
        queries.get("target_query"),
        options.getPrimaryKeys(),
        options.getPartitionCount(),
        options.getResultsStagingGcsLocation(),
        options.getFullyQualifiedResultsTableName());

    pipeline.run();
  }

  private static Map<String, String> getSourceAndTargetQueries(
      String configGcsLocation,
      String sourceConnection,
      String fullyQualifiedSourceTableName,
      String targetConnection,
      String fullyQualifiedTargetTableName,
      List<String> primaryKeys,
      String partitionKey)
      throws IOException, InterruptedException {

    ProcessBuilder processBuilder = new ProcessBuilder();

    processBuilder.environment().put("PSO_DV_CONFIG_HOME", configGcsLocation);

    processBuilder.command(
        "/bin/bash",
        "-c",
        "source /opt/professional-services-data-validator/env/bin/activate &&"
            + " data-validation validate --dry-run row -sc "
            + sourceConnection
            + " -tc "
            + targetConnection
            + " -tbls "
            + fullyQualifiedSourceTableName
            + "="
            + fullyQualifiedTargetTableName
            + " --primary-keys "
            + String.join(",", primaryKeys)
            + " --hash '*'");
    LOG.debug("Command: " + String.join(" ", processBuilder.command()));

    Process process = processBuilder.start();
    String stdOut =
        new BufferedReader(new InputStreamReader(process.getInputStream()))
            .lines()
            .peek(LOG::debug)
            .collect(Collectors.joining("\n"));
    String stdErr =
        new BufferedReader(new InputStreamReader(process.getErrorStream()))
            .lines()
            .peek(LOG::error)
            .collect(Collectors.joining("\n"));
    int exitCode = process.waitFor();

    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> queries = mapper.readValue(stdOut, Map.class);
    queries.replaceAll(
        (destination, query) ->
            "SELECT " + partitionKey + " as " + PARTITION_KEY_ALIAS + ", " + query.substring(6));
    LOG.info("Source query: " + queries.get("source_query"));
    LOG.info("Target query: " + queries.get("target_query"));

    return queries;
  }

  private static DataSourceConfiguration getDataSourceConfiguration(
      String configGcsLocation, String connection) throws IOException, JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    Path connectionPath =
        Paths.get(
            URI.create(configGcsLocation + "/connections/" + connection + ".connection.json"));

    Map<String, String> connectionConfig =
        mapper.readValue(Files.readString(connectionPath, StandardCharsets.UTF_8), Map.class);

    return DataSourceConfiguration.create(
        "com.simba.googlebigquery.jdbc.Driver",
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;IgnoreTransactions=1;ProjectId="
            + connectionConfig.get("project_id")
            + ";");
  }

  private static void buildPipeline(
      Pipeline pipeline,
      DataSourceConfiguration sourceDataSourceConfiguration,
      String fullyQualifiedSourceTableName,
      String sourceQuery,
      DataSourceConfiguration targetDataSourceConfiguration,
      String fullyQualifiedTargetTableName,
      String targetQuery,
      List<String> primaryKeys,
      Integer partitionCount,
      ValueProvider<String> resultsStagingGcsLocation,
      String fullyQualifiedResultsTableName) {

    PCollection<Row> sourceRows =
        pipeline.apply(
            "Read source",
            JdbcIO.<Row>readWithPartitions()
                .withDataSourceConfiguration(sourceDataSourceConfiguration)
                .withNumPartitions((int) partitionCount)
                .withPartitionColumn(PARTITION_KEY_ALIAS)
                .withTable("(" + sourceQuery + ") as source")
                .withRowOutput());

    PCollection<Row> targetRows =
        pipeline.apply(
            "Read target",
            JdbcIO.<Row>readWithPartitions()
                .withDataSourceConfiguration(targetDataSourceConfiguration)
                .withNumPartitions((int) partitionCount)
                .withPartitionColumn(PARTITION_KEY_ALIAS)
                .withTable("(" + targetQuery + ") as target")
                .withRowOutput());

    PCollection<Row> joinedRows =
        sourceRows.apply(
            "Join source and target",
            Join.<Row, Row>fullOuterJoin(targetRows).using(primaryKeys.toArray(new String[0])));

    String runId = UUID.randomUUID().toString();
    String startTime =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.from(ZoneOffset.UTC))
            .format(Instant.now());
    PCollection<Row> validatedRows =
        joinedRows.apply(
            "Validate",
            SqlTransform.query(
                "select '"
                    + runId
                    + "' as run_id, "
                    + "'Row' as validation_type, "
                    + "timestamp '"
                    + startTime
                    + "' as start_time, "
                    + "current_timestamp as end_time, '"
                    + fullyQualifiedSourceTableName
                    + "' as source_table_name, '"
                    + fullyQualifiedTargetTableName
                    + "' as target_table_name, "
                    + "p.lhs.hash__all as source_agg_value, "
                    + "p.rhs.hash__all as target_agg_value, "
                    + String.join(
                        " || ',' || ",
                        primaryKeys.stream()
                            .map(pk -> "coalesce(p.lhs." + pk + ", p.rhs." + pk + ")")
                            .collect(Collectors.toList()))
                    + " as primary_keys, "
                    + "case when p.lhs.hash__all = p.rhs.hash__all then 'success' "
                    + "else 'fail' end as validation_status "
                    + "from PCOLLECTION as p"));

    validatedRows.apply(
        "Write results",
        BigQueryIO.<Row>write()
            .useBeamSchema()
            .to(fullyQualifiedResultsTableName)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withCustomGcsTempLocation(resultsStagingGcsLocation)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
  }
}
