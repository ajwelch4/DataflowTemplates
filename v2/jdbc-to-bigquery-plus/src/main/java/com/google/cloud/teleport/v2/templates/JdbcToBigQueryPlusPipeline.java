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

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.JdbcToBigQueryPlusPipelineOptions;
import com.google.cloud.teleport.v2.values.JdbcToBigQueryPlusTask;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JdbcToBigQueryPlusPipeline. */
@Template(
    name = "JDBC_To_BigQuery_Plus",
    category = TemplateCategory.BATCH,
    displayName = "JDBC Plus to BigQuery Pipeline",
    description = "A pipeline to replicate data from JDBC sources to BigQuery.",
    optionsClass = JdbcToBigQueryPlusPipelineOptions.class,
    flexContainerName = "jdbctobigqueryplus")
public class JdbcToBigQueryPlusPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryPlusPipeline.class);

  private static String getSourceDatabaseConnectionURL(String secretId) throws IOException {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse response = client.accessSecretVersion(secretId);
      return response.getPayload().getData().toStringUtf8();
    }
  }

  public static void main(String[] args) throws IOException {
    PipelineOptionsFactory.register(JdbcToBigQueryPlusPipelineOptions.class);
    JdbcToBigQueryPlusPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(JdbcToBigQueryPlusPipelineOptions.class);

    DataSourceConfiguration sourceDataSourceConfiguration =
        DataSourceConfiguration.create(
            options.getSourceDatabaseJdbcDriverClassName(),
            getSourceDatabaseConnectionURL(options.getSourceDatabaseConnectionURLSecretId()));

    List<JdbcToBigQueryPlusTask> tasks =
        JdbcToBigQueryPlusTask.getTasks(options.getJdbcToBigQueryPlusConfigurationGcsLocation());

    Pipeline p = Pipeline.create(options);

    LOG.info("Building Beam pipeline from tasks.");

    buildPipeline(p, sourceDataSourceConfiguration, tasks, buildBigQueryIOWrite(options));

    p.run();
  }

  @VisibleForTesting
  static BigQueryIO.Write<Row> buildBigQueryIOWrite(JdbcToBigQueryPlusPipelineOptions options) {
    return BigQueryIO.<Row>write()
        .useBeamSchema()
        .withCustomGcsTempLocation(options.getJdbcToBigQueryPlusStagingGcsLocation())
        .withMethod(BigQueryIO.Write.Method.FILE_LOADS);
  }

  @VisibleForTesting
  static void buildPipeline(
      Pipeline p,
      DataSourceConfiguration sourceDatabaseDataSourceConfiguration,
      List<JdbcToBigQueryPlusTask> tasks,
      BigQueryIO.Write<Row> bigQueryIOWrite) {
    for (JdbcToBigQueryPlusTask task : tasks) {
      p.apply(
              "Read source table: " + task.getFullyQualifiedSourceTableName(),
              task.getSourcePartitionColumn() == null
                  ? JdbcIO.readRows()
                      .withDataSourceConfiguration(sourceDatabaseDataSourceConfiguration)
                      .withQuery(task.getSourceSql())
                  : JdbcIO.<Row>readWithPartitions()
                      .withDataSourceConfiguration(sourceDatabaseDataSourceConfiguration)
                      .withNumPartitions((int) task.getSourcePartitionCount())
                      .withPartitionColumn(task.getSourcePartitionColumn())
                      .withTable("(" + task.getSourceSql() + ") as source_sql")
                      .withRowOutput())
          .apply(
              "Write target table: " + task.getFullyQualifiedTargetTableName(),
              bigQueryIOWrite
                  .to(task.getTargetTableReference())
                  .withCreateDisposition(
                      BigQueryIO.Write.CreateDisposition.valueOf(task.getTargetCreateDisposition()))
                  .withWriteDisposition(
                      BigQueryIO.Write.WriteDisposition.valueOf(task.getTargetWriteDisposition())));
    }
  }
}
