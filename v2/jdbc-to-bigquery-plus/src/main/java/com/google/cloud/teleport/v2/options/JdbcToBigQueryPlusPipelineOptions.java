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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

/** JdbcToBigQueryPlusPipelineOptions. */
public interface JdbcToBigQueryPlusPipelineOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = true,
      description = "Fully qualified JDBC driver class name for the source database.",
      helpText = "Fully qualified JDBC driver class name for the source database.",
      example = "org.postgresql.Driver")
  @Description("Fully qualified JDBC driver class name for the source database.")
  @Required
  @Default.String("org.postgresql.Driver")
  String getSourceDatabaseJdbcDriverClassName();

  void setSourceDatabaseJdbcDriverClassName(String value);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      description = "Secret Manager secret ID for the source database connection URL.",
      helpText = "Secret Manager secret ID for the source database connection URL.",
      example = "projects/479389860811/secrets/source-postgres-jdbc-url/versions/1")
  @Description("Secret Manager secret ID for the source database connection URL.")
  @Required
  String getSourceDatabaseConnectionURLSecretId();

  void setSourceDatabaseConnectionURLSecretId(String value);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      description = "GCS path to JSON configuration file for JdbcToBigQueryPlus pipeline.",
      helpText = "GCS path to JSON configuration file for JdbcToBigQueryPlus pipeline.",
      example = "gs://project-374206-jdbctobigqueryplus-config/config.json")
  @Description("GCS path to JSON configuration file for JdbcToBigQueryPlus pipeline.")
  @Required
  String getJdbcToBigQueryPlusConfigurationGcsLocation();

  void setJdbcToBigQueryPlusConfigurationGcsLocation(String value);

  @TemplateParameter.Text(
      order = 4,
      optional = false,
      description =
          "GCS folder where data from the JDBC source will be staged before loading into BigQuery.",
      helpText =
          "GCS folder where data from the JDBC source will be staged before loading into BigQuery.",
      example = "gs://project-374206-jdbctobigquery-staging")
  @Description(
      "GCS folder where data from the JDBC source will be staged before loading into BigQuery.")
  @Required
  ValueProvider<String> getJdbcToBigQueryPlusStagingGcsLocation();

  void setJdbcToBigQueryPlusStagingGcsLocation(ValueProvider<String> value);
}
