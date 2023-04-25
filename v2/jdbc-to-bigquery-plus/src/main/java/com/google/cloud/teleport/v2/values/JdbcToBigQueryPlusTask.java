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
package com.google.cloud.teleport.v2.values;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.services.bigquery.model.TableReference;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.GcsUtils;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/** JdbcToBigQueryPlusTask. */
@AutoValue
@JsonDeserialize(builder = AutoValue_JdbcToBigQueryPlusTask.Builder.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public abstract class JdbcToBigQueryPlusTask implements Serializable {

  public static Builder builder() {
    return new AutoValue_JdbcToBigQueryPlusTask.Builder();
  }

  @Nullable
  public abstract String getSourceSchema();

  public abstract String getSourceTable();

  @Nullable
  public abstract ImmutableList<String> getSourceColumns();

  @Nullable
  public abstract String getSourceWatermarkColumn();

  @Nullable
  public abstract String getSourcePartitionColumn();

  @Nullable
  public abstract Integer getSourcePartitionCount();

  public abstract String getTargetProject();

  public abstract String getTargetDataset();

  public abstract String getTargetCreateDisposition();

  public abstract String getTargetWriteDisposition();

  @AutoValue.Builder
  @JsonPOJOBuilder(withPrefix = "set")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public abstract static class Builder {

    public abstract Builder setSourceSchema(String value);

    public abstract Builder setSourceTable(String value);

    public abstract Builder setSourceColumns(ImmutableList<String> value);

    public abstract Builder setSourceWatermarkColumn(String value);

    public abstract Builder setSourcePartitionColumn(String value);

    public abstract Builder setSourcePartitionCount(Integer value);

    public abstract Builder setTargetProject(String value);

    public abstract Builder setTargetDataset(String value);

    public abstract Builder setTargetCreateDisposition(String value);

    public abstract Builder setTargetWriteDisposition(String value);

    public abstract JdbcToBigQueryPlusTask build();
  }

  public static List<JdbcToBigQueryPlusTask> getTasks(final String gcsPath) throws IOException {
    ObjectMapper objectMapper =
        new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new GuavaModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    List<JdbcToBigQueryPlusTask> tasks = new ArrayList<>();
    List<String> configLines = GcsUtils.readAllLines(gcsPath);
    for (String configLine : configLines) {
      // TODO: if source_watermark_column != null, get getWaterMarkValueFromTarget
      tasks.add(objectMapper.readValue(configLine, JdbcToBigQueryPlusTask.class));
    }
    return tasks;
  }

  @JsonIgnore
  public String getFullyQualifiedSourceTableName() {
    String prefix = getSourceSchema() == null ? "" : getSourceSchema() + ".";
    return prefix + getSourceTable();
  }

  @JsonIgnore
  public String getSourceSql() {
    String selectList =
        (getSourceColumns() == null || getSourceColumns().isEmpty())
            ? "*"
            : String.join(", ", getSourceColumns());
    return "select " + selectList + " from " + getFullyQualifiedSourceTableName();
  }

  @JsonIgnore
  public TableReference getTargetTableReference() {
    // TODO: Handle characters that are invalid in BigQuery table names.
    String targetTable = getSourceTable();
    return new TableReference()
        .setProjectId(getTargetProject())
        .setDatasetId(getTargetDataset())
        .setTableId(targetTable);
  }

  @JsonIgnore
  public String getFullyQualifiedTargetTableName() {
    return getTargetTableReference().getProjectId()
        + "."
        + getTargetTableReference().getDatasetId()
        + "."
        + getTargetTableReference().getTableId();
  }
}
