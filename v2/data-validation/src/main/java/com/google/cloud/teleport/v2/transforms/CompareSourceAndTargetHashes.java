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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CompareSourceAndTargetHashes. */
public class CompareSourceAndTargetHashes
    extends PTransform<
        PCollection<KV<List<String>, CoGbkResult>>, PCollection<Map<String, String>>> {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(CompareSourceAndTargetHashes.class);

  private static class CompareSourceAndTargetHashesFn
      extends DoFn<KV<List<String>, CoGbkResult>, Map<String, String>> {

    private final String runId;

    private final String startTime;

    private String endTime;

    private final TupleTag<String> sourceTupleTag;

    private final TupleTag<String> targetTupleTag;

    private final PCollectionView<Map<String, JsonNode>> validationConfigurationsView;

    public enum SystemType {
      SOURCE,
      TARGET;
    }

    public CompareSourceAndTargetHashesFn(
        TupleTag<String> sourceTupleTag,
        TupleTag<String> targetTupleTag,
        PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
      runId = UUID.randomUUID().toString();
      startTime =
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
              .withZone(ZoneId.from(ZoneOffset.UTC))
              .format(Instant.now());
      this.sourceTupleTag = sourceTupleTag;
      this.targetTupleTag = targetTupleTag;
      this.validationConfigurationsView = validationConfigurationsView;
    }

    public static CompareSourceAndTargetHashesFn create(
        TupleTag<String> sourceTupleTag,
        TupleTag<String> targetTupleTag,
        PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
      return new CompareSourceAndTargetHashesFn(
          sourceTupleTag, targetTupleTag, validationConfigurationsView);
    }

    // TODO: Java doc for logic
    private static String getValidationName(KV<List<String>, CoGbkResult> joinedHashes) {
      return joinedHashes.getKey().get(0);
    }

    private String getEndTime() {
      if (endTime == null) {
        endTime =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.from(ZoneOffset.UTC))
                .format(Instant.now());
      }
      return endTime;
    }

    // TODO: Java doc for logic
    private static String getValidationConfigurationFieldValue(
        JsonNode validationConfiguration, String fieldName) {
      return validationConfiguration.get("validations").get(0).get(fieldName).asText();
    }

    // TODO: Java doc for logic
    private static String getSchemaQualifiedTableName(
        JsonNode validationConfiguration, SystemType systemType) {
      String prefix = "";
      if (systemType.equals(SystemType.TARGET)) {
        prefix = "target_";
      }
      return getValidationConfigurationFieldValue(validationConfiguration, prefix + "schema_name")
          + "."
          + getValidationConfigurationFieldValue(validationConfiguration, prefix + "table_name");
    }

    // TODO: javadoc for logic
    private static String getPrimaryKeyValues(KV<List<String>, CoGbkResult> joinedHashes) {
      return joinedHashes.getKey().subList(1, joinedHashes.getKey().size()).toString();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element KV<List<String>, CoGbkResult> joinedHashes,
        OutputReceiver<Map<String, String>> out) {
      final Map<String, String> validationResult = new HashMap<>();
      validationResult.put("run_id", runId);
      validationResult.put("validation_name", getValidationName(joinedHashes));
      validationResult.put("validation_type", "Row");
      validationResult.put("start_time", startTime);
      validationResult.put("end_time", getEndTime());
      final JsonNode validationConfiguration =
          c.sideInput(validationConfigurationsView).get(getValidationName(joinedHashes));
      validationResult.put(
          "source_table_name",
          getSchemaQualifiedTableName(validationConfiguration, SystemType.SOURCE));
      validationResult.put(
          "target_table_name",
          getSchemaQualifiedTableName(validationConfiguration, SystemType.TARGET));
      final String sourceHash = joinedHashes.getValue().getOnly(sourceTupleTag);
      final String targetHash = joinedHashes.getValue().getOnly(targetTupleTag);
      validationResult.put("source_agg_value", sourceHash);
      validationResult.put("target_agg_value", targetHash);
      validationResult.put("primary_keys", getPrimaryKeyValues(joinedHashes));
      validationResult.put("validation_status", sourceHash.equals(targetHash) ? "success" : "fail");
      out.output(validationResult);
    }
  }

  private final TupleTag<String> sourceTupleTag;

  private final TupleTag<String> targetTupleTag;

  private final PCollectionView<Map<String, JsonNode>> validationConfigurationsView;

  public CompareSourceAndTargetHashes(
      TupleTag<String> sourceTupleTag,
      TupleTag<String> targetTupleTag,
      PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
    this.sourceTupleTag = sourceTupleTag;
    this.targetTupleTag = targetTupleTag;
    this.validationConfigurationsView = validationConfigurationsView;
  }

  public static CompareSourceAndTargetHashes create(
      TupleTag<String> sourceTupleTag,
      TupleTag<String> targetTupleTag,
      PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
    return new CompareSourceAndTargetHashes(
        sourceTupleTag, targetTupleTag, validationConfigurationsView);
  }

  public PCollection<Map<String, String>> expand(
      PCollection<KV<List<String>, CoGbkResult>> joinedHashes) {
    return joinedHashes.apply(
        ParDo.of(
                CompareSourceAndTargetHashesFn.create(
                    sourceTupleTag, targetTupleTag, validationConfigurationsView))
            .withSideInputs(validationConfigurationsView));
  }
}
