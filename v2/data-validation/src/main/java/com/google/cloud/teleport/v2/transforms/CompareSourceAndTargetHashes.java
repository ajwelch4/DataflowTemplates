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

public class CompareSourceAndTargetHashes
    extends PTransform<
        PCollection<KV<List<String>, CoGbkResult>>, PCollection<Map<String, String>>> {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(CompareSourceAndTargetHashes.class);

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
    String runId = UUID.randomUUID().toString();
    String startTime =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.from(ZoneOffset.UTC))
            .format(Instant.now());
    return joinedHashes.apply(
        ParDo.of(
                new DoFn<KV<List<String>, CoGbkResult>, Map<String, String>>() {

                  private String endTime;

                  @ProcessElement
                  public void processElement(
                      ProcessContext c,
                      @Element KV<List<String>, CoGbkResult> joinedHash,
                      OutputReceiver<Map<String, String>> out) {
                    final Map<String, String> validationResult = new HashMap<>();
                    validationResult.put("run_id", runId);
                    validationResult.put("validation_name", joinedHash.getKey().get(0));
                    validationResult.put("validation_type", "Row");
                    validationResult.put("start_time", startTime);
                    if (endTime == null) {
                      endTime =
                          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                              .withZone(ZoneId.from(ZoneOffset.UTC))
                              .format(Instant.now());
                    }
                    validationResult.put("end_time", endTime);
                    JsonNode validationConfiguration =
                        c.sideInput(validationConfigurationsView).get(joinedHash.getKey().get(0));
                    validationResult.put(
                        "source_table_name",
                        validationConfiguration
                                .get("validations")
                                .get(0)
                                .get("schema_name")
                                .asText()
                            + "."
                            + validationConfiguration
                                .get("validations")
                                .get(0)
                                .get("table_name")
                                .asText());
                    validationResult.put(
                        "target_table_name",
                        validationConfiguration
                                .get("validations")
                                .get(0)
                                .get("target_schema_name")
                                .asText()
                            + "."
                            + validationConfiguration
                                .get("validations")
                                .get(0)
                                .get("target_table_name")
                                .asText());
                    String sourceHash = joinedHash.getValue().getOnly(sourceTupleTag);
                    String targetHash = joinedHash.getValue().getOnly(targetTupleTag);
                    validationResult.put("source_agg_value", sourceHash);
                    validationResult.put("target_agg_value", targetHash);
                    validationResult.put(
                        "primary_keys",
                        joinedHash.getKey().subList(1, joinedHash.getKey().size()).toString());
                    validationResult.put(
                        "validation_status", sourceHash.equals(targetHash) ? "success" : "fail");
                    out.output(validationResult);
                  }
                })
            .withSideInputs(validationConfigurationsView));
  }
}
