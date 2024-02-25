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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DeserializeValidationConfigurations. */
public class DeserializeValidationConfigurations
    extends PTransform<PCollection<ReadableFile>, PCollection<KV<String, JsonNode>>> {

  @SuppressWarnings("unused")
  private static final Logger LOG =
      LoggerFactory.getLogger(DeserializeValidationConfigurations.class);

  private static class DeserializeValidationConfigurationsFn
      extends InferableFunction<ReadableFile, KV<String, JsonNode>> {

    private final ObjectMapper objectMapper;

    public DeserializeValidationConfigurationsFn() {
      objectMapper = new ObjectMapper(new YAMLFactory());
    }

    public static DeserializeValidationConfigurationsFn create() {
      return new DeserializeValidationConfigurationsFn();
    }

    @Override
    public KV<String, JsonNode> apply(ReadableFile file) {
      JsonNode configuration = null;
      try {
        configuration = objectMapper.readValue(file.readFullyAsUTF8String(), JsonNode.class);
      } catch (JsonProcessingException ex) {
        throw new RuntimeException(
            "Error deserializing YAML validation configuration file: "
                + file.getMetadata().resourceId().toString()
                + ".",
            ex);
      } catch (IOException ex) {
        throw new RuntimeException(
            "Error reading YAML validation configuration file: "
                + file.getMetadata().resourceId().toString()
                + ".",
            ex);
      }
      return KV.of(file.getMetadata().resourceId().toString(), configuration);
    }
  }

  public static DeserializeValidationConfigurations create() {
    return new DeserializeValidationConfigurations();
  }

  @Override
  public PCollection<KV<String, JsonNode>> expand(PCollection<ReadableFile> files) {
    return files.apply(MapElements.via(DeserializeValidationConfigurationsFn.create()));
  }
}
