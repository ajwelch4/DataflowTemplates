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
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeserializeConnectionConfigurations
    extends PTransform<PCollection<ReadableFile>, PCollection<KV<String, JsonNode>>> {

  @SuppressWarnings("unused")
  private static final Logger LOG =
      LoggerFactory.getLogger(DeserializeConnectionConfigurations.class);

  public static DeserializeConnectionConfigurations create() {
    return new DeserializeConnectionConfigurations();
  }

  @Override
  public PCollection<KV<String, JsonNode>> expand(PCollection<ReadableFile> files) {
    return files.apply(
        MapElements.via(
            new InferableFunction<ReadableFile, KV<String, JsonNode>>() {
              @Override
              public KV<String, JsonNode> apply(ReadableFile file) {
                ObjectMapper connectionObjectMapper = new ObjectMapper();
                JsonNode connectionConfiguration = null;
                try {
                  connectionConfiguration =
                      connectionObjectMapper.readValue(
                          file.readFullyAsUTF8String(), JsonNode.class);
                } catch (JsonProcessingException ex) {
                  throw new RuntimeException(
                      "Error deserializing JSON connection configuration file: "
                          + file.getMetadata().resourceId().toString()
                          + ".",
                      ex);
                } catch (IOException ex) {
                  throw new RuntimeException(
                      "Error reading JSON connection configuration file: "
                          + file.getMetadata().resourceId().toString()
                          + ".",
                      ex);
                }
                String connectionName =
                    Files.getNameWithoutExtension(file.getMetadata().resourceId().toString())
                        .split("\\.")[0];
                return KV.of(connectionName, connectionConfiguration);
              }
            }));
  }
}
