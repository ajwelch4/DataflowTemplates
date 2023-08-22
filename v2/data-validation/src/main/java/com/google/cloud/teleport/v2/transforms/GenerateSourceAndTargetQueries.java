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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateSourceAndTargetQueries
    extends PTransform<PCollection<KV<String, JsonNode>>, PCollection<KV<String, JsonNode>>> {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(GenerateSourceAndTargetQueries.class);

  private final String connectionsGcsLocation;

  public GenerateSourceAndTargetQueries(String connectionsGcsLocation) {
    this.connectionsGcsLocation = connectionsGcsLocation;
  }

  public static GenerateSourceAndTargetQueries create(String connectionsGcsLocation) {
    return new GenerateSourceAndTargetQueries(connectionsGcsLocation);
  }

  public PCollection<KV<String, JsonNode>> expand(
      PCollection<KV<String, JsonNode>> validationConfigurations) {
    return validationConfigurations.apply(
        ParDo.of(
            new DoFn<KV<String, JsonNode>, KV<String, JsonNode>>() {
              @ProcessElement
              public void processElement(
                  @Element KV<String, JsonNode> validationConfiguration,
                  OutputReceiver<KV<String, JsonNode>> out) {

                String validationConfigurationLocation = validationConfiguration.getKey();

                String rootConfigurationLocation =
                    URI.create(
                            connectionsGcsLocation.endsWith("/")
                                ? connectionsGcsLocation
                                : connectionsGcsLocation + "/")
                        .resolve("..")
                        .toString();

                ProcessBuilder processBuilder = new ProcessBuilder();

                processBuilder.environment().put("PSO_DV_CONFIG_HOME", rootConfigurationLocation);

                processBuilder.command(
                    "/bin/bash",
                    "-c",
                    "source /opt/professional-services-data-validator/env/bin/activate && "
                        + "data-validation configs run --dry-run --config-file "
                        + URI.create(rootConfigurationLocation + "validations/")
                            .relativize(URI.create(validationConfigurationLocation))
                            .toString());
                LOG.info("DVT dry run command: " + String.join(" ", processBuilder.command()));

                Process process = null;
                try {
                  process = processBuilder.start();
                } catch (IOException ex) {
                  throw new RuntimeException("Error starting DVT dry run process.", ex);
                }
                String stdOut =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))
                        .lines()
                        .peek(LOG::info)
                        .collect(Collectors.joining("\n"));
                String stdErr =
                    new BufferedReader(new InputStreamReader(process.getErrorStream()))
                        .lines()
                        .peek(LOG::error)
                        .collect(Collectors.joining("\n"));

                int exitCode;
                try {
                  exitCode = process.waitFor();
                } catch (InterruptedException ex) {
                  throw new RuntimeException(
                      "Received interrupt while executing DVT dry run process.", ex);
                }
                if (exitCode != 0) {
                  throw new RuntimeException("DVT dry run process returned non-zero exit code.");
                }

                ObjectMapper mapper = new ObjectMapper();
                JsonNode queries = null;
                try {
                  queries = mapper.readValue(stdOut, JsonNode.class);
                } catch (JsonProcessingException ex) {
                  throw new RuntimeException("Error deserializing DVT dry run JSON output.", ex);
                } catch (IOException ex) {
                  throw new RuntimeException("Error reading DVT dry run JSON output.", ex);
                }

                out.output(KV.of(validationConfigurationLocation, queries));
              }
            }));
  }
}
