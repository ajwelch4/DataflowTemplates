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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.ArtifactClient;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManagerException;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performance tests for Data Validation template. */
@TemplateLoadTest(DataValidation.class)
@RunWith(JUnit4.class)
public class DataValidationLT extends TemplateLoadTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(DataValidationLT.class);
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/DataValidation");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_CLASS_NAME =
      DataValidationLT.class.getSimpleName().toLowerCase();
  // 370,000,000 messages of the given schema make up approximately 100GB
  private static final String NUM_MESSAGES = "370000000";
  private static final String INPUT_PCOLLECTION =
      "Read source/Read ranges/ParDo(Read)/ParMultiDo(Read).out0";
  private static final String OUTPUT_PCOLLECTION = "Validate/BeamCalcRel_19/ParMultiDo(Calc).out0";
  private static final String SOURCE_CONNECTION_NAME = "source_connection";
  private static final String SOURCE_TABLE_NAME = "source_table";
  private static final String TARGET_CONNECTION_NAME = "target_connection";
  private static final String TARGET_TABLE_NAME = "target_table";
  private static BigQueryResourceManager bigQueryResourceManager;
  private static ArtifactClient gcsClient;

  @BeforeClass
  public static void setup()
      throws IOException, BigQueryResourceManagerException, InterruptedException {
    gcsClient =
        GcsResourceManager.builder(
                ARTIFACT_BUCKET, TEST_CLASS_NAME, TestProperties.googleCredentials())
            .build();

    String generatorSchemaPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            gcsClient
                .uploadArtifact(
                    "config/generatorSchema.json",
                    Resources.getResource("generatorSchema.json").getPath())
                .name());

    String generatorDataPath =
        getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "data");

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(TEST_CLASS_NAME, generatorSchemaPath)
            .setQPS("1000000")
            .setMessagesLimit(NUM_MESSAGES)
            .setSinkType("GCS")
            .setOutputDirectory(generatorDataPath)
            .setOutputType("JSON")
            .setWorkerMachineType("n2-highmem-2")
            .setNumShards("50")
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(60));

    bigQueryResourceManager =
        BigQueryResourceManager.builder(
                TEST_CLASS_NAME, project, TestProperties.googleCredentials())
            .build();

    bigQueryResourceManager.createDataset(region);

    Schema bigQuerySchema =
        Schema.of(
            Field.of("primary_key", StandardSQLTypeName.STRING),
            Field.of("partition_key", StandardSQLTypeName.INT64),
            Field.of("payload", StandardSQLTypeName.STRING));

    RangePartitioning partitioning =
        RangePartitioning.newBuilder()
            .setField("partition_key")
            .setRange(
                RangePartitioning.Range.newBuilder()
                    .setStart(1L)
                    .setInterval(1L)
                    .setEnd(1000L)
                    .build())
            .build();

    bigQueryResourceManager.createTable(SOURCE_TABLE_NAME, bigQuerySchema, partitioning);

    bigQueryResourceManager.createTable(TARGET_TABLE_NAME, bigQuerySchema, partitioning);

    // Explicitly passing the column list ensures auto-detect is not enabled.
    // Auto-detect was occasionally detecting our alphanumeric payload as BYTES not
    // STRING.
    String bigQueryColumnList = "";
    for (Field field : bigQuerySchema.getFields()) {
      bigQueryColumnList += field.getName() + " " + field.getType().name() + ",";
    }
    bigQueryResourceManager.runQuery(
        "load data into "
            + bigQueryResourceManager.getDatasetId()
            + "."
            + SOURCE_TABLE_NAME
            + "("
            + bigQueryColumnList
            + ")"
            + " from files(format='JSON', uris=['"
            + generatorDataPath
            + "/*'])");

    bigQueryResourceManager.runQuery(
        "insert into "
            + bigQueryResourceManager.getDatasetId()
            + "."
            + TARGET_TABLE_NAME
            + " select * from "
            + bigQueryResourceManager.getDatasetId()
            + "."
            + SOURCE_TABLE_NAME);

    ProcessBuilder dvtConnectionsAddSourceProcessBuilder = new ProcessBuilder();
    dvtConnectionsAddSourceProcessBuilder
        .environment()
        .put(
            "PSO_DV_CONFIG_HOME",
            getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "config"));
    dvtConnectionsAddSourceProcessBuilder.command(
        "/bin/bash",
        "-c",
        "data-validation --verbose connections add "
            + "--connection-name "
            + SOURCE_CONNECTION_NAME
            + " BigQuery "
            + "--project-id "
            + project);
    LOG.info(
        "Starting process with command: "
            + String.join(" ", dvtConnectionsAddSourceProcessBuilder.command()));
    Process dvtConnectionsAddSourceProcess = dvtConnectionsAddSourceProcessBuilder.start();
    List<String> dvtConnectionsAddSourceStdOut =
        new BufferedReader(new InputStreamReader(dvtConnectionsAddSourceProcess.getInputStream()))
            .lines()
            .peek(LOG::info)
            .collect(Collectors.toList());
    List<String> dvtConnectionsAddSourceStdErr =
        new BufferedReader(new InputStreamReader(dvtConnectionsAddSourceProcess.getErrorStream()))
            .lines()
            .peek(LOG::error)
            .collect(Collectors.toList());
    int dvtConnectionsAddSourceExitCode = dvtConnectionsAddSourceProcess.waitFor();
    if (dvtConnectionsAddSourceExitCode != 0) {
      throw new RuntimeException(
          "data-validation connections add exited with non-zero exit code: "
              + dvtConnectionsAddSourceExitCode);
    }

    ProcessBuilder dvtConnectionsAddTargetProcessBuilder = new ProcessBuilder();
    dvtConnectionsAddTargetProcessBuilder
        .environment()
        .put(
            "PSO_DV_CONFIG_HOME",
            getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "config"));
    dvtConnectionsAddTargetProcessBuilder.command(
        "/bin/bash",
        "-c",
        "data-validation --verbose connections add "
            + "--connection-name "
            + TARGET_CONNECTION_NAME
            + " BigQuery "
            + "--project-id "
            + project);
    LOG.info(
        "Starting process with command: "
            + String.join(" ", dvtConnectionsAddTargetProcessBuilder.command()));
    Process dvtConnectionsAddTargetProcess = dvtConnectionsAddTargetProcessBuilder.start();
    List<String> dvtConnectionsAddTargetStdOut =
        new BufferedReader(new InputStreamReader(dvtConnectionsAddTargetProcess.getInputStream()))
            .lines()
            .peek(LOG::info)
            .collect(Collectors.toList());
    List<String> dvtConnectionsAddTargetStdErr =
        new BufferedReader(new InputStreamReader(dvtConnectionsAddTargetProcess.getErrorStream()))
            .lines()
            .peek(LOG::error)
            .collect(Collectors.toList());
    int dvtConnectionsAddTargetExitCode = dvtConnectionsAddTargetProcess.waitFor();
    if (dvtConnectionsAddTargetExitCode != 0) {
      throw new RuntimeException(
          "data-validation connections add exited with non-zero exit code: "
              + dvtConnectionsAddTargetExitCode);
    }

    ProcessBuilder dvtGenerateTablePartitionsProcessBuilder = new ProcessBuilder();
    dvtGenerateTablePartitionsProcessBuilder
        .environment()
        .put(
            "PSO_DV_CONFIG_HOME",
            getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "config"));
    dvtGenerateTablePartitionsProcessBuilder.command(
        "/bin/bash",
        "-c",
        "data-validation --verbose generate-table-partitions -sc "
            + SOURCE_CONNECTION_NAME
            + " -tc "
            + TARGET_CONNECTION_NAME
            + " -tbls "
            + project
            + "."
            + bigQueryResourceManager.getDatasetId()
            + "."
            + SOURCE_TABLE_NAME
            + "="
            + project
            + "."
            + bigQueryResourceManager.getDatasetId()
            + "."
            + TARGET_TABLE_NAME
            + " --primary-keys primary_key --hash '*' "
            + "--config-dir '"
            + getFullGcsPath(
                ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "config", "validations")
            + "' --partition-num 100 --labels tag=test,class="
            + TEST_CLASS_NAME
            + " --format table --filter-status fail");
    LOG.info(
        "Starting process with command: "
            + String.join(" ", dvtGenerateTablePartitionsProcessBuilder.command()));
    Process dvtGenerateTablePartitionsProcess = dvtGenerateTablePartitionsProcessBuilder.start();
    List<String> dvtGenerateTablePartitionsStdOut =
        new BufferedReader(
                new InputStreamReader(dvtGenerateTablePartitionsProcess.getInputStream()))
            .lines()
            .peek(LOG::info)
            .collect(Collectors.toList());
    List<String> dvtGenerateTablePartitionsStdErr =
        new BufferedReader(
                new InputStreamReader(dvtGenerateTablePartitionsProcess.getErrorStream()))
            .lines()
            .peek(LOG::error)
            .collect(Collectors.toList());
    int dvtGenerateTablePartitionsExitCode = dvtGenerateTablePartitionsProcess.waitFor();
    if (dvtGenerateTablePartitionsExitCode != 0) {
      throw new RuntimeException(
          "data-validation generate-table-partitions exited with non-zero"
              + " exit code: "
              + dvtGenerateTablePartitionsExitCode);
    }
  }

  @AfterClass
  public static void teardown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager, gcsClient);
  }

  @Test
  public void testCli() throws IOException, InterruptedException {
    // Arrange.
    ProcessBuilder processBuilder = new ProcessBuilder();

    Map<String, String> environment = processBuilder.environment();
    environment.put(
        "PSO_DV_CONFIG_HOME",
        getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "config"));
    LOG.debug(
        "Environment: "
            + environment.keySet().stream()
                .<String>map(key -> key + "=" + environment.get(key))
                .collect(Collectors.joining(", ", "{", "}")));

    processBuilder.command(
        "/bin/bash",
        "-c",
        "data-validation --verbose validate row -sc "
            + SOURCE_CONNECTION_NAME
            + " -tc "
            + TARGET_CONNECTION_NAME
            + " -tbls "
            + project
            + "."
            + bigQueryResourceManager.getDatasetId()
            + "."
            + SOURCE_TABLE_NAME
            + "="
            + project
            + "."
            + bigQueryResourceManager.getDatasetId()
            + "."
            + TARGET_TABLE_NAME
            + " --primary-keys primary_key --hash '*' --filter-status fail");
    LOG.debug("Command: " + String.join(" ", processBuilder.command()));

    Instant start = Instant.now();

    // Act.
    Process process = processBuilder.start();
    List<String> stdOut =
        new BufferedReader(new InputStreamReader(process.getInputStream()))
            .lines()
            .peek(LOG::info)
            .collect(Collectors.toList());
    List<String> stdErr =
        new BufferedReader(new InputStreamReader(process.getErrorStream()))
            .lines()
            .peek(LOG::error)
            .collect(Collectors.toList());
    int exitCode = process.waitFor();

    // Assert.
    Instant finish = Instant.now();
    LOG.info("Elapsed time (seconds): " + Duration.between(start, finish).toSeconds());
    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  public void testDataflow() throws IOException, ParseException, InterruptedException {
    // Arrange.
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addEnvironment(
                "tempLocation",
                getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "tmp"))
            .addEnvironment("serviceAccountEmail", System.getProperty("serviceAccountEmail"))
            .addEnvironment(
                "additionalExperiments",
                Stream.of(System.getProperty("additionalExperiments").split(",", -1))
                    .collect(Collectors.toList()))
            .addEnvironment("machineType", System.getProperty("machineType"))
            .addEnvironment("numWorkers", Integer.parseInt(System.getProperty("numWorkers")))
            .addEnvironment("maxWorkers", Integer.parseInt(System.getProperty("maxWorkers")))
            .addParameter("sdkContainerImage", System.getProperty("sdkContainerImage"))
            .addParameter(
                "connectionsGcsLocation",
                getFullGcsPath(
                    ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "config", "connections"))
            .addParameter(
                "configurationGcsLocationsPattern",
                getFullGcsPath(
                    ARTIFACT_BUCKET,
                    TEST_CLASS_NAME,
                    gcsClient.runId(),
                    "config",
                    "validations",
                    "**",
                    "*.yaml"))
            .addParameter(
                "resultsStagingGcsLocation",
                getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "data"))
            .addParameter(
                "fullyQualifiedResultsTableName",
                System.getProperty("fullyQualifiedResultsTableName"))
            .build();

    // Act.
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert.
    assertThatResult(result).isLaunchFinished();

    // Export results.
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }
}
