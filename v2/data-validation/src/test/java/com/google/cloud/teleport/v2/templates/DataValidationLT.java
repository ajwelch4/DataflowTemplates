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

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerException;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
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
    private static final String SPEC_PATH = MoreObjects.firstNonNull(
            TestProperties.specPath(), "gs://dataflow-templates/latest/DataValidation");
    private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
    private static final String TEST_CLASS_NAME = DataValidationLT.class.getSimpleName().toLowerCase();
    // 370,000,000 messages of the given schema make up approximately 100GB
    private static final String NUM_MESSAGES = "370000000";
    private static final String INPUT_PCOLLECTION = "Read source/Read ranges/ParDo(Read)/ParMultiDo(Read).out0";
    private static final String OUTPUT_PCOLLECTION = "Validate/BeamCalcRel_19/ParMultiDo(Calc).out0";
    private static final String SOURCE_TABLE_NAME = "source_table";
    private static final String TARGET_TABLE_NAME = "target_table";
    private static BigQueryResourceManager bigQueryResourceManager;
    private static ArtifactClient gcsClient;

    @BeforeClass
    public static void setup() throws IOException, BigQueryResourceManagerException, InterruptedException {
        Storage storageClient = createStorageClient(CREDENTIALS);
        gcsClient = GcsArtifactClient.builder(storageClient, ARTIFACT_BUCKET, TEST_CLASS_NAME).build();

        String generatorSchemaPath = getFullGcsPath(
                ARTIFACT_BUCKET,
                gcsClient
                        .uploadArtifact(
                                "config/generatorSchema.json",
                                Resources.getResource("generatorSchema.json").getPath())
                        .name());

        String generatorDataPath = getFullGcsPath(ARTIFACT_BUCKET, TEST_CLASS_NAME, gcsClient.runId(), "data");

        DataGenerator dataGenerator = DataGenerator.builderWithSchemaLocation(TEST_CLASS_NAME, generatorSchemaPath)
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

        bigQueryResourceManager = DefaultBigQueryResourceManager.builder(TEST_CLASS_NAME, PROJECT).build();

        bigQueryResourceManager.createDataset(REGION);

        Schema bigQuerySchema = Schema.of(
                Field.of("primary_key", StandardSQLTypeName.STRING),
                Field.of("partition_key", StandardSQLTypeName.INT64),
                Field.of("payload", StandardSQLTypeName.STRING));

        RangePartitioning partitioning = RangePartitioning.newBuilder()
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

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.environment().put("PSO_DV_CONFIG_HOME", System.getProperty("configGcsLocation"));
        processBuilder.command(
                "/bin/bash",
                "-c",
                "data-validation --verbose generate-table-partitions -sc "
                        + System.getProperty("sourceConnection")
                        + " -tc "
                        + System.getProperty("targetConnection")
                        + " -tbls "
                        + PROJECT
                        + "."
                        + bigQueryResourceManager.getDatasetId()
                        + "."
                        + SOURCE_TABLE_NAME
                        + "="
                        + PROJECT
                        + "."
                        + bigQueryResourceManager.getDatasetId()
                        + "."
                        + TARGET_TABLE_NAME
                        + " --primary-keys primary_key --hash '*'");
        Process process = processBuilder.start();
        List<String> stdOut = new BufferedReader(new InputStreamReader(process.getInputStream()))
                .lines()
                .peek(LOG::info)
                .collect(Collectors.toList());
        List<String> stdErr = new BufferedReader(new InputStreamReader(process.getErrorStream()))
                .lines()
                .peek(LOG::error)
                .collect(Collectors.toList());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException(
                    "data-validation generate-table-partitions exited with non-zero" + " exit code: " + exitCode);
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
        environment.put("PSO_DV_CONFIG_HOME", System.getProperty("configGcsLocation"));
        LOG.debug(
                "Environment: "
                        + environment.keySet().stream()
                                .<String>map(key -> key + "=" + environment.get(key))
                                .collect(Collectors.joining(", ", "{", "}")));

        processBuilder.command(
                "/bin/bash",
                "-c",
                "data-validation --verbose validate row -sc "
                        + System.getProperty("sourceConnection")
                        + " -tc "
                        + System.getProperty("targetConnection")
                        + " -tbls "
                        + PROJECT
                        + "."
                        + bigQueryResourceManager.getDatasetId()
                        + "."
                        + SOURCE_TABLE_NAME
                        + "="
                        + PROJECT
                        + "."
                        + bigQueryResourceManager.getDatasetId()
                        + "."
                        + TARGET_TABLE_NAME
                        + " --primary-keys primary_key --hash '*' --filter-status fail");
        LOG.debug("Command: " + String.join(" ", processBuilder.command()));

        Instant start = Instant.now();

        // Act.
        Process process = processBuilder.start();
        List<String> stdOut = new BufferedReader(new InputStreamReader(process.getInputStream()))
                .lines()
                .peek(LOG::info)
                .collect(Collectors.toList());
        List<String> stdErr = new BufferedReader(new InputStreamReader(process.getErrorStream()))
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
        LaunchConfig options = LaunchConfig.builder(testName, SPEC_PATH)
                .addEnvironment("tempLocation", System.getProperty("tempLocation"))
                .addEnvironment("serviceAccountEmail", System.getProperty("serviceAccountEmail"))
                .addEnvironment(
                        "additionalExperiments",
                        Stream.of(System.getProperty("additionalExperiments").split(",", -1))
                                .collect(Collectors.toList()))
                .addEnvironment("machineType", System.getProperty("machineType"))
                .addEnvironment("numWorkers", Integer.parseInt(System.getProperty("numWorkers")))
                .addEnvironment("maxWorkers", Integer.parseInt(System.getProperty("maxWorkers")))
                .addParameter("sdkContainerImage", System.getProperty("sdkContainerImage"))
                .addParameter("connectionsGcsLocation", System.getProperty("connectionsGcsLocation"))
                .addParameter("configurationGcsLocationsPattern",
                        System.getProperty("configurationGcsLocationsPattern"))
                .addParameter("resultsStagingGcsLocation", System.getProperty("resultsStagingGcsLocation"))
                .addParameter("fullyQualifiedResultsTableName", System.getProperty("fullyQualifiedResultsTableName"))
                .build();

        // Act.
        LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
        assertThatPipeline(info).isRunning();
        Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

        // Assert.
        assertThatResult(result).isLaunchFinished();

        // Export results.
        exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
    }
}
