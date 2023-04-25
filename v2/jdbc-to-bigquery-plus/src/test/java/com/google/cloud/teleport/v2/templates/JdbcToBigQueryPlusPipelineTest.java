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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.options.JdbcToBigQueryPlusPipelineOptions;
import com.google.cloud.teleport.v2.values.JdbcToBigQueryPlusTask;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JdbcToBigQueryPlusPipeline}. */
@RunWith(JUnit4.class)
public class JdbcToBigQueryPlusPipelineTest {
  private static final String PROJECT = "test-project";
  private static final String DATASET = "test-dataset";

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private String sourceDatabaseConnectionUrl;
  private DataSourceConfiguration sourceDataSourceConfiguration;
  private FakeDatasetService fakeDatasetService;
  private BigQueryServices bigQueryServices;
  private JdbcToBigQueryPlusPipelineOptions options;

  @Before
  public void setUp() throws SQLException, IOException, InterruptedException {
    // setup JDBC
    String sourceDatabaseJdbcDriverClassName = "org.apache.derby.jdbc.EmbeddedDriver";
    sourceDatabaseConnectionUrl = "jdbc:derby:memory:jdc2bq";
    sourceDataSourceConfiguration =
        DataSourceConfiguration.create(
            sourceDatabaseJdbcDriverClassName, sourceDatabaseConnectionUrl);
    System.setProperty("derby.stream.error.field", "System.out"); // log to console, not a log file
    try (Connection conn =
            DriverManager.getConnection(sourceDatabaseConnectionUrl + ";create=true");
        Statement statement = conn.createStatement()) {
      statement.execute(
          "create table customers ("
              + "customer_id integer primary key, "
              + "first_name varchar(30) not null, "
              + "last_name varchar(30) not null, "
              + "created_ts timestamp not null,"
              + "updated_ts timestamp, "
              + "deleted_ts timestamp);");
      statement.execute(
          "INSERT INTO customers VALUES (1, 'Lilyana', "
              + "'Browning','2023-01-09 21:14:29','2023-01-09 21:14:29', NULL)");
      statement.execute(
          "INSERT INTO customers VALUES (2, 'Carter', "
              + "'Holt','2023-01-09 22:33:47','2023-01-09 22:33:47', NULL)");
      statement.execute(
          "INSERT INTO customers VALUES (3, 'Darien', "
              + "'Becker','2023-01-10 00:07:17','2023-01-10 00:07:17', NULL)");

      statement.execute(
          "create table orders ("
              + "order_id integer primary key, "
              + "customer_id integer not null, "
              + "status varchar(12) not null, "
              + "created_ts timestamp not null,"
              + "updated_ts timestamp, "
              + "deleted_ts timestamp);");
      statement.execute(
          "INSERT INTO orders VALUES (1, 1, 'SHIPPED', "
              + "'2023-01-09 22:13:19','2023-01-09 22:13:19', NULL)");
      statement.execute(
          "INSERT INTO orders VALUES (2, 1, 'ORDERED', "
              + "'2023-01-09 23:54:39','2023-01-09 23:54:39', NULL)");
      statement.execute(
          "INSERT INTO orders VALUES (3, 2, 'SHIPPED', "
              + "'2023-01-09 23:37:15','2023-01-09 23:37:15', NULL)");
      statement.execute(
          "INSERT INTO orders VALUES (4, 3, 'SHIPPED', "
              + "'2023-01-10 00:08:16','2023-01-10 00:08:16', NULL)");
      statement.execute(
          "INSERT INTO orders VALUES (5, 3, 'ORDERED', "
              + "'2023-01-10 00:09:47','2023-01-10 00:09:47', NULL)");
      statement.execute(
          "INSERT INTO orders VALUES (6, 3, 'ORDERED', "
              + "'2023-01-10 00:10:40','2023-01-10 00:10:40', NULL)");
    }

    // setup BQ
    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);

    TableSchema customersBqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("customer_id").setType("integer"),
                    new TableFieldSchema().setName("first_name").setType("string"),
                    new TableFieldSchema().setName("last_name").setType("string"),
                    new TableFieldSchema().setName("created_ts").setType("timestamp"),
                    new TableFieldSchema().setName("updated_ts").setType("timestamp"),
                    new TableFieldSchema().setName("deleted_ts").setType("timestamp")));
    fakeDatasetService.createTable(
        new Table()
            .setTableReference(
                new TableReference()
                    .setProjectId(PROJECT)
                    .setDatasetId(DATASET)
                    .setTableId("customers"))
            .setSchema(customersBqSchema));

    TableSchema ordersBqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("order_id").setType("integer"),
                    new TableFieldSchema().setName("customer_id").setType("integer"),
                    new TableFieldSchema().setName("status").setType("string"),
                    new TableFieldSchema().setName("created_ts").setType("timestamp"),
                    new TableFieldSchema().setName("updated_ts").setType("timestamp"),
                    new TableFieldSchema().setName("deleted_ts").setType("timestamp")));
    fakeDatasetService.createTable(
        new Table()
            .setTableReference(
                new TableReference()
                    .setProjectId(PROJECT)
                    .setDatasetId(DATASET)
                    .setTableId("orders"))
            .setSchema(ordersBqSchema));

    FakeJobService fakeJobService = new FakeJobService();
    bigQueryServices =
        new FakeBigQueryServices()
            .withJobService(fakeJobService)
            .withDatasetService(fakeDatasetService);

    // Setup options.
    options = PipelineOptionsFactory.create().as(JdbcToBigQueryPlusPipelineOptions.class);
    options.setSourceDatabaseJdbcDriverClassName(sourceDatabaseJdbcDriverClassName);
    options.setJdbcToBigQueryPlusStagingGcsLocation(
        ValueProvider.StaticValueProvider.of(tmp.newFolder("bq-tmp").getAbsolutePath()));
    // Not used by tests.
    options.setSourceDatabaseConnectionURLSecretId("");
    options.setJdbcToBigQueryPlusConfigurationGcsLocation("");
  }

  @After
  public void tearDown() throws SQLException {
    try (Connection conn = DriverManager.getConnection(sourceDatabaseConnectionUrl);
        Statement statement = conn.createStatement()) {
      statement.execute("DROP TABLE customers");
      statement.execute("DROP TABLE orders");
    }
  }

  @Test
  public void testE2E() throws IOException, InterruptedException {
    // Arrange
    List<JdbcToBigQueryPlusTask> tasks;
    Pipeline p = Pipeline.create(options);
    JdbcToBigQueryPlusPipeline.buildPipeline(
        p,
        sourceDataSourceConfiguration,
        tasks,
        JdbcToBigQueryPlusPipeline.buildBigQueryIOWrite(options)
            .withTestServices(bigQueryServices));

    // Act
    p.run().waitUntilFinish();

    // Assert
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    assertThat(fakeDatasetService.getAllRows(PROJECT, DATASET, "customers"))
        .isEqualTo(
            ImmutableList.of(
                new TableRow()
                    .set("customer_id", 1)
                    .set("first_name", "Lilyana")
                    .set("last_name", "Browning")
                    .set("created_ts", dateFormat.parse("2023-01-09 21:14:29"))
                    .set("updated_ts", dateFormat.parse("2023-01-09 21:14:29"))
                    .set("deleted_ts", null),
                new TableRow()
                    .set("customer_id", 2)
                    .set("first_name", "Carter")
                    .set("last_name", "Holt")
                    .set("created_ts", dateFormat.parse("2023-01-09 22:33:47"))
                    .set("updated_ts", dateFormat.parse("2023-01-09 22:33:47"))
                    .set("deleted_ts", null),
                new TableRow()
                    .set("customer_id", 3)
                    .set("first_name", "Darien")
                    .set("last_name", "Becker")
                    .set("created_ts", dateFormat.parse("2023-01-10 00:07:17"))
                    .set("updated_ts", dateFormat.parse("2023-01-10 00:07:17"))
                    .set("deleted_ts", null)));
  }
}
