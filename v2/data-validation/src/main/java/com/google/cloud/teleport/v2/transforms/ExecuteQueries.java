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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExecuteQueries.
 */
public class ExecuteQueries
        extends PTransform<PCollection<KV<String, JsonNode>>, PCollection<KV<List<String>, String>>> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ExecuteQueries.class);
    private final SystemType systemType;
    private final PCollectionView<Map<String, JsonNode>> connectionConfigurationsView;
    private final PCollectionView<Map<String, JsonNode>> validationConfigurationsView;

    public ExecuteQueries(
            SystemType systemType,
            PCollectionView<Map<String, JsonNode>> connectionConfigurationsView,
            PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
        this.systemType = systemType;
        this.connectionConfigurationsView = connectionConfigurationsView;
        this.validationConfigurationsView = validationConfigurationsView;
    }

    public static ExecuteQueries create(
            SystemType systemType,
            PCollectionView<Map<String, JsonNode>> connectionConfigurationsView,
            PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
        return new ExecuteQueries(
                systemType, connectionConfigurationsView, validationConfigurationsView);
    }

    public PCollection<KV<List<String>, String>> expand(
            PCollection<KV<String, JsonNode>> sourceAndTargetQueries) {
        return sourceAndTargetQueries.apply(
                ParDo.of(
                                ExecuteQueriesFn.create(
                                        systemType, connectionConfigurationsView, validationConfigurationsView))
                        .withSideInputs(connectionConfigurationsView, validationConfigurationsView));
    }

    public enum SystemType {
        SOURCE,
        TARGET;
    }

    private static class ExecuteQueriesFn
            extends DoFn<KV<String, JsonNode>, KV<List<String>, String>> {
        private static final int DEFAULT_FETCH_SIZE = 50_000;
        private final SystemType systemType;
        private final PCollectionView<Map<String, JsonNode>> connectionConfigurationsView;
        private final PCollectionView<Map<String, JsonNode>> validationConfigurationsView;
        private Map<String, DataSource> dataSources;
        private Map<String, Connection> connections;

        public ExecuteQueriesFn(
                SystemType systemType,
                PCollectionView<Map<String, JsonNode>> connectionConfigurationsView,
                PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
            dataSources = new HashMap<>();
            connections = new HashMap<>();
            this.systemType = systemType;
            this.connectionConfigurationsView = connectionConfigurationsView;
            this.validationConfigurationsView = validationConfigurationsView;
        }

        private static ExecuteQueriesFn create(
                SystemType systemType,
                PCollectionView<Map<String, JsonNode>> connectionConfigurationsView,
                PCollectionView<Map<String, JsonNode>> validationConfigurationsView) {
            return new ExecuteQueriesFn(
                    systemType, connectionConfigurationsView, validationConfigurationsView);
        }

        private Connection getConnection(String connectionName, JsonNode connectionConfig) {
            String driverClassName;
            String jdbcUrl;
            String sourceType = connectionConfig.get("source_type").asText();
            switch (sourceType) {
                case "BigQuery":
                    driverClassName =
                            "com.google.cloud.teleport.v2.bigquery.jdbc.vendor."
                                    + "com.simba.googlebigquery.jdbc.Driver";
                    // TODO: Make LargeResultDataset a pipeline option
                    jdbcUrl =
                            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
                                    + "OAuthType=3;QueryDialect=SQL;IgnoreTransactions=1;"
                                    + "AllowLargeResults=1;LargeResultDataset=_simba_jdbc_us;"
                                    + "ProjectId="
                                    + connectionConfig.get("project_id").asText()
                                    + ";";
                    break;
                case "MySQL":
                    driverClassName =
                            "com.mysql.cj.jdbc.Driver";
                    jdbcUrl =
                            "jdbc:mysql://"
                                    + connectionConfig.get("host").asText()
                                    + ":" + connectionConfig.get("port").asText() + "/"
                                    + connectionConfig.get("database").asText();
                    break;
                case "Spanner":
                    driverClassName =
                            "nl.topicus.jdbc.CloudSpannerDriver";
                    jdbcUrl =
                            "jdbc:cloudspanner://localhost;Project="
                                    + connectionConfig.get("project_id").asText()
                                    + ";Instance="
                                    + connectionConfig.get("instance_id").asText()
                                    + ";Database="
                                    + connectionConfig.get("database_id").asText();
                    break;
                default:
                    throw new RuntimeException(
                            "Invlaid source_type " + sourceType + " for connection " + connectionName + ".");
            }
            DataSource dataSource =
                    dataSources.computeIfAbsent(
                            connectionName,
                            k ->
                                    JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(
                                                    JdbcIO.DataSourceConfiguration.create(driverClassName, jdbcUrl)
                                                            .withUsername(connectionConfig.has("user") ? connectionConfig.get("user").asText() : null)
                                                            .withPassword(connectionConfig.has("password") ? connectionConfig.get("password").asText() : null))
                                            .apply(null));
            return connections.computeIfAbsent(
                    connectionName,
                    k -> {
                        try {
                            return dataSource.getConnection();
                        } catch (SQLException ex) {
                            throw new RuntimeException(
                                    "Error getting getting connection: " + connectionName + ".", ex);
                        }
                    });
        }

        @ProcessElement
        public void processElement(
                ProcessContext c,
                @Element KV<String, JsonNode> sourceAndTargetQuery,
                OutputReceiver<KV<List<String>, String>> out) {
            String validationConfigurationResourceId = sourceAndTargetQuery.getKey();
            String connectionName =
                    c.sideInput(validationConfigurationsView)
                            .get(validationConfigurationResourceId)
                            .get(systemType.name().toLowerCase())
                            .asText();
            Connection connection =
                    getConnection(
                            connectionName, c.sideInput(connectionConfigurationsView).get(connectionName));
            String query =
                    sourceAndTargetQuery.getValue().get(systemType.name().toLowerCase() + "_query").asText();
            LOG.info("Query for connection " + connectionName + " is :" + query);
            // PostgreSQL requires autocommit to be disabled to enable cursor streaming
            // see
            // https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
            try {
                connection.setAutoCommit(false);
                LOG.info("Autocommit has been disabled");
                try (PreparedStatement statement =
                             connection.prepareStatement(
                                     query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    statement.setFetchSize(DEFAULT_FETCH_SIZE);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        final int columnCount = resultSetMetaData.getColumnCount();
                        while (resultSet.next()) {
                            List<String> key = new ArrayList<>();
                            key.add(validationConfigurationResourceId);
                            for (int column = 1; column <= columnCount; ++column) {
                                if (resultSetMetaData.getColumnName(column).equals("hash__all") || resultSetMetaData.getColumnName(column).equals("`hash__all`")) {
                                    continue;
                                }
                              switch (resultSetMetaData.getColumnType(column)) {
                                  case Types.VARCHAR:
                                      key.add(resultSet.getString(column));
                                    break;
                                  case Types.INTEGER:
                                      key.add(String.valueOf(resultSet.getInt(column)));
                                    break;
                                  case Types.BIGINT:
                                      key.add(String.valueOf(resultSet.getLong(column)));
                                      break;
                                  default:
                                      LOG.info("Unknown column Type");
                                }
                            }
                            out.output(KV.of(key, resultSet.getString("hash__all")));
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException(
                        "Error using connection " + connectionName + " to execute SQL: " + query + ".", ex);
            }
        }
    }
}
