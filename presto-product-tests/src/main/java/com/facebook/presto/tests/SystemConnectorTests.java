/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests;

import com.google.common.collect.ImmutableList;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.JDBCType;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.tests.TestGroups.JDBC;
import static com.facebook.presto.tests.TestGroups.SYSTEM_CONNECTOR;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.VARCHAR;

public class SystemConnectorTests
        extends ProductTest
{
    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectRuntimeNodes()
    {
        String sql = "SELECT node_id, http_uri, node_version, state FROM system.runtime.nodes";
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectRuntimeQueries()
    {
        String sql = "SELECT" +
                "  query_id," +
                "  state," +
                "  user," +
                "  query," +
                "  resource_group_id," +
                "  queued_time_ms," +
                "  analysis_time_ms," +
                "  created," +
                "  started," +
                "  last_heartbeat," +
                "  'end' " +
                "FROM system.runtime.queries";
        JDBCType arrayType = usingTeradataJdbcDriver(defaultQueryExecutor().getConnection()) ? VARCHAR : ARRAY;
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR, arrayType,
                        BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectRuntimeTasks()
    {
        String sql = "SELECT" +
                "  node_id," +
                "  task_id," +
                "  stage_id," +
                "  query_id," +
                "  state," +
                "  splits," +
                "  queued_splits," +
                "  running_splits," +
                "  completed_splits," +
                "  split_scheduled_time_ms," +
                "  split_cpu_time_ms," +
                "  split_blocked_time_ms," +
                "  raw_input_bytes," +
                "  raw_input_rows," +
                "  processed_input_bytes," +
                "  processed_input_rows," +
                "  output_bytes," +
                "  output_rows," +
                "  physical_written_bytes," +
                "  created," +
                "  start," +
                "  last_heartbeat," +
                "  'end' " +
                "FROM SYSTEM.runtime.tasks";
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                        BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT,
                        BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectMetadataCatalogs()
    {
        String sql = "select catalog_name, connector_id, connector_name from system.metadata.catalogs";
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectJdbcColumns()
    {
        try {
            String hiveSQL = "select table_name, column_name from system.jdbc.columns where table_cat = 'hive' AND table_schem = 'default'";
            String icebergSQL = "select table_name, column_name from system.jdbc.columns where table_cat = 'iceberg' AND table_schem = 'default'";

            List<QueryAssert.Row> preexistingHiveColumns = onPresto().executeQuery(hiveSQL).rows().stream()
                    .map(list -> row(list.toArray()))
                    .collect(Collectors.toList());

            List<QueryAssert.Row> preexistingIcebergColumns = onPresto().executeQuery(icebergSQL).rows().stream()
                    .map(list -> row(list.toArray()))
                    .collect(Collectors.toList());

            onPresto().executeQuery("CREATE TABLE hive.default.test_hive_system_jdbc_columns (_double DOUBLE)");
            onPresto().executeQuery("CREATE TABLE iceberg.default.test_iceberg_system_jdbc_columns (_string VARCHAR, _integer INTEGER)");

            assertThat(onPresto().executeQuery(hiveSQL))
                    .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                            .addAll(preexistingHiveColumns)
                            .add(row("test_hive_system_jdbc_columns", "_double"))
                            .build());

            assertThat(onPresto().executeQuery(icebergSQL))
                    .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                            .addAll(preexistingIcebergColumns)
                            .add(row("test_iceberg_system_jdbc_columns", "_string"))
                            .add(row("test_iceberg_system_jdbc_columns", "_integer"))
                            .build());
        }
        finally {
            onPresto().executeQuery("DROP TABLE IF EXISTS hive.default.test_hive_system_jdbc_columns");
            onPresto().executeQuery("DROP TABLE IF EXISTS iceberg.default.test_iceberg_system_jdbc_columns");
        }
    }
}
