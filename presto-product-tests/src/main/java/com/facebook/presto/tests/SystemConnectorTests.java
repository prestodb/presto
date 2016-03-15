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

import com.teradata.tempto.ProductTest;
import org.testng.annotations.Test;

import java.sql.Connection;

import static com.facebook.presto.tests.TestGroups.SYSTEM_CONNECTOR;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingPrestoJdbcDriver;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.VARCHAR;

public class SystemConnectorTests
        extends ProductTest
{
    @Test(groups = SYSTEM_CONNECTOR)
    public void selectRuntimeNodes()
    {
        Connection connection = defaultQueryExecutor().getConnection();
        String sql = "SELECT node_id, http_uri, node_version, state FROM system.runtime.nodes";
        if (usingPrestoJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR)
                    .hasAnyRows();
        }
        else if (usingTeradataJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                    .hasAnyRows();
        }
        else {
            throw new IllegalStateException();
        }
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectRuntimeQueries()
    {
        Connection connection = defaultQueryExecutor().getConnection();
        String sql = "SELECT" +
                "  node_id," +
                "  query_id," +
                "  state," +
                "  user," +
                "  query," +
                "  queued_time_ms," +
                "  analysis_time_ms," +
                "  distributed_planning_time_ms," +
                "  created," +
                "  started," +
                "  last_heartbeat," +
                "  'end' " +
                "FROM system.runtime.queries";
        if (usingPrestoJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR,
                            BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, LONGNVARCHAR)
                    .hasAnyRows();
        }
        else if (usingTeradataJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                            BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, VARCHAR)
                    .hasAnyRows();
        }
        else {
            throw new IllegalStateException();
        }
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectRuntimeTasks()
    {
        Connection connection = defaultQueryExecutor().getConnection();
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
                "  split_user_time_ms," +
                "  split_blocked_time_ms," +
                "  raw_input_bytes," +
                "  raw_input_rows," +
                "  processed_input_bytes," +
                "  processed_input_rows," +
                "  output_bytes," +
                "  output_rows," +
                "  created," +
                "  start," +
                "  last_heartbeat," +
                "  'end' " +
                "FROM SYSTEM.runtime.tasks";
        if (usingPrestoJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR,
                            BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT,
                            BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, LONGNVARCHAR)
                    .hasAnyRows();
        }
        else if (usingTeradataJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                            BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT,
                            BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, VARCHAR)
                    .hasAnyRows();
        }
        else {
            throw new IllegalStateException();
        }
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectMetadataCatalogs()
    {
        Connection connection = defaultQueryExecutor().getConnection();
        String sql = "select catalog_name, connector_id from system.metadata.catalogs";

        if (usingPrestoJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(LONGNVARCHAR, LONGNVARCHAR)
                    .hasAnyRows();
        }
        else if (usingTeradataJdbcDriver(connection)) {
            assertThat(query(sql))
                    .hasColumns(VARCHAR, VARCHAR)
                    .hasAnyRows();
        }
        else {
            throw new IllegalStateException();
        }
    }
}
