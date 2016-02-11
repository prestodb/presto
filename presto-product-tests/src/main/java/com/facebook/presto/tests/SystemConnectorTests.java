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

import static com.facebook.presto.tests.TestGroups.SYSTEM_CONNECTOR;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.TIMESTAMP;

public class SystemConnectorTests
        extends ProductTest
{
    @Test(groups = SYSTEM_CONNECTOR)
    public void selectRuntimeNodes()
    {
        assertThat(query("SELECT node_id, http_uri, node_version, state FROM system.runtime.nodes"))
                .hasColumns(LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR)
                .hasAnyRows();
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectRuntimeQueries()
    {
        assertThat(query("SELECT" +
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
                "FROM system.runtime.queries"))
                .hasColumns(LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR,
                        BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, LONGNVARCHAR)
                .hasAnyRows();
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectRuntimeTasks()
    {
        assertThat(query("SELECT" +
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
                "FROM SYSTEM.runtime.tasks"))
                .hasColumns(LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR, LONGNVARCHAR,
                        BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT,
                        BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, LONGNVARCHAR)
                .hasAnyRows();
    }

    @Test(groups = SYSTEM_CONNECTOR)
    public void selectMetadataCatalogs()
    {
        assertThat(query("select catalog_name, connector_id from system.metadata.catalogs"))
                .hasColumns(LONGNVARCHAR, LONGNVARCHAR)
                .hasAnyRows();
    }
}
