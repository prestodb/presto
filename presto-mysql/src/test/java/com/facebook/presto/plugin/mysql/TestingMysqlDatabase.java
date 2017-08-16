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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;

public class TestingMysqlDatabase
        implements AutoCloseable
{
    public static final String CONNECTOR_ID = "test";
    private final TestingMySqlServer mysqlServer;
    private final JdbcClient jdbcClient;

    public TestingMysqlDatabase(String username, String password)
            throws Exception
    {
        this.mysqlServer = new TestingMySqlServer(username, password, "example", "exampleCamelCase");

        jdbcClient = new MySqlClient(
                new JdbcConnectorId(CONNECTOR_ID),
                new BaseJdbcConfig().setConnectionUrl(mysqlServer.getJdbcUrl()),
                new MySqlConfig());

        loadInitialData();
    }

    protected void loadInitialData()
            throws Exception
    {
        Connection connection = DriverManager.getConnection(mysqlServer.getJdbcUrl());

        connection.createStatement().execute("CREATE TABLE example.numbers(text VARCHAR(255) DEFAULT NULL, text_short VARCHAR(32) DEFAULT NULL, value bigint(20) DEFAULT NULL)");
        connection.createStatement().execute("CREATE TABLE exampleCamelCase.camelCaseNumbers(text VARCHAR(255) DEFAULT NULL, text_short VARCHAR(32) DEFAULT NULL, value bigint(20) DEFAULT NULL)");

        connection.createStatement().execute("INSERT INTO example.numbers(text, text_short, value) VALUES " +
                "('one', 'one', 1)," +
                "('two', 'two', 2)," +
                "('three', 'three', 3)," +
                "('ten', 'ten', 10)," +
                "('eleven', 'eleven', 11)," +
                "('twelve', 'twelve', 12)" +
                "");

        connection.createStatement().execute("INSERT INTO exampleCamelCase.camelCaseNumbers(text, text_short, value) VALUES " +
                "('one', 'one', 1)," +
                "('two', 'two', 2)," +
                "('three', 'three', 3)," +
                "('ten', 'ten', 10)," +
                "('eleven', 'eleven', 11)," +
                "('twelve', 'twelve', 12)" +
                "");
    }

    @Override
    public void close()
    {
        mysqlServer.close();
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public JdbcSplit getSplit(String schemaName, String tableName)
            throws InterruptedException
    {
        JdbcTableHandle jdbcTableHandle = jdbcClient.getTableHandle(new SchemaTableName(schemaName, tableName));
        JdbcTableLayoutHandle jdbcLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.all());
        ConnectorSplitSource splits = jdbcClient.getSplits(jdbcLayoutHandle);
        return (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(1000)));
    }

    public Map<String, JdbcColumnHandle> getColumnHandles(String schemaName, String tableName)
    {
        JdbcTableHandle tableHandle = jdbcClient.getTableHandle(new SchemaTableName(schemaName, tableName));
        List<JdbcColumnHandle> columns = jdbcClient.getColumns(tableHandle);
        checkArgument(columns != null, "table not found: %s.%s", schemaName, tableName);

        ImmutableMap.Builder<String, JdbcColumnHandle> columnHandles = ImmutableMap.builder();
        for (JdbcColumnHandle column : columns) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }
}
