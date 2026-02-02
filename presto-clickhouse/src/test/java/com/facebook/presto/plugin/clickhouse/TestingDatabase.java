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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.spi.ConnectorSession;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

final class TestingDatabase
        implements AutoCloseable
{
    public static final String CONNECTOR_ID = "clickhouse";
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private final Connection connection;
    private final ClickHouseClient clickHouseClient;
    private final TestingClickHouseServer testingClickHouseServer;

    public TestingDatabase()
            throws SQLException
    {
        testingClickHouseServer = new TestingClickHouseServer();
        Properties connectionProperties = new Properties();
        clickHouseClient = new ClickHouseClient(new ClickHouseConnectorId(CONNECTOR_ID),
                new ClickHouseConfig(),
                new DriverConnectionFactory(new ClickHouseDriver(),
                        testingClickHouseServer.getJdbcUrl(),
                Optional.ofNullable(testingClickHouseServer.getClickHouseContainer().getUsername()),
                Optional.ofNullable(testingClickHouseServer.getClickHouseContainer().getPassword()),
                connectionProperties));
        connection = DriverManager.getConnection(testingClickHouseServer.getJdbcUrl());
        connection.createStatement().execute("CREATE DATABASE example");

        connection.createStatement().execute("CREATE TABLE example.numbers(text varchar(10), text_short varchar(10), value bigint)  ENGINE = TinyLog ");
        connection.createStatement().execute("INSERT INTO example.numbers(text, text_short, value) VALUES " +
                "('one', 'one', 1)," +
                "('two', 'two', 2)," +
                "('three', 'three', 3)," +
                "('ten', 'ten', 10)," +
                "('eleven', 'eleven', 11)," +
                "('twelve', 'twelve', 12)" +
                "");
        connection.createStatement().execute("CREATE TABLE example.view_source(id varchar(10))  ENGINE = TinyLog ");
        //connection.createStatement().execute("CREATE TABLE example.view  ENGINE = TinyLog AS SELECT id FROM example.view_source");
        connection.createStatement().execute("CREATE DATABASE tpch");
        connection.createStatement().execute("CREATE TABLE tpch.orders(orderkey bigint, custkey bigint)  ENGINE = TinyLog ");
        connection.createStatement().execute("CREATE TABLE tpch.lineitem(orderkey bigint , partkey bigint)  ENGINE = TinyLog ");

        connection.createStatement().execute("CREATE DATABASE exa_ple");
        connection.createStatement().execute("CREATE TABLE exa_ple.num_ers(te_t varchar(10), \"VA%UE\" bigint)  ENGINE = TinyLog ");
        connection.createStatement().execute("CREATE TABLE exa_ple.table_with_float_col(col1 bigint, col2 double, col3 float, col4 real)  ENGINE = TinyLog ");

        connection.createStatement().execute("CREATE DATABASE schema_for_create_table_tests");
        connection.commit();
    }

    @Override
    public void close()
            throws SQLException
    {
        connection.close();
        testingClickHouseServer.close();
    }

    public Connection getConnection()
    {
        return connection;
    }

    public ClickHouseClient getClickHouseClient()
    {
        return clickHouseClient;
    }
}
