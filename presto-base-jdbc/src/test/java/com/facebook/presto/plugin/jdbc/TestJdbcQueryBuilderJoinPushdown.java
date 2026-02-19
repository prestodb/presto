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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertContains;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilderJoinPushdown
{
    private TestingDatabaseJoinPushdown database;
    private JdbcClient jdbcClient;
    private ConnectorSession session;
    private List<JdbcColumnHandle> columns1;
    private List<JdbcColumnHandle> columns2;

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabaseJoinPushdown();
        jdbcClient = database.getJdbcClient();

        session = new TestingConnectorSession(ImmutableList.of());
        columns1 = ImmutableList.of(
                new JdbcColumnHandle("test_id", "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.empty()));
        columns2 = ImmutableList.of(
                new JdbcColumnHandle("test_id", "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.empty()));

        Connection connection = database.getConnection();

        try (PreparedStatement preparedStatement = connection.prepareStatement("CREATE SCHEMA \"test_schema\"")) {
            preparedStatement.execute();
        }

        createTableWithData(connection, "test_table_1", "integer_col_1", 10);
        createTableWithData(connection, "test_table_2", "integer_col_2", 10);
    }

    private void createTableWithData(Connection connection, String tableName, String columnName, int rowCount)
            throws SQLException
    {
        String createTableSQL = format("CREATE TABLE \"test_schema\".\"%s\" (\"%s\" INTEGER)", tableName, columnName);
        try (PreparedStatement createTableStmt = connection.prepareStatement(createTableSQL)) {
            createTableStmt.execute();
        }

        StringBuilder insertSQL = new StringBuilder(format("INSERT INTO \"test_schema\".\"%s\" VALUES ", tableName));
        for (int i = 0; i < rowCount; i++) {
            insertSQL.append(format(Locale.ENGLISH, "(%d)", i));
            if (i != rowCount - 1) {
                insertSQL.append(",");
            }
        }
        try (PreparedStatement insertStatement = connection.prepareStatement(insertSQL.toString())) {
            insertStatement.execute();
        }
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        database.close();
    }

    // Verifies that SQL generation correctly includes table aliases, selected columns, and join condition in a multi-table query
    @Test
    public void testWithAliasAndPushDownList()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(INTEGER),
                columns2.get(0), Domain.all(INTEGER)));

        Connection connection = database.getConnection();
        ConnectorId connectorId = new ConnectorId("test_catalog");

        SchemaTableName schemaTableName1 = new SchemaTableName("test_schema", "test_table_1");
        JdbcTableHandle handle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, "test_catalog", "test_schema", "test_table_1", Collections.emptyList(), Optional.of("table_alias_1"));

        SchemaTableName schemaTableName2 = new SchemaTableName("test_schema", "test_table_2");
        JdbcTableHandle handle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, "test_catalog", "test_schema", "test_table_2", Collections.emptyList(), Optional.of("table_alias_2"));

        List<ConnectorTableHandle> joinTablesList = ImmutableList.of(handle1, handle2);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")));

        String joinCondition = quote("table_alias_1") + "." + quote("integer_col_1") + " = " +
                quote("table_alias_2") + "." + quote("integer_col_2");

        Optional<JdbcExpression> additionalPredicate = Optional.of(new JdbcExpression(joinCondition));

        PreparedStatement preparedStatement = new QueryBuilder("\"")
                .buildSql(jdbcClient, session, connection, "test_catalog", "test_schema", "test_table_1", joinTablesList, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate, Optional.empty());

        String actualSql = preparedStatement.toString();

        // SELECT list should use table aliases
        assertContains(actualSql, "\"table_alias_1\".\"integer_col_1\"");
        assertContains(actualSql, "\"table_alias_2\".\"integer_col_2\"");

        // FROM clause should include a comma-separated list of join tables with aliases
        String expectedFromClause = "FROM \"test_schema\".\"test_table_1\" \"table_alias_1\", " +
                "\"test_schema\".\"test_table_2\" \"table_alias_2\"";
        assertContains(actualSql, expectedFromClause);

        // Join condition should appear in the WHERE clause
        assertContains(actualSql, joinCondition);
    }

    private String quote(String str)
    {
        return "\"" + str + "\"";
    }
}
