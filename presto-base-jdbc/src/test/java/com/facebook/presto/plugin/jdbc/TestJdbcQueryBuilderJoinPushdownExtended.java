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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilderJoinPushdownExtended
{
    private TestingDatabaseJoinPushdown database;
    private JdbcClient jdbcClient;
    private ConnectorSession session;
    private List<JdbcColumnHandle> columns1;
    private List<JdbcColumnHandle> columns2;
    private List<JdbcColumnHandle> columns3;
    private List<JdbcColumnHandle> columns4;

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
        columns3 = ImmutableList.of(
                new JdbcColumnHandle("test_id", "integer_col_3", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.empty()));
        columns4 = ImmutableList.of(
                new JdbcColumnHandle("test_id", "integer_col_4", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.empty()));

        Connection connection = database.getConnection();

        try (PreparedStatement preparedStatement = connection.prepareStatement("CREATE SCHEMA \"test_schema\"")) {
            preparedStatement.execute();
        }

        createTableWithData(connection, "test_table_1", "integer_col_1", 10);
        createTableWithData(connection, "test_table_2", "integer_col_2", 10);
        createTableWithData(connection, "test_table_3", "integer_col_3", 10);
        createTableWithData(connection, "test_table_4", "integer_col_4", 10);
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

    @Test
    public void testCreatedPreparedStatement()
            throws SQLException
    {
        // Join Query that we are trying to test in this test :
        // SELECT table_alias_1.integer_col_1, table_alias_2.integer_col_2, table_alias_3.integer_col_3
        // FROM test_schema.test_table_1 table_alias_1
        // JOIN test_schema.test_table_2 table_alias_2 ON table_alias_2.integer_col_1 = table_alias_2.integer_col_2;

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(INTEGER),
                columns2.get(0), Domain.all(INTEGER)));

        Connection connection = database.getConnection();

        ConnectorId connectorId = new ConnectorId("test_catalog");

        SchemaTableName schemaTableName1 = new SchemaTableName("test_schema", "test_table_1");
        JdbcTableHandle connectorHandle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, "test_catalog", "test_schema", "test_table_1", Optional.empty(), Optional.of("table_alias_1"));

        SchemaTableName schemaTableName2 = new SchemaTableName("test_schema", "test_table_2");
        JdbcTableHandle connectorHandle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, "test_catalog", "test_schema", "test_table_2", Optional.empty(), Optional.of("table_alias_2"));

        // outputVariables1 will be integer_col_1
        // outputVariables2 will be integer_col_2
        VariableReferenceExpression firstJoinLeftColumn = new VariableReferenceExpression(Optional.empty(), "integer_col_1", INTEGER);
        VariableReferenceExpression firstJoinRightColumn = new VariableReferenceExpression(Optional.empty(), "integer_col_2", INTEGER);

        List<VariableReferenceExpression> variableReferenceExpression1 = new ArrayList<>();
        List<VariableReferenceExpression> variableReferenceExpression2 = new ArrayList<>();
        Optional<List<VariableReferenceExpression>> outputVariables1 = Optional.of(variableReferenceExpression1);
        Optional<List<VariableReferenceExpression>> outputVariables2 = Optional.of(variableReferenceExpression2);
        if (outputVariables1.isPresent()) {
            outputVariables1.get().add(firstJoinLeftColumn);
        }
        if (outputVariables2.isPresent()) {
            outputVariables2.get().add(firstJoinRightColumn);
        }

        // assignments1 will be integer_col_1
        // assignments2 will be integer_col_2
        Map<VariableReferenceExpression, ColumnHandle> assignments1Hashmap = new HashMap<>();
        Map<VariableReferenceExpression, ColumnHandle> assignments2Hashmap = new HashMap<>();
        Optional<Map<VariableReferenceExpression, ColumnHandle>> assignments1 = Optional.of(assignments1Hashmap);
        Optional<Map<VariableReferenceExpression, ColumnHandle>> assignments2 = Optional.of(assignments2Hashmap);
        if (assignments1.isPresent()) {
            Map<VariableReferenceExpression, ColumnHandle> map = assignments1.get();
            VariableReferenceExpression key = firstJoinLeftColumn;
            JdbcColumnHandle value = new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1"));
            map.put(key, value);
        }
        if (assignments2.isPresent()) {
            Map<VariableReferenceExpression, ColumnHandle> map = assignments2.get();
            VariableReferenceExpression key = firstJoinRightColumn;
            JdbcColumnHandle value = new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2"));
            map.put(key, value);
        }

        List<ConnectorTableHandle> joinTablesList = new ArrayList<>();
        joinTablesList.add(connectorHandle1);
        joinTablesList.add(connectorHandle2);

        Optional<List<ConnectorTableHandle>> joinPushdownTables = Optional.of(joinTablesList);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")));

        // Make Additional Predicate. This will have the Join Conditions.
        // Join condition : "table_alias_1"."integer_col_1" = "table_alias_2"."integer_col_2"
        StringBuilder predicateAsString = new StringBuilder();
        predicateAsString.append(quote("table_alias_1")).append(".").append(quote("integer_col_1"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_2")).append(".").append(quote("integer_col_2"));
        JdbcExpression predicateAsExpression = new JdbcExpression(predicateAsString.toString());
        Optional<JdbcExpression> additionalPredicate = Optional.of(predicateAsExpression);

        PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "test_catalog", "test_schema", "test_table_1", joinPushdownTables, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate);

        String expectedPreparedStatement = "SELECT \"table_alias_1\".\"integer_col_1\", \"table_alias_2\".\"integer_col_2\" " +
                "FROM \"test_schema\".\"test_table_2\" \"table_alias_2\", \"test_schema\".\"test_table_1\" \"table_alias_1\" " +
                "WHERE \"table_alias_1\".\"integer_col_1\" = \"table_alias_2\".\"integer_col_2\"";

        // preparedStatement will have some additional metadata like name of the user, and some text in the beginning as well. We can ignore those.
        // We only need to assert the generated SQL.
        assertTrue(preparedStatement.toString().contains(expectedPreparedStatement),
                "The expected SQL fragment is not found in the actual prepared statement.");
    }

    @Test
    public void testJoinBetweenThreeTables()
            throws SQLException
    {
        // Join Query that we are trying to test in this test :
        // SELECT table_alias_1.integer_col_1, table_alias_2.integer_col_2, table_alias_3.integer_col_3
        // FROM test_schema.test_table_1 table_alias_1
        // JOIN test_schema.test_table_2 table_alias_2 ON table_alias_2.integer_col_1 = table_alias_2.integer_col_2
        // JOIN test_schema.test_table_3 table_alias_3 ON table_alias_2.integer_col_2 = table_alias_2.integer_col_3;

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(INTEGER),
                columns2.get(0), Domain.all(INTEGER),
                columns3.get(0), Domain.all(INTEGER)));

        Connection connection = database.getConnection();

        ConnectorId connectorId = new ConnectorId("test_catalog");

        SchemaTableName schemaTableName1 = new SchemaTableName("test_schema", "test_table_1");
        JdbcTableHandle connectorHandle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, "test_catalog", "test_schema", "test_table_1", Optional.empty(), Optional.of("table_alias_1"));

        SchemaTableName schemaTableName2 = new SchemaTableName("test_schema", "test_table_2");
        JdbcTableHandle connectorHandle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, "test_catalog", "test_schema", "test_table_2", Optional.empty(), Optional.of("table_alias_2"));

        SchemaTableName schemaTableName3 = new SchemaTableName("test_schema", "test_table_3");
        JdbcTableHandle connectorHandle3 = new JdbcTableHandle(connectorId.toString(), schemaTableName3, "test_catalog", "test_schema", "test_table_3", Optional.empty(), Optional.of("table_alias_3"));

        // outputVariables1 will be integer_col_1
        // outputVariables2 will be integer_col_2
        // outputVariables3 will be integer_col_3
        VariableReferenceExpression firstJoinLeftColumn = new VariableReferenceExpression(Optional.empty(), "integer_col_1", INTEGER);
        VariableReferenceExpression firstJoinRightColumn = new VariableReferenceExpression(Optional.empty(), "integer_col_2", INTEGER);
        VariableReferenceExpression secondJoinLeftColumn = new VariableReferenceExpression(Optional.empty(), "integer_col_2", INTEGER);
        VariableReferenceExpression secondJoinRightColumn = new VariableReferenceExpression(Optional.empty(), "integer_col_3", INTEGER);

        List<VariableReferenceExpression> variableReferenceExpression1 = new ArrayList<>();
        List<VariableReferenceExpression> variableReferenceExpression2 = new ArrayList<>();
        List<VariableReferenceExpression> variableReferenceExpression3 = new ArrayList<>();
        Optional<List<VariableReferenceExpression>> outputVariables1 = Optional.of(variableReferenceExpression1);
        Optional<List<VariableReferenceExpression>> outputVariables2 = Optional.of(variableReferenceExpression2);
        Optional<List<VariableReferenceExpression>> outputVariables3 = Optional.of(variableReferenceExpression3);
        if (outputVariables1.isPresent()) {
            outputVariables1.get().add(firstJoinLeftColumn);
        }
        if (outputVariables2.isPresent()) {
            outputVariables2.get().add(firstJoinRightColumn);
        }
        if (outputVariables3.isPresent()) {
            outputVariables3.get().add(secondJoinRightColumn);
        }

        // assignments1 will be integer_col_1
        // assignments2 will be integer_col_2
        // assignments3 will be integer_col_3
        Map<VariableReferenceExpression, ColumnHandle> assignments1Hashmap = new HashMap<>();
        Map<VariableReferenceExpression, ColumnHandle> assignments2Hashmap = new HashMap<>();
        Map<VariableReferenceExpression, ColumnHandle> assignments3Hashmap = new HashMap<>();
        Optional<Map<VariableReferenceExpression, ColumnHandle>> assignments1 = Optional.of(assignments1Hashmap);
        Optional<Map<VariableReferenceExpression, ColumnHandle>> assignments2 = Optional.of(assignments2Hashmap);
        Optional<Map<VariableReferenceExpression, ColumnHandle>> assignments3 = Optional.of(assignments3Hashmap);
        if (assignments1.isPresent()) {
            Map<VariableReferenceExpression, ColumnHandle> map = assignments1.get();
            VariableReferenceExpression key = firstJoinLeftColumn;
            JdbcColumnHandle value = new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1"));
            map.put(key, value);
        }
        if (assignments2.isPresent()) {
            Map<VariableReferenceExpression, ColumnHandle> map = assignments2.get();
            VariableReferenceExpression key = firstJoinRightColumn;
            JdbcColumnHandle value = new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2"));
            map.put(key, value);
        }
        if (assignments3.isPresent()) {
            Map<VariableReferenceExpression, ColumnHandle> map = assignments3.get();
            VariableReferenceExpression key = secondJoinRightColumn;
            JdbcColumnHandle value = new JdbcColumnHandle(connectorId.toString(), "integer_col_3", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_3"));
            map.put(key, value);
        }

        // Create a list to hold JoinTables instances
        List<ConnectorTableHandle> joinTablesList = new ArrayList<>();
        // Add the instances to the list
        joinTablesList.add(connectorHandle1);
        joinTablesList.add(connectorHandle2);
        joinTablesList.add(connectorHandle3);

        Optional<List<ConnectorTableHandle>> joinPushdownTables = Optional.of(joinTablesList);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_3", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_3")));

        // Make Additional Predicate. This will have the Join Conditions.
        // Join condition : "table_alias_1"."integer_col_1" = "table_alias_2"."integer_col_2" AND "table_alias_2."integer_col_2" = "table_alias_3."integer_col_3"
        StringBuilder predicateAsString = new StringBuilder();
        predicateAsString.append(quote("table_alias_1")).append(".").append(quote("integer_col_1"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_2")).append(".").append(quote("integer_col_2"));
        predicateAsString.append(" AND ");
        predicateAsString.append(quote("table_alias_2")).append(".").append(quote("integer_col_2"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_3")).append(".").append(quote("integer_col_3"));
        JdbcExpression predicateAsExpression = new JdbcExpression(predicateAsString.toString());
        Optional<JdbcExpression> additionalPredicate = Optional.of(predicateAsExpression);

        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "test_catalog", "test_schema", "test_table_1", joinPushdownTables, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Integer> integerCol1Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol2Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol3Values = ImmutableSet.builder();

            while (resultSet.next()) {
                integerCol1Values.add(resultSet.getInt("integer_col_1"));
                integerCol2Values.add(resultSet.getInt("integer_col_2"));
                integerCol3Values.add(resultSet.getInt("integer_col_3"));
            }

            // Expected values based on data setup
            ImmutableSet<Integer> expectedIntegerCol1Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            ImmutableSet<Integer> expectedIntegerCol2Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            ImmutableSet<Integer> expectedIntegerCol3Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // We are Asserting the preparedStatement and also the resultSet.
            String expectedPreparedStatement = "SELECT \"table_alias_1\".\"integer_col_1\", \"table_alias_2\".\"integer_col_2\", \"table_alias_3\".\"integer_col_3\" " +
                    "FROM \"test_schema\".\"test_table_3\" \"table_alias_3\", \"test_schema\".\"test_table_2\" \"table_alias_2\", \"test_schema\".\"test_table_1\" \"table_alias_1\" " +
                    "WHERE \"table_alias_1\".\"integer_col_1\" = \"table_alias_2\".\"integer_col_2\" AND \"table_alias_2\".\"integer_col_2\" = \"table_alias_3\".\"integer_col_3\"";

            assertTrue(preparedStatement.toString().contains(expectedPreparedStatement),
                    "The expected SQL fragment is not found in the actual prepared statement.");
            assertEquals(integerCol1Values.build(), expectedIntegerCol1Values);
            assertEquals(integerCol2Values.build(), expectedIntegerCol2Values);
            assertEquals(integerCol3Values.build(), expectedIntegerCol3Values);
        }
    }

    @Test
    public void testJoinBetweenFourTables()
            throws SQLException
    {
        // Join Query that we are trying to test in this test :
        // SELECT table_alias_1.integer_col_1, table_alias_2.integer_col_2, table_alias_3.integer_col_3, table_alias_4.integer_col_4
        // FROM test_schema.test_table_1 table_alias_1
        // JOIN test_schema.test_table_2 table_alias_2 ON table_alias_2.integer_col_1 = table_alias_2.integer_col_2
        // JOIN test_schema.test_table_3 table_alias_3 ON table_alias_2.integer_col_2 = table_alias_2.integer_col_3
        // JOIN test_schema.test_table_3 table_alias_4 ON table_alias_2.integer_col_3 = table_alias_2.integer_col_4;

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(INTEGER),
                columns2.get(0), Domain.all(INTEGER),
                columns3.get(0), Domain.all(INTEGER),
                columns4.get(0), Domain.all(INTEGER)));

        Connection connection = database.getConnection();

        ConnectorId connectorId = new ConnectorId("test_catalog");

        SchemaTableName schemaTableName1 = new SchemaTableName("test_schema", "test_table_1");
        JdbcTableHandle connectorHandle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, "test_catalog", "test_schema", "test_table_1", Optional.empty(), Optional.of("table_alias_1"));

        SchemaTableName schemaTableName2 = new SchemaTableName("test_schema", "test_table_2");
        JdbcTableHandle connectorHandle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, "test_catalog", "test_schema", "test_table_2", Optional.empty(), Optional.of("table_alias_2"));

        SchemaTableName schemaTableName3 = new SchemaTableName("test_schema", "test_table_3");
        JdbcTableHandle connectorHandle3 = new JdbcTableHandle(connectorId.toString(), schemaTableName3, "test_catalog", "test_schema", "test_table_3", Optional.empty(), Optional.of("table_alias_3"));

        SchemaTableName schemaTableName4 = new SchemaTableName("test_schema", "test_table_4");
        JdbcTableHandle connectorHandle4 = new JdbcTableHandle(connectorId.toString(), schemaTableName4, "test_catalog", "test_schema", "test_table_4", Optional.empty(), Optional.of("table_alias_4"));

        // Create a list to hold JoinTables instances
        List<ConnectorTableHandle> joinTablesList = new ArrayList<>();
        // Add the instances to the list
        joinTablesList.add(connectorHandle1);
        joinTablesList.add(connectorHandle2);
        joinTablesList.add(connectorHandle3);
        joinTablesList.add(connectorHandle4);

        Optional<List<ConnectorTableHandle>> joinPushdownTables = Optional.of(joinTablesList);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_3", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_3")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_4", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_4")));

        // Make Additional Predicate. This will have the Join Conditions.
        // Join condition : "table_alias_1"."integer_col_1" = "table_alias_2"."integer_col_2"
        // AND "table_alias_2."integer_col_2" = "table_alias_3."integer_col_3"
        // AND "table_alias_3."integer_col_3" = "table_alias_4."integer_col_4"
        StringBuilder predicateAsString = new StringBuilder();
        predicateAsString.append(quote("table_alias_1")).append(".").append(quote("integer_col_1"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_2")).append(".").append(quote("integer_col_2"));
        predicateAsString.append(" AND ");
        predicateAsString.append(quote("table_alias_2")).append(".").append(quote("integer_col_2"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_3")).append(".").append(quote("integer_col_3"));
        predicateAsString.append(" AND ");
        predicateAsString.append(quote("table_alias_3")).append(".").append(quote("integer_col_3"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_4")).append(".").append(quote("integer_col_4"));

        JdbcExpression predicateAsExpression = new JdbcExpression(predicateAsString.toString());
        Optional<JdbcExpression> additionalPredicate = Optional.of(predicateAsExpression);

        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "test_catalog", "test_schema", "test_table_1", joinPushdownTables, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Integer> integerCol1Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol2Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol3Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol4Values = ImmutableSet.builder();

            while (resultSet.next()) {
                integerCol1Values.add(resultSet.getInt("integer_col_1"));
                integerCol2Values.add(resultSet.getInt("integer_col_2"));
                integerCol3Values.add(resultSet.getInt("integer_col_3"));
                integerCol4Values.add(resultSet.getInt("integer_col_4"));
            }

            ImmutableSet<Integer> expectedIntegerCol1Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            ImmutableSet<Integer> expectedIntegerCol2Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            ImmutableSet<Integer> expectedIntegerCol3Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            ImmutableSet<Integer> expectedIntegerCol4Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            String expectedPreparedStatement = "SELECT \"table_alias_1\".\"integer_col_1\", \"table_alias_2\".\"integer_col_2\", \"table_alias_3\".\"integer_col_3\", \"table_alias_4\".\"integer_col_4\" " +
                    "FROM \"test_schema\".\"test_table_4\" \"table_alias_4\", \"test_schema\".\"test_table_3\" \"table_alias_3\", \"test_schema\".\"test_table_2\" \"table_alias_2\", \"test_schema\".\"test_table_1\" \"table_alias_1\" " +
                    "WHERE \"table_alias_1\".\"integer_col_1\" = \"table_alias_2\".\"integer_col_2\" AND \"table_alias_2\".\"integer_col_2\" = \"table_alias_3\".\"integer_col_3\" AND \"table_alias_3\".\"integer_col_3\" = \"table_alias_4\".\"integer_col_4\"";

            assertTrue(preparedStatement.toString().contains(expectedPreparedStatement),
                    "The expected SQL fragment is not found in the actual prepared statement.");
            assertEquals(integerCol1Values.build(), expectedIntegerCol1Values);
            assertEquals(integerCol2Values.build(), expectedIntegerCol2Values);
            assertEquals(integerCol3Values.build(), expectedIntegerCol3Values);
            assertEquals(integerCol4Values.build(), expectedIntegerCol4Values);
        }
    }

    @Test
    public void testJoinWithWrongPredicate()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(INTEGER),
                columns2.get(0), Domain.all(INTEGER)));

        Connection connection = database.getConnection();

        ConnectorId connectorId = new ConnectorId("test_catalog");

        SchemaTableName schemaTableName1 = new SchemaTableName("test_schema", "test_table_1");
        JdbcTableHandle connectorHandle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, "test_catalog", "test_schema", "test_table_1", Optional.empty(), Optional.of("table_alias_1"));

        SchemaTableName schemaTableName2 = new SchemaTableName("test_schema", "test_table_2");
        JdbcTableHandle connectorHandle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, "test_catalog", "test_schema", "test_table_2", Optional.empty(), Optional.of("table_alias_2"));

        List<ConnectorTableHandle> joinTablesList = new ArrayList<>();
        joinTablesList.add(connectorHandle1);
        joinTablesList.add(connectorHandle2);

        Optional<List<ConnectorTableHandle>> joinPushdownTables = Optional.of(joinTablesList);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")));

        // Intentionally giving wrong predicate here
        // Join condition : "table_alias_1"."integer_col_1" = "table_alias_3"."integer_col_3"
        StringBuilder predicateAsString = new StringBuilder();
        predicateAsString.append(quote("table_alias_1")).append(".").append(quote("integer_col_1"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_3")).append(".").append(quote("integer_col_3"));
        JdbcExpression predicateAsExpression = new JdbcExpression(predicateAsString.toString());
        Optional<JdbcExpression> additionalPredicate = Optional.of(predicateAsExpression);

        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "test_catalog", "test_schema", "test_table_1", joinPushdownTables, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Integer> integerCol1Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol2Values = ImmutableSet.builder();

            while (resultSet.next()) {
                integerCol1Values.add(resultSet.getInt("integer_col_1"));
                integerCol2Values.add(resultSet.getInt("integer_col_2"));
            }

            fail("Expected SQLException was not thrown");
        }
        catch (SQLException e) {
            assertTrue(e.getMessage().contains("Column \"table_alias_3.integer_col_3\" not found"));
        }
    }

    @Test
    public void testJoinWithWrongTableName()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(INTEGER),
                columns2.get(0), Domain.all(INTEGER)));

        Connection connection = database.getConnection();

        ConnectorId connectorId = new ConnectorId("test_catalog");

        SchemaTableName schemaTableName1 = new SchemaTableName("test_schema", "test_table_1");
        JdbcTableHandle connectorHandle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, "test_catalog", "test_schema", "test_table_1", Optional.empty(), Optional.of("table_alias_1"));

        // Giving wrong table name here
        SchemaTableName schemaTableName2 = new SchemaTableName("test_schema", "test_table_3");
        JdbcTableHandle connectorHandle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, "test_catalog", "test_schema", "test_table_3", Optional.empty(), Optional.of("table_alias_2"));

        List<ConnectorTableHandle> joinTablesList = new ArrayList<>();
        joinTablesList.add(connectorHandle1);
        joinTablesList.add(connectorHandle2);

        Optional<List<ConnectorTableHandle>> joinPushdownTables = Optional.of(joinTablesList);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle(connectorId.toString(), "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")));

        // Make Additional Predicate. This will have the Join Conditions.
        // Join condition : "table_alias_1"."integer_col_1" = "table_alias_2"."integer_col_2"
        StringBuilder predicateAsString = new StringBuilder();
        predicateAsString.append(quote("table_alias_1")).append(".").append(quote("integer_col_1"));
        predicateAsString.append(" = ");
        predicateAsString.append(quote("table_alias_2")).append(".").append(quote("integer_col_2"));
        JdbcExpression predicateAsExpression = new JdbcExpression(predicateAsString.toString());
        Optional<JdbcExpression> additionalPredicate = Optional.of(predicateAsExpression);

        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "test_catalog", "test_schema", "test_table_1", joinPushdownTables, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Integer> integerCol1Values = ImmutableSet.builder();
            ImmutableSet.Builder<Integer> integerCol2Values = ImmutableSet.builder();

            while (resultSet.next()) {
                integerCol1Values.add(resultSet.getInt("integer_col_1"));
                integerCol2Values.add(resultSet.getInt("integer_col_2"));
            }

            fail("Expected SQLException was not thrown");
        }
        catch (SQLException e) {
            assertTrue(e.getMessage().contains("Column \"table_alias_2.integer_col_2\" not found"));
        }
    }

    public String quote(String str)
    {
        return "\"" + str + "\"";
    }
}
