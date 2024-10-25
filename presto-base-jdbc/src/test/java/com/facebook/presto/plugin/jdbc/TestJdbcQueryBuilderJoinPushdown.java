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
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BOOLEAN;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_CHAR;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DATE;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_SMALLINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TIME;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TIMESTAMP;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_TINYINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilderJoinPushdown
{
    private TestingDatabaseJoinPushdown database;
    private JdbcClient jdbcClient;
    private ConnectorSession session;

    private static final List<JdbcColumnHandle> columns1;
    private static final List<JdbcColumnHandle> columns2;

    static {
        CharType charType = CharType.createCharType(0);
        columns1 = ImmutableList.of(
                new JdbcColumnHandle("test_connectorId", "bigint_col_1", JDBC_BIGINT, BIGINT, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "double_col_1", JDBC_DOUBLE, DOUBLE, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "boolean_col_1", JDBC_BOOLEAN, BOOLEAN, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "varchar128_col_1", JDBC_VARCHAR, VARCHAR, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "date_col_1", JDBC_DATE, DATE, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "time_col_1", JDBC_TIME, TIME, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "timestamp_col_1", JDBC_TIMESTAMP, TIMESTAMP, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "tinyint_col_1", JDBC_TINYINT, TINYINT, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "smallint_col_1", JDBC_SMALLINT, SMALLINT, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "integer_col_1", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "real_col_1", JDBC_REAL, REAL, true, Optional.empty(), Optional.of("table_alias_1")),
                new JdbcColumnHandle("test_connectorId", "char128_col_1", JDBC_CHAR, charType, true, Optional.empty(), Optional.of("table_alias_1")));
        columns2 = ImmutableList.of(
                new JdbcColumnHandle("test_connectorId", "bigint_col_2", JDBC_BIGINT, BIGINT, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "double_col_2", JDBC_DOUBLE, DOUBLE, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "boolean_col_2", JDBC_BOOLEAN, BOOLEAN, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "varchar128_col_2", JDBC_VARCHAR, VARCHAR, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "date_col_2", JDBC_DATE, DATE, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "time_col_2", JDBC_TIME, TIME, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "timestamp_col_2", JDBC_TIMESTAMP, TIMESTAMP, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "tinyint_col_2", JDBC_TINYINT, TINYINT, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "smallint_col_2", JDBC_SMALLINT, SMALLINT, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "integer_col_2", JDBC_INTEGER, INTEGER, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "real_col_2", JDBC_REAL, REAL, true, Optional.empty(), Optional.of("table_alias_2")),
                new JdbcColumnHandle("test_connectorId", "char128_col_2", JDBC_CHAR, charType, true, Optional.empty(), Optional.of("table_alias_2")));
    }

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabaseJoinPushdown();
        jdbcClient = database.getJdbcClient();

        session = new TestingConnectorSession(ImmutableList.of());

        Connection connection = database.getConnection();
        // Create schema
        try (PreparedStatement preparedStatement = connection.prepareStatement("CREATE SCHEMA \"test_schema\"")) {
            preparedStatement.execute();
        }
        // For Join Queries we need 2 tables
        createTableWithData(connection, "test_schema", "test_table_1",
                "\"bigint_col_1\" BIGINT, \"double_col_1\" DOUBLE, \"boolean_col_1\" BOOLEAN, \"varchar128_col_1\" VARCHAR(128), " +
                        "\"date_col_1\" DATE, \"time_col_1\" TIME, \"timestamp_col_1\" TIMESTAMP, \"tinyint_col_1\" TINYINT, " +
                        "\"smallint_col_1\" SMALLINT, \"integer_col_1\" INTEGER, \"real_col_1\" REAL, \"char128_col_1\" CHAR(128)", 10);

        createTableWithData(connection, "test_schema", "test_table_2",
                "\"bigint_col_2\" BIGINT, \"double_col_2\" DOUBLE, \"boolean_col_2\" BOOLEAN, \"varchar128_col_2\" VARCHAR(128), " +
                        "\"date_col_2\" DATE, \"time_col_2\" TIME, \"timestamp_col_2\" TIMESTAMP, \"tinyint_col_2\" TINYINT, " +
                        "\"smallint_col_2\" SMALLINT, \"integer_col_2\" INTEGER, \"real_col_2\" REAL, \"char128_col_2\" CHAR(128)", 10);
    }

    private void createTableWithData(Connection connection, String schemaName, String tableName, String columnsDefinition, int rowCount)
            throws SQLException
    {
        String createTableSQL = format("CREATE TABLE \"%s\".\"%s\" (%s)", schemaName, tableName, columnsDefinition);
        try (PreparedStatement createTableStatement = connection.prepareStatement(createTableSQL)) {
            createTableStatement.execute();
        }

        StringBuilder insertSQL = new StringBuilder(format("INSERT INTO \"%s\".\"%s\" VALUES ", schemaName, tableName));
        LocalDateTime dateTime = LocalDateTime.of(2016, 3, 23, 12, 23, 37);

        for (int i = 0; i < rowCount; i++) {
            insertSQL.append(format(Locale.ENGLISH,
                    "(%d, %f, %b, 'test_str_%d', '%s', '%s', '%s', %d, %d, %d, %f, 'test_str_%d')",
                    i,
                    200000.0 + i / 2.0,
                    i % 2 == 0,
                    i,
                    Date.valueOf(dateTime.toLocalDate()),
                    Time.valueOf(dateTime.toLocalTime()),
                    Timestamp.valueOf(dateTime),
                    i % 128,
                    -i,
                    i + 1,
                    100.0f + i,
                    i));
            dateTime = dateTime.plusHours(6);
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

    // Test what happens when you pass joinPushdownTables as empty
    @Test
    public void testEmptyBuildSql()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(BIGINT),
                columns1.get(1), Domain.onlyNull(DOUBLE)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "test_schema", "test_table_1", Optional.empty(), columns1, ImmutableMap.of(), tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            assertEquals(resultSet.next(), false);
        }
    }

    @DataProvider
    public static Object[][] DifferentDataTypes()
    {
        CharType charType = CharType.createCharType(0);
        TupleDomain<ColumnHandle> tupleDomainBigInt = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(0), Domain.all(BIGINT),
                columns2.get(0), Domain.all(BIGINT)));
        TupleDomain<ColumnHandle> tupleDomainDouble = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(1), Domain.all(DOUBLE),
                columns2.get(1), Domain.all(DOUBLE)));
        TupleDomain<ColumnHandle> tupleDomainBoolean = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(2), Domain.all(BOOLEAN),
                columns2.get(2), Domain.all(BOOLEAN)));
        TupleDomain<ColumnHandle> tupleDomainVarchar = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(3), Domain.all(VARCHAR),
                columns2.get(3), Domain.all(VARCHAR)));
        TupleDomain<ColumnHandle> tupleDomainDate = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(4), Domain.all(DATE),
                columns2.get(4), Domain.all(DATE)));
        TupleDomain<ColumnHandle> tupleDomainTime = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(5), Domain.all(TIME),
                columns2.get(5), Domain.all(TIME)));
        TupleDomain<ColumnHandle> tupleDomainTimeStamp = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(6), Domain.all(TIMESTAMP),
                columns2.get(6), Domain.all(TIMESTAMP)));
        TupleDomain<ColumnHandle> tupleDomainTinyInt = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(7), Domain.all(TINYINT),
                columns2.get(7), Domain.all(TINYINT)));
        TupleDomain<ColumnHandle> tupleDomainSmallInt = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(8), Domain.all(SMALLINT),
                columns2.get(8), Domain.all(SMALLINT)));
        TupleDomain<ColumnHandle> tupleDomainInteger = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(9), Domain.all(INTEGER),
                columns2.get(9), Domain.all(INTEGER)));
        TupleDomain<ColumnHandle> tupleDomainReal = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(10), Domain.all(REAL),
                columns2.get(10), Domain.all(REAL)));
        TupleDomain<ColumnHandle> tupleDomainChar = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns1.get(11), Domain.all(charType),
                columns2.get(11), Domain.all(charType)));

        // Expected values based on data setup
        ImmutableSet<Integer> expectedIntegerCol1Values = ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        ImmutableSet<Integer> expectedIntegerCol2Values = ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        ImmutableSet<Float> expectedRealCol1Values = ImmutableSet.of(100.0F, 101.0F, 102.0F, 103.0F, 104.0F, 105.0F, 106.0F, 107.0F, 108.0F, 109.0F);
        ImmutableSet<Float> expectedRealCol2Values = ImmutableSet.of(100.0F, 101.0F, 102.0F, 103.0F, 104.0F, 105.0F, 106.0F, 107.0F, 108.0F, 109.0F);

        ImmutableSet<String> expectedVarchar128Col1Values = ImmutableSet.of("test_str_0", "test_str_1", "test_str_2", "test_str_3", "test_str_4", "test_str_5", "test_str_6", "test_str_7", "test_str_8", "test_str_9");
        ImmutableSet<String> expectedVarchar128Col2Values = ImmutableSet.of("test_str_0", "test_str_1", "test_str_2", "test_str_3", "test_str_4", "test_str_5", "test_str_6", "test_str_7", "test_str_8", "test_str_9");

        ImmutableSet<Double> expectedDoubleCol1Values = ImmutableSet.of(200000.0, 200000.5, 200001.0, 200001.5, 200002.0, 200002.5, 200003.0, 200003.5, 200004.0, 200004.5);
        ImmutableSet<Double> expectedDoubleCol2Values = ImmutableSet.of(200000.0, 200000.5, 200001.0, 200001.5, 200002.0, 200002.5, 200003.0, 200003.5, 200004.0, 200004.5);

        ImmutableSet<Boolean> expectedBooleanCol1Values = ImmutableSet.of(true, false, true, false, true, false, true, false, true, false);
        ImmutableSet<Boolean> expectedBooleanCol2Values = ImmutableSet.of(true, false, true, false, true, false, true, false, true, false);

        ImmutableSet<Integer> expectedBigIntCol1Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        ImmutableSet<Integer> expectedBigIntCol2Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        ImmutableSet<Date> expectedDateCol1Values = ImmutableSet.of(
                toDate(2016, 3, 23),
                toDate(2016, 3, 23),
                toDate(2016, 3, 24),
                toDate(2016, 3, 24),
                toDate(2016, 3, 24),
                toDate(2016, 3, 24),
                toDate(2016, 3, 25),
                toDate(2016, 3, 25),
                toDate(2016, 3, 25),
                toDate(2016, 3, 25));
        ImmutableSet<Date> expectedDateCol2Values = ImmutableSet.of(
                toDate(2016, 3, 23),
                toDate(2016, 3, 23),
                toDate(2016, 3, 24),
                toDate(2016, 3, 24),
                toDate(2016, 3, 24),
                toDate(2016, 3, 24),
                toDate(2016, 3, 25),
                toDate(2016, 3, 25),
                toDate(2016, 3, 25),
                toDate(2016, 3, 25));

        ImmutableSet<Time> expectedTimeCol1Values = ImmutableSet.of(
                toTime(2016, 6, 7, 12, 23, 37),
                toTime(2016, 6, 7, 18, 23, 37),
                toTime(2016, 6, 8, 0, 23, 37),
                toTime(2016, 6, 8, 6, 23, 37),
                toTime(2016, 6, 8, 12, 23, 37),
                toTime(2016, 6, 8, 18, 23, 37),
                toTime(2016, 6, 9, 0, 23, 37),
                toTime(2016, 6, 9, 6, 23, 37),
                toTime(2016, 6, 9, 12, 23, 37),
                toTime(2016, 6, 9, 18, 23, 37));
        ImmutableSet<Time> expectedTimeCol2Values = ImmutableSet.of(
                toTime(2016, 6, 7, 12, 23, 37),
                toTime(2016, 6, 7, 18, 23, 37),
                toTime(2016, 6, 8, 0, 23, 37),
                toTime(2016, 6, 8, 6, 23, 37),
                toTime(2016, 6, 8, 12, 23, 37),
                toTime(2016, 6, 8, 18, 23, 37),
                toTime(2016, 6, 9, 0, 23, 37),
                toTime(2016, 6, 9, 6, 23, 37),
                toTime(2016, 6, 9, 12, 23, 37),
                toTime(2016, 6, 9, 18, 23, 37));

        ImmutableSet<Timestamp> expectedTimestampCol1Values = ImmutableSet.of(
                toTimestamp(2016, 3, 23, 12, 23, 37),
                toTimestamp(2016, 3, 23, 18, 23, 37),
                toTimestamp(2016, 3, 24, 0, 23, 37),
                toTimestamp(2016, 3, 24, 6, 23, 37),
                toTimestamp(2016, 3, 24, 12, 23, 37),
                toTimestamp(2016, 3, 24, 18, 23, 37),
                toTimestamp(2016, 3, 25, 0, 23, 37),
                toTimestamp(2016, 3, 25, 6, 23, 37),
                toTimestamp(2016, 3, 25, 12, 23, 37),
                toTimestamp(2016, 3, 25, 18, 23, 37));

        ImmutableSet<Timestamp> expectedTimestampCol2Values = ImmutableSet.of(
                toTimestamp(2016, 3, 23, 12, 23, 37),
                toTimestamp(2016, 3, 23, 18, 23, 37),
                toTimestamp(2016, 3, 24, 0, 23, 37),
                toTimestamp(2016, 3, 24, 6, 23, 37),
                toTimestamp(2016, 3, 24, 12, 23, 37),
                toTimestamp(2016, 3, 24, 18, 23, 37),
                toTimestamp(2016, 3, 25, 0, 23, 37),
                toTimestamp(2016, 3, 25, 6, 23, 37),
                toTimestamp(2016, 3, 25, 12, 23, 37),
                toTimestamp(2016, 3, 25, 18, 23, 37));

        ImmutableSet<Integer> expectedTinyInt1Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        ImmutableSet<Integer> expectedTinyInt2Values = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        ImmutableSet<Integer> expectedSmallIntCol1Values = ImmutableSet.of(0, -1, -2, -3, -4, -5, -6, -7, -8, -9);
        ImmutableSet<Integer> expectedSmallIntCol2Values = ImmutableSet.of(0, -1, -2, -3, -4, -5, -6, -7, -8, -9);

        ImmutableSet<String> expectedChar128Col1Values = ImmutableSet.of("test_str_0", "test_str_1", "test_str_2", "test_str_3", "test_str_4", "test_str_5", "test_str_6", "test_str_7", "test_str_8", "test_str_9");
        ImmutableSet<String> expectedChar128Col2Values = ImmutableSet.of("test_str_0", "test_str_1", "test_str_2", "test_str_3", "test_str_4", "test_str_5", "test_str_6", "test_str_7", "test_str_8", "test_str_9");

        // Create a map to link data types to their result extraction methods
        Map<Type, ValueExtractor<?>> extractorMap = new HashMap<>();
        extractorMap.put(INTEGER, ResultSet::getInt);
        extractorMap.put(REAL, ResultSet::getFloat);
        extractorMap.put(VARCHAR, (ResultSet::getString));
        extractorMap.put(DOUBLE, (ResultSet::getDouble));
        extractorMap.put(BOOLEAN, (ResultSet::getBoolean));
        extractorMap.put(BIGINT, (ResultSet::getInt));
        extractorMap.put(DATE, (ResultSet::getDate));
        extractorMap.put(TIME, (ResultSet::getTime));
        extractorMap.put(TIMESTAMP, (ResultSet::getTimestamp));
        extractorMap.put(TINYINT, (ResultSet::getInt));
        extractorMap.put(SMALLINT, (ResultSet::getInt));
        extractorMap.put(charType, (ResultSet::getString));

        return new Object[][] {
                {BIGINT, JDBC_BIGINT, extractorMap, tupleDomainBigInt, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "bigint_col_1", "bigint_col_2", expectedBigIntCol1Values, expectedBigIntCol2Values},
                {DOUBLE, JDBC_DOUBLE, extractorMap, tupleDomainDouble, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "double_col_1", "double_col_2", expectedDoubleCol1Values, expectedDoubleCol2Values},
                {BOOLEAN, JDBC_BOOLEAN, extractorMap, tupleDomainBoolean, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "boolean_col_1", "boolean_col_2", expectedBooleanCol1Values, expectedBooleanCol2Values},
                {VARCHAR, JDBC_VARCHAR, extractorMap, tupleDomainVarchar, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "varchar128_col_1", "varchar128_col_2", expectedVarchar128Col1Values, expectedVarchar128Col2Values},
                {DATE, JDBC_DATE, extractorMap, tupleDomainDate, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "date_col_1", "date_col_2", expectedDateCol1Values, expectedDateCol2Values},
                {TIME, JDBC_TIME, extractorMap, tupleDomainTime, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "time_col_1", "time_col_2", expectedTimeCol1Values, expectedTimeCol2Values},
                {TIMESTAMP, JDBC_TIMESTAMP, extractorMap, tupleDomainTimeStamp, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "timestamp_col_1", "timestamp_col_2", expectedTimestampCol1Values, expectedTimestampCol2Values},
                {TINYINT, JDBC_TINYINT, extractorMap, tupleDomainTinyInt, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "tinyint_col_1", "tinyint_col_2", expectedTinyInt1Values, expectedTinyInt2Values},
                {SMALLINT, JDBC_SMALLINT, extractorMap, tupleDomainSmallInt, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "smallint_col_1", "smallint_col_2", expectedSmallIntCol1Values, expectedSmallIntCol2Values},
                {INTEGER, JDBC_INTEGER, extractorMap, tupleDomainInteger, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "integer_col_1", "integer_col_2", expectedIntegerCol1Values, expectedIntegerCol2Values},
                {REAL, JDBC_REAL, extractorMap, tupleDomainReal, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "real_col_1", "real_col_2", expectedRealCol1Values, expectedRealCol2Values},
                {charType, JDBC_CHAR, extractorMap, tupleDomainChar, "test_connectorId", "test_catalog", "test_schema", "test_schema", "test_table_1", "test_table_2", "table_alias_1", "table_alias_2", "char128_col_1", "char128_col_2", expectedChar128Col1Values, expectedChar128Col2Values},
        };
    }

    @FunctionalInterface
    public interface ValueExtractor<T>
    {
        T extractValue(ResultSet resultSet, String column) throws SQLException;
    }

    // This is to test a basic join query with different data types in the join condition
    @Test(dataProvider = "DifferentDataTypes")
    public void testBasicJoinQueriesWithDifferentDataTypes(Type valueType, JdbcTypeHandle jdbcTypeHandle, Map<Type, ValueExtractor<?>> extractorMap,
                                                           TupleDomain<ColumnHandle> tupleDomain, String connectorIdStr, String catalogName,
                                                           String schemaName1, String schemaName2, String tableName1, String tableName2,
                                                           String tableAlias1, String tableAlias2, String column1, String column2,
                                                           ImmutableSet<Object> expectedCol1Values, ImmutableSet<Object> expectedCol2Values)
            throws SQLException
    {
        Connection connection = database.getConnection();
        ConnectorId connectorId = new ConnectorId(connectorIdStr);

        SchemaTableName schemaTableName1 = new SchemaTableName(schemaName1, tableName1);
        JdbcTableHandle connectorHandle1 = new JdbcTableHandle(connectorId.toString(), schemaTableName1, catalogName, schemaName1, tableName1, Optional.empty(), Optional.of(tableAlias1));
        SchemaTableName schemaTableName2 = new SchemaTableName(schemaName2, tableName2);
        JdbcTableHandle connectorHandle2 = new JdbcTableHandle(connectorId.toString(), schemaTableName2, catalogName, schemaName2, tableName2, Optional.empty(), Optional.of(tableAlias2));

        List<ConnectorTableHandle> joinTablesList = new ArrayList<>();
        joinTablesList.add(connectorHandle1);
        joinTablesList.add(connectorHandle2);
        Optional<List<ConnectorTableHandle>> joinPushdownTables = Optional.of(joinTablesList);

        List<JdbcColumnHandle> selectColumns = ImmutableList.of(
                new JdbcColumnHandle(connectorId.toString(), column1, jdbcTypeHandle, valueType, true, Optional.empty(), Optional.of(tableAlias1)),
                new JdbcColumnHandle(connectorId.toString(), column2, jdbcTypeHandle, valueType, true, Optional.empty(), Optional.of(tableAlias2)));

        // Make Additional Predicate. This will have the Join Condition.
        // Join condition : "tableAlias1"."column1" = "tableAlias2"."column2"
        StringBuilder predicateAsString = new StringBuilder();
        predicateAsString.append(quote(tableAlias1)).append(".").append(quote(column1));
        predicateAsString.append(" = ");
        predicateAsString.append(quote(tableAlias2)).append(".").append(quote(column2));
        JdbcExpression predicateAsExpression = new JdbcExpression(predicateAsString.toString());
        Optional<JdbcExpression> additionalPredicate = Optional.of(predicateAsExpression);

        // Ensure the extractorMap contains the appropriate extractor for the given valueType
        ValueExtractor<?> extractor = extractorMap.get(valueType);
        if (extractor == null) {
            throw new IllegalArgumentException("Unsupported value type: " + valueType);
        }

        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, catalogName, schemaName1, tableName1, joinPushdownTables, selectColumns, ImmutableMap.of(), tupleDomain, additionalPredicate);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Object> col1Values = ImmutableSet.builder();
            ImmutableSet.Builder<Object> col2Values = ImmutableSet.builder();

            while (resultSet.next()) {
                Object col1Value = extractor.extractValue(resultSet, column1);
                Object col2Value = extractor.extractValue(resultSet, column2);
                col1Values.add(col1Value);
                col2Values.add(col2Value);
            }

            // We are Asserting the preparedStatement as well as the resultSet.
            StringBuilder expectedPreparedStatement = new StringBuilder();
            expectedPreparedStatement.append("SELECT ");
            expectedPreparedStatement.append(quote(tableAlias1)).append(".").append(quote(column1));
            expectedPreparedStatement.append(", ");
            expectedPreparedStatement.append(quote(tableAlias2)).append(".").append(quote(column2));
            expectedPreparedStatement.append(" FROM ");
            expectedPreparedStatement.append(quote(schemaName1)).append(".").append(quote(tableName2)).append(" ").append(quote(tableAlias2));
            expectedPreparedStatement.append(", ");
            expectedPreparedStatement.append(quote(schemaName2)).append(".").append(quote(tableName1)).append(" ").append(quote(tableAlias1));
            expectedPreparedStatement.append(" WHERE ");
            expectedPreparedStatement.append(predicateAsString);

            // preparedStatement will have some additional metadata like name of the user, and some text in the beginning as well. We can ignore those.
            // We only need to assert the generated SQL.
            assertTrue(preparedStatement.toString().contains(expectedPreparedStatement.toString()),
                    "The expected SQL fragment is not found in the actual prepared statement.");

            assertEquals(col1Values.build(), expectedCol1Values);
            assertEquals(col2Values.build(), expectedCol2Values);
        }
    }

    private static Timestamp toTimestamp(int year, int month, int day, int hour, int minute, int second)
    {
        return Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, minute, second));
    }

    private static Date toDate(int year, int month, int day)
    {
        return Date.valueOf(format("%d-%d-%d", year, month, day));
    }

    private static Time toTime(int year, int month, int day, int hour, int minute, int second)
    {
        return Time.valueOf(LocalDateTime.of(year, month, day, hour, minute, second).toLocalTime());
    }

    public static String quote(String str)
    {
        return "\"" + str + "\"";
    }
}
