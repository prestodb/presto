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
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertContains;
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
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilder
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private ConnectorSession session;

    private List<JdbcColumnHandle> columns;

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        CharType charType = CharType.createCharType(0);

        session = new TestingConnectorSession(ImmutableList.of());
        columns = ImmutableList.of(
                new JdbcColumnHandle("test_id", "col_0", JDBC_BIGINT, BIGINT, true),
                new JdbcColumnHandle("test_id", "col_1", JDBC_DOUBLE, DOUBLE, true),
                new JdbcColumnHandle("test_id", "col_2", JDBC_BOOLEAN, BOOLEAN, true),
                new JdbcColumnHandle("test_id", "col_3", JDBC_VARCHAR, VARCHAR, true),
                new JdbcColumnHandle("test_id", "col_4", JDBC_DATE, DATE, true),
                new JdbcColumnHandle("test_id", "col_5", JDBC_TIME, TIME, true),
                new JdbcColumnHandle("test_id", "col_6", JDBC_TIMESTAMP, TIMESTAMP, true),
                new JdbcColumnHandle("test_id", "col_7", JDBC_TINYINT, TINYINT, true),
                new JdbcColumnHandle("test_id", "col_8", JDBC_SMALLINT, SMALLINT, true),
                new JdbcColumnHandle("test_id", "col_9", JDBC_INTEGER, INTEGER, true),
                new JdbcColumnHandle("test_id", "col_10", JDBC_REAL, REAL, true),
                new JdbcColumnHandle("test_id", "col_11", JDBC_CHAR, charType, true));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement("create table \"test_table\" (" + "" +
                "\"col_0\" BIGINT, " +
                "\"col_1\" DOUBLE, " +
                "\"col_2\" BOOLEAN, " +
                "\"col_3\" VARCHAR(128), " +
                "\"col_4\" DATE, " +
                "\"col_5\" TIME, " +
                "\"col_6\" TIMESTAMP, " +
                "\"col_7\" TINYINT, " +
                "\"col_8\" SMALLINT, " +
                "\"col_9\" INTEGER, " +
                "\"col_10\" REAL, " +
                "\"col_11\" CHAR(128) " +
                ")")) {
            preparedStatement.execute();
            StringBuilder stringBuilder = new StringBuilder("insert into \"test_table\" values ");
            int len = 1000;
            LocalDateTime dateTime = LocalDateTime.of(2016, 3, 23, 12, 23, 37);
            for (int i = 0; i < len; i++) {
                stringBuilder.append(format(
                        Locale.ENGLISH,
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
                        i - 100,
                        100.0f + i,
                        i));
                dateTime = dateTime.plusHours(26);
                if (i != len - 1) {
                    stringBuilder.append(",");
                }
            }
            try (PreparedStatement preparedStatement2 = connection.prepareStatement(stringBuilder.toString())) {
                preparedStatement2.execute();
            }
        }
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testNormalBuildSql()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(columns.get(0), Domain.create(SortedRangeSet.copyOf(BIGINT,
                        ImmutableList.of(
                                Range.equal(BIGINT, 128L),
                                Range.equal(BIGINT, 180L),
                                Range.equal(BIGINT, 233L),
                                Range.lessThan(BIGINT, 25L),
                                Range.range(BIGINT, 66L, true, 96L, true),
                                Range.greaterThan(BIGINT, 192L))),
                        false))
                .put(columns.get(1), Domain.create(SortedRangeSet.copyOf(DOUBLE,
                        ImmutableList.of(
                                Range.equal(DOUBLE, 200011.0),
                                Range.equal(DOUBLE, 200014.0),
                                Range.equal(DOUBLE, 200017.0),
                                Range.equal(DOUBLE, 200116.5),
                                Range.range(DOUBLE, 200030.0, true, 200036.0, true),
                                Range.range(DOUBLE, 200048.0, true, 200099.0, true))),
                        false))
                .put(columns.get(7), Domain.create(SortedRangeSet.copyOf(TINYINT,
                        ImmutableList.of(
                                Range.range(TINYINT, 60L, true, 70L, false),
                                Range.range(TINYINT, 52L, true, 55L, false))),
                        false))
                .put(columns.get(8), Domain.create(SortedRangeSet.copyOf(SMALLINT,
                        ImmutableList.of(
                                Range.range(SMALLINT, -75L, true, -68L, true),
                                Range.range(SMALLINT, -200L, true, -100L, false))),
                        false))
                .put(columns.get(9), Domain.create(SortedRangeSet.copyOf(INTEGER,
                        ImmutableList.of(
                                Range.equal(INTEGER, 80L),
                                Range.equal(INTEGER, 96L),
                                Range.lessThan(INTEGER, 0L))),
                        false))
                .put(columns.get(2), Domain.create(SortedRangeSet.copyOf(BOOLEAN,
                        ImmutableList.of(Range.equal(BOOLEAN, true))),
                        false))
                .build());

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
            while (resultSet.next()) {
                builder.add((Long) resultSet.getObject("col_0"));
            }
            assertEquals(builder.build(), ImmutableSet.of(68L, 180L, 196L));
        }
    }

    @Test
    public void testBuildSqlWithFloat()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(10), Domain.create(SortedRangeSet.copyOf(REAL,
                        ImmutableList.of(
                                Range.equal(REAL, (long) floatToRawIntBits(100.0f + 0)),
                                Range.equal(REAL, (long) floatToRawIntBits(100.008f + 0)),
                                Range.equal(REAL, (long) floatToRawIntBits(100.0f + 14)))),
                        false)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Long> longBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<Float> floatBuilder = ImmutableSet.builder();
            while (resultSet.next()) {
                longBuilder.add((Long) resultSet.getObject("col_0"));
                floatBuilder.add((Float) resultSet.getObject("col_10"));
            }
            assertEquals(longBuilder.build(), ImmutableSet.of(0L, 14L));
            assertEquals(floatBuilder.build(), ImmutableSet.of(100.0f, 114.0f));
        }
    }

    @Test
    public void testBuildSqlWithVarchar()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(3), Domain.create(SortedRangeSet.copyOf(VARCHAR,
                        ImmutableList.of(
                                Range.range(VARCHAR, utf8Slice("test_str_700"), true, utf8Slice("test_str_702"), false),
                                Range.equal(VARCHAR, utf8Slice("test_str_180")),
                                Range.equal(VARCHAR, utf8Slice("test_str_196")))),
                        false)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            while (resultSet.next()) {
                builder.add((String) resultSet.getObject("col_3"));
            }
            assertEquals(builder.build(), ImmutableSet.of("test_str_700", "test_str_701", "test_str_180", "test_str_196"));

            assertContains(preparedStatement.toString(), "\"col_3\" >= ?");
            assertContains(preparedStatement.toString(), "\"col_3\" < ?");
            assertContains(preparedStatement.toString(), "\"col_3\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithChar()
            throws SQLException
    {
        CharType charType = CharType.createCharType(0);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(11), Domain.create(SortedRangeSet.copyOf(charType,
                        ImmutableList.of(
                                Range.range(charType, utf8Slice("test_str_700"), true, utf8Slice("test_str_702"), false),
                                Range.equal(charType, utf8Slice("test_str_180")),
                                Range.equal(charType, utf8Slice("test_str_196")))),
                        false)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            while (resultSet.next()) {
                builder.add((String) resultSet.getObject("col_11"));
            }
            assertEquals(builder.build(), ImmutableSet.of("test_str_700", "test_str_701", "test_str_180", "test_str_196"));

            assertContains(preparedStatement.toString(), "\"col_11\" >= ?");
            assertContains(preparedStatement.toString(), "\"col_11\" < ?");
            assertContains(preparedStatement.toString(), "\"col_11\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithDateTime()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(4), Domain.create(SortedRangeSet.copyOf(DATE,
                        ImmutableList.of(
                                Range.range(DATE, toDays(2016, 6, 7), true, toDays(2016, 6, 17), false),
                                Range.equal(DATE, toDays(2016, 6, 3)),
                                Range.equal(DATE, toDays(2016, 10, 21)))),
                        false),
                columns.get(5), Domain.create(SortedRangeSet.copyOf(TIME,
                        ImmutableList.of(
                                Range.range(TIME, toTime(2016, 6, 7, 6, 12, 23).getTime(), false, toTime(2016, 6, 7, 8, 23, 37).getTime(), true),
                                Range.equal(TIME, toTime(2016, 6, 1, 2, 3, 4).getTime()),
                                Range.equal(TIME, toTime(2016, 10, 21, 20, 23, 37).getTime()))),
                        false)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Date> dateBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<Time> timeBuilder = ImmutableSet.builder();
            while (resultSet.next()) {
                dateBuilder.add((Date) resultSet.getObject("col_4"));
                timeBuilder.add((Time) resultSet.getObject("col_5"));
            }
            assertEquals(dateBuilder.build(), ImmutableSet.of(toDate(2016, 6, 7), toDate(2016, 6, 13), toDate(2016, 10, 21)));
            assertEquals(timeBuilder.build(), ImmutableSet.of(toTime(2016, 6, 7, 8, 23, 37), toTime(2016, 10, 21, 20, 23, 37)));

            assertContains(preparedStatement.toString(), "\"col_4\" >= ?");
            assertContains(preparedStatement.toString(), "\"col_4\" < ?");
            assertContains(preparedStatement.toString(), "\"col_4\" IN (?,?)");
            assertContains(preparedStatement.toString(), "\"col_5\" > ?");
            assertContains(preparedStatement.toString(), "\"col_5\" <= ?");
            assertContains(preparedStatement.toString(), "\"col_5\" IN (?,?)");
        }
    }

    @Test
    public void testBuildSqlWithTimestamp()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(6), Domain.create(SortedRangeSet.copyOf(TIMESTAMP,
                        ImmutableList.of(
                                Range.equal(TIMESTAMP, toTimestamp(2016, 6, 3, 0, 23, 37).getTime()),
                                Range.equal(TIMESTAMP, toTimestamp(2016, 10, 19, 16, 23, 37).getTime()),
                                Range.range(TIMESTAMP, toTimestamp(2016, 6, 7, 8, 23, 37).getTime(), false, toTimestamp(2016, 6, 9, 12, 23, 37).getTime(), true))),
                        false)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Timestamp> builder = ImmutableSet.builder();
            while (resultSet.next()) {
                builder.add((Timestamp) resultSet.getObject("col_6"));
            }
            assertEquals(builder.build(), ImmutableSet.of(
                    toTimestamp(2016, 6, 3, 0, 23, 37),
                    toTimestamp(2016, 6, 8, 10, 23, 37),
                    toTimestamp(2016, 6, 9, 12, 23, 37),
                    toTimestamp(2016, 10, 19, 16, 23, 37)));

            assertContains(preparedStatement.toString(), "\"col_6\" > ?");
            assertContains(preparedStatement.toString(), "\"col_6\" <= ?");
            assertContains(preparedStatement.toString(), "\"col_6\" IN (?,?)");
        }
    }

    @Test
    public void testEmptyBuildSql()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(0), Domain.all(BIGINT),
                columns.get(1), Domain.onlyNull(DOUBLE)));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, session, connection, "", "", "test_table", columns, tupleDomain, Optional.empty());
                ResultSet resultSet = preparedStatement.executeQuery()) {
            assertEquals(resultSet.next(), false);
        }
    }

    private static Timestamp toTimestamp(int year, int month, int day, int hour, int minute, int second)
    {
        return Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, minute, second));
    }

    private static long toDays(int year, int month, int day)
    {
        return DAYS.between(LocalDate.of(1970, 1, 1), LocalDate.of(year, month, day));
    }

    private static Date toDate(int year, int month, int day)
    {
        return Date.valueOf(format("%d-%d-%d", year, month, day));
    }

    private static Time toTime(int year, int month, int day, int hour, int minute, int second)
    {
        return Time.valueOf(LocalDateTime.of(year, month, day, hour, minute, second).toLocalTime());
    }
}
