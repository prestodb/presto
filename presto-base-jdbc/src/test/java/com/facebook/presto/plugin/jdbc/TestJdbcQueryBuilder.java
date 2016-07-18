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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertContains;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilder
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;

    private List<JdbcColumnHandle> columns;

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();

        columns = ImmutableList.of(
                new JdbcColumnHandle("test_id", "col_0", BIGINT),
                new JdbcColumnHandle("test_id", "col_1", DOUBLE),
                new JdbcColumnHandle("test_id", "col_2", BOOLEAN),
                new JdbcColumnHandle("test_id", "col_3", VARCHAR),
                new JdbcColumnHandle("test_id", "col_4", DATE),
                new JdbcColumnHandle("test_id", "col_5", TIME),
                new JdbcColumnHandle("test_id", "col_6", TIMESTAMP));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement("create table \"test_table\" (" + "" +
                "\"col_0\" BIGINT, " +
                "\"col_1\" DOUBLE, " +
                "\"col_2\" BOOLEAN, " +
                "\"col_3\" VARCHAR(128), " +
                "\"col_4\" DATE, " +
                "\"col_5\" TIME, " +
                "\"col_6\" TIMESTAMP, " +
                ")")) {
            preparedStatement.execute();
            StringBuilder stringBuilder = new StringBuilder("insert into \"test_table\" values ");
            int len = 1000;
            LocalDateTime dateTime = LocalDateTime.of(2016, 3, 23, 12, 23, 37);
            for (int i = 0; i < len; i++) {
                stringBuilder.append(format(
                        Locale.ENGLISH,
                        "(%d, %f, %b, 'test_str_%d', '%s', '%s', '%s')",
                        i,
                        200000.0 + i / 2.0,
                        i % 2 == 0,
                        i,
                        Date.valueOf(dateTime.toLocalDate()),
                        Time.valueOf(dateTime.toLocalTime()),
                        Timestamp.valueOf(dateTime)));
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
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(0), Domain.create(SortedRangeSet.copyOf(BIGINT,
                        ImmutableList.of(
                                Range.equal(BIGINT, 128L),
                                Range.equal(BIGINT, 180L),
                                Range.equal(BIGINT, 233L),
                                Range.lessThan(BIGINT, 25L),
                                Range.range(BIGINT, 66L, true, 96L, true),
                                Range.greaterThan(BIGINT, 192L))),
                        false),
                columns.get(1), Domain.create(SortedRangeSet.copyOf(DOUBLE,
                        ImmutableList.of(
                                Range.equal(DOUBLE, 200011.0),
                                Range.equal(DOUBLE, 200014.0),
                                Range.equal(DOUBLE, 200017.0),
                                Range.equal(DOUBLE, 200116.5),
                                Range.range(DOUBLE, 200030.0, true, 200036.0, true),
                                Range.range(DOUBLE, 200048.0, true, 200099.0, true))),
                        false),
                columns.get(2), Domain.create(SortedRangeSet.copyOf(BOOLEAN,
                        ImmutableList.of(Range.equal(BOOLEAN, true))),
                        false)
        ));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
            while (resultSet.next()) {
                builder.add((Long) resultSet.getObject("col_0"));
            }
            assertEquals(builder.build(), ImmutableSet.of(22L, 66L, 68L, 70L, 72L, 96L, 128L, 180L, 194L, 196L, 198L));
        }
    }

    @Test
    public void testBuildSqlWithString()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columns.get(3), Domain.create(SortedRangeSet.copyOf(VARCHAR,
                        ImmutableList.of(
                                Range.range(VARCHAR, utf8Slice("test_str_700"), true, utf8Slice("test_str_702"), false),
                                Range.equal(VARCHAR, utf8Slice("test_str_180")),
                                Range.equal(VARCHAR, utf8Slice("test_str_196")))),
                        false)
        ));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain);
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
                        false)
        ));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain);
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
                        false)
        ));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain);
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
                columns.get(1), Domain.onlyNull(DOUBLE)
        ));

        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain);
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
