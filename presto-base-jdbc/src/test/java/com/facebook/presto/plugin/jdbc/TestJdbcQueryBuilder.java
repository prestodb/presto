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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
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
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestJdbcQueryBuilder
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;

    private List<JdbcColumnHandle> cols = new ArrayList<>();

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();

        Connection connection = database.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("create table \"test_table\" (" + "" +
                "\"col_0\" BIGINT, " +
                "\"col_1\" DOUBLE, " +
                "\"col_2\" BOOLEAN, " +
                "\"col_3\" TIMESTAMP, " +
                "\"col_4\" Time, " +
                "\"col_5\" DATE, " +
                "\"col_6\" VARCHAR, " +
                ")");
        preparedStatement.execute();
        StringBuilder stringBuilder = new StringBuilder("insert into \"test_table\" values ");
        int len = 1000;
        for (int i = 0; i < len; i++) {
            stringBuilder.append("(" + i + ", " + (200000.0 + i / 2.0) + ", " + (i % 2 == 0) + ", now(), now(), current_date, 'test'" + ")");
            if (i != len - 1) {
                stringBuilder.append(",");
            }
        }
        PreparedStatement preparedStatement2 = connection.prepareStatement(stringBuilder.toString());
        preparedStatement2.execute();

        cols.clear();
        cols.add(new JdbcColumnHandle("test_id", "col_0", BigintType.BIGINT));
        cols.add(new JdbcColumnHandle("test_id", "col_1", DoubleType.DOUBLE));
        cols.add(new JdbcColumnHandle("test_id", "col_2", BooleanType.BOOLEAN));
        cols.add(new JdbcColumnHandle("test_id", "col_3", TimestampType.TIMESTAMP));
        cols.add(new JdbcColumnHandle("test_id", "col_4", TimeType.TIME));
        cols.add(new JdbcColumnHandle("test_id", "col_5", DateType.DATE));
        cols.add(new JdbcColumnHandle("test_id", "col_6", VarcharType.VARCHAR));
    }

    @AfterMethod
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
                cols.get(0), Domain.create(SortedRangeSet.copyOf(BigintType.BIGINT,
                        ImmutableList.of(
                                Range.equal(BigintType.BIGINT, 128L),
                                Range.equal(BigintType.BIGINT, 180L),
                                Range.equal(BigintType.BIGINT, 233L),
                                Range.lessThan(BigintType.BIGINT, 25L),
                                Range.range(BigintType.BIGINT, 66L, true, 96L, true),
                                Range.greaterThan(BigintType.BIGINT, 192L))),
                        false),
                cols.get(1), Domain.create(SortedRangeSet.copyOf(DoubleType.DOUBLE,
                        ImmutableList.of(
                                Range.equal(DoubleType.DOUBLE, 200011.0),
                                Range.equal(DoubleType.DOUBLE, 200014.0),
                                Range.equal(DoubleType.DOUBLE, 200017.0),
                                Range.equal(DoubleType.DOUBLE, 200116.5),
                                Range.range(DoubleType.DOUBLE, 200030.0, true, 200036.0, true),
                                Range.range(DoubleType.DOUBLE, 200048.0, true, 200099.0, true))),
                        false),
                cols.get(2), Domain.create(SortedRangeSet.copyOf(BooleanType.BOOLEAN,
                        ImmutableList.of(Range.equal(BooleanType.BOOLEAN, true))),
                        false)
        ));
        Connection connection = database.getConnection();

        PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);

        ResultSet res = preparedStatement.executeQuery();

        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        while (res.next()) {
            builder.add((Long) res.getObject("col_0"));
        }
        assertEquals(builder.build(), ImmutableSet.of(22L, 66L, 68L, 70L, 72L, 96L, 128L, 180L, 194L, 196L, 198L));
    }

    @Test
    public void testEmptyBuildSql()
            throws SQLException
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                cols.get(0), Domain.all(BigintType.BIGINT),
                cols.get(1), Domain.onlyNull(DoubleType.DOUBLE)
        ));
        Connection connection = database.getConnection();

        PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);

        ResultSet res = preparedStatement.executeQuery();
        assertEquals(res.next(), false);
    }

    @Test
    public void testPushdownTimestamp()
            throws SQLException
    {
        Connection connection = database.getConnection();

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(3), Domain.create(SortedRangeSet.copyOf(TimestampType.TIMESTAMP,
                ImmutableList.of(
                    Range.greaterThan(TimestampType.TIMESTAMP, System.currentTimeMillis())
                        )),
                        false)
                ));
        PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        String sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_3\" > ?"), true);

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(3), Domain.create(SortedRangeSet.copyOf(TimestampType.TIMESTAMP,
                ImmutableList.of(
                    Range.lessThan(TimestampType.TIMESTAMP, System.currentTimeMillis())
                        )),
                        false)
                ));
        preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_3\" < ?"), true);

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(3), Domain.create(SortedRangeSet.copyOf(TimestampType.TIMESTAMP,
                ImmutableList.of(
                    Range.lessThanOrEqual(TimestampType.TIMESTAMP, System.currentTimeMillis())
                        )),
                        false)
                ));
        preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_3\" <= ?"), true);

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(3), Domain.create(SortedRangeSet.copyOf(TimestampType.TIMESTAMP,
                ImmutableList.of(
                    Range.greaterThanOrEqual(TimestampType.TIMESTAMP, System.currentTimeMillis())
                        )),
                        false)
                ));
        preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_3\" >= ?"), true);
    }

    @Test
    public void testPushdownDate()
            throws SQLException
    {
        Connection connection = database.getConnection();

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(5), Domain.create(SortedRangeSet.copyOf(DateType.DATE,
                ImmutableList.of(
                    Range.greaterThan(DateType.DATE, (long) 16900)
                        )),
                        false)
                ));
        PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        String sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_5\" > ?"), true);

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(5), Domain.create(SortedRangeSet.copyOf(DateType.DATE,
                ImmutableList.of(
                    Range.lessThan(DateType.DATE, (long) 16900)
                        )),
                        false)
                ));
        preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_5\" < ?"), true);

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(5), Domain.create(SortedRangeSet.copyOf(DateType.DATE,
                ImmutableList.of(
                    Range.lessThanOrEqual(DateType.DATE, (long) 16900)
                        )),
                        false)
                ));
        preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_5\" <= ?"), true);

        tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(5), Domain.create(SortedRangeSet.copyOf(DateType.DATE,
                ImmutableList.of(
                    Range.greaterThanOrEqual(DateType.DATE, (long) 16900)
                        )),
                        false)
                ));
        preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_5\" >= ?"), true);
    }

    @Test
    public void testPushdownString()
            throws SQLException
    {
        Connection connection = database.getConnection();

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
            cols.get(6), Domain.create(SortedRangeSet.copyOf(VarcharType.VARCHAR,
                ImmutableList.of(
                    Range.equal(VarcharType.VARCHAR, utf8Slice("test"))
                        )),
                        false)
                ));
        PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", cols, tupleDomain);
        String sql = preparedStatement.toString();
        assertEquals(sql.contains("\"col_6\" = ?"), true);
    }
}
