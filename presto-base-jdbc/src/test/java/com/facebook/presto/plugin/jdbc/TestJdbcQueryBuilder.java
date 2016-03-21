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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;
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
                new JdbcColumnHandle("test_id", "col_2", BOOLEAN));
        Connection connection = database.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement("create table \"test_table\" (" + "" +
                "\"col_0\" BIGINT, " +
                "\"col_1\" DOUBLE, " +
                "\"col_2\" BOOLEAN, " +
                ")")) {
            preparedStatement.execute();
            StringBuilder stringBuilder = new StringBuilder("insert into \"test_table\" values ");
            int len = 1000;
            for (int i = 0; i < len; i++) {
                stringBuilder.append(format("(%d, %f, %b)", i, (200000.0 + i / 2.0), i % 2 == 0));
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
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
            while (resultSet.next()) {
                builder.add((Long) resultSet.getObject("col_0"));
            }
            assertEquals(builder.build(), ImmutableSet.of(22L, 66L, 68L, 70L, 72L, 96L, 128L, 180L, 194L, 196L, 198L));
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
        try (PreparedStatement preparedStatement = new QueryBuilder("\"").buildSql(jdbcClient, connection, "", "", "test_table", columns, tupleDomain)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            assertEquals(resultSet.next(), false);
        }
    }
}
