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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlIntegrationSmokeTest(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(() -> PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ORDERS));
        this.postgreSqlServer = postgreSqlServer;
        execute("CREATE EXTENSION file_fdw");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        postgreSqlServer.close();
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
    }

    @Test
    public void testForeignTable()
            throws Exception
    {
        execute("CREATE SERVER devnull FOREIGN DATA WRAPPER file_fdw");
        execute("CREATE FOREIGN TABLE tpch.test_ft (x bigint) SERVER devnull OPTIONS (filename '/dev/null')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_ft"));
        computeActual("SELECT * FROM test_ft");
        execute("DROP FOREIGN TABLE tpch.test_ft");
        execute("DROP SERVER devnull");
    }

    private class ArrayTest
    {
        final String rowDefinition;
        final String insertionValue;
        final Type expectedType;
        final Object expectedValue;

        ArrayTest(String rowDefinition,
                  String insertionValue,
                  Type expectedTypes,
                  Object expectedValues)
        {
            this.rowDefinition = rowDefinition;
            this.insertionValue = insertionValue;
            this.expectedType = expectedTypes;
            this.expectedValue = expectedValues;
        }

        MaterializedResult getExpectedResult()
        {
            return MaterializedResult
                    .resultBuilder(getSession(), expectedType)
                    .row(expectedValue)
                    .build();
        }

        String createTableStatement(String name)
        {
            return String.format("CREATE TABLE %s (%s)", name, rowDefinition);
        }

        String insertStatement(String name)
        {
            return String.format("INSERT INTO %s VALUES %s", name, insertionValue);
        }
    }

    @Test
    public void testPostgreSqlArrays() throws SQLException, ParseException
    {
        List<ArrayTest> tests = ImmutableList.of(
                new ArrayTest(
                        "text_array text[]",
                        "('{a, b, c}')",
                        new ArrayType(VarcharType.createUnboundedVarcharType()),
                        Arrays.asList("a", "b", "c")),
                new ArrayTest(
                        "varchar_array varchar(128)[]",
                        "('{a, bcdefg}')",
                        new ArrayType(VarcharType.createVarcharType(128)),
                        Arrays.asList("a", "bcdefg")),
                new ArrayTest(
                        "bit_array bit[]",
                        "('{1, 0}')",
                        new ArrayType(BooleanType.BOOLEAN),
                        Arrays.asList(true, false)),
                new ArrayTest(
                        "bool_array bool[]",
                        "('{true, false, true}')",
                        new ArrayType(BooleanType.BOOLEAN),
                        Arrays.asList(true, false, true)),
                new ArrayTest(
                        "int2_array int2[]",
                        "('{2, -10, 40}')",
                        new ArrayType(SmallintType.SMALLINT),
                        Arrays.asList((short) 2, (short) -10, (short) 40)),
                new ArrayTest(
                        "int4_array int4[]",
                        "('{2, -10, 40, 1000000}')",
                        new ArrayType(IntegerType.INTEGER),
                        Arrays.asList(2, -10, 40, 1000000)),
                new ArrayTest(
                        "int8_array int8[]",
                        "('{2, -123456789, 40, 123456789012}')",
                        new ArrayType(BigintType.BIGINT),
                        Arrays.asList(2L, -123456789L, 40L, 123456789012L)),
                new ArrayTest(
                        "float4_array float4[]",
                        "('{0.25,100.25}')",
                        new ArrayType(DoubleType.DOUBLE),
                        Arrays.asList(0.25d, 100.25d)),
                new ArrayTest(
                        "float8_array float8[]",
                        "('{0.25,234556790.5}')",
                        new ArrayType(DoubleType.DOUBLE),
                        Arrays.asList(0.25d, 234556790.5d)),
                new ArrayTest(
                        "numeric_array numeric[]",
                        "('{-100.25, 0, 923.25}')",
                        new ArrayType(DoubleType.DOUBLE),
                        Arrays.asList(-100.25d, 0d, 923.25d)),
                new ArrayTest(
                        "byte_array bytea[]",
                        "('{a,b,c,d}')",
                        new ArrayType(VarbinaryType.VARBINARY),
                        Arrays.asList(
                                ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
                                ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)),
                                ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)),
                                ByteBuffer.wrap("d".getBytes(StandardCharsets.UTF_8)))),
                new ArrayTest(
                        "date_array date[]",
                        "('{2008-08-08, 2002-02-02}')",
                        new ArrayType(DateType.DATE),
                        Arrays.asList(
                                new Date(LocalDate.parse("2008-08-08")
                                        .atStartOfDay(ZoneOffset.UTC)
                                        .toEpochSecond() * 1000),
                                new Date(LocalDate.parse("2002-02-02")
                                        .atStartOfDay(ZoneOffset.UTC)
                                        .toEpochSecond() * 1000))),
                new ArrayTest(
                        "time_array time[]",
                        "('{12:00:00, 16:59:01}')",
                        new ArrayType(TimeType.TIME),
                        Arrays.asList(
                                Time.valueOf("12:00:00"),
                                Time.valueOf("16:59:01"))),
                new ArrayTest(
                        "timestamp_array timestamp[]",
                        "('{2004-12-31T00:23:12, 2001-12-24T11:32:22}')",
                        new ArrayType(TimestampType.TIMESTAMP),
                        Arrays.asList(
                                Timestamp.valueOf("2004-12-31 00:23:12"),
                                Timestamp.valueOf("2001-12-24 11:32:22"))),
                new ArrayTest(
                        "int_array int4[]",
                        "('{1,null}')",
                        new ArrayType(IntegerType.INTEGER),
                        Arrays.asList(1, null)),
                new ArrayTest(
                        "int_array int4[]",
                        "('{}')",
                        new ArrayType(IntegerType.INTEGER),
                        Arrays.asList()));
        for (ArrayTest test : tests) {
            execute(test.createTableStatement("tpch.test_arrays"));
            execute(test.insertStatement("tpch.test_arrays"));
            MaterializedResult actual = computeActual("select * from test_arrays");
            assertEquals(actual, test.getExpectedResult());
            execute("drop table tpch.test_arrays");
        }
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
