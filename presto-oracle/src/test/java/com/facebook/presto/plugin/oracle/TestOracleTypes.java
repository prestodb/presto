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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataSetup;
import com.facebook.presto.tests.datatype.DataType;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.facebook.presto.tests.sql.PrestoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.function.Function;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.facebook.presto.tests.datatype.DataType.stringDataType;
import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOracleTypes
        extends AbstractTestQueryFramework
{
    private final OracleServerTester oracleServer;
    private final QueryRunner queryRunner;

    private TestOracleTypes()
            throws Exception
    {
        this.oracleServer = new OracleServerTester();
        this.queryRunner = createOracleQueryRunner(oracleServer);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    @Test
    public void testBooleanType()
    {
        DataTypeTest.create()
                .addRoundTrip(booleanOracleType(), true)
                .addRoundTrip(booleanOracleType(), false)
                .execute(getQueryRunner(), prestoCreateAsSelect("boolean_types"));
    }

    @Test
    public void testSpecialNumberFormats()
    {
        oracleServer.execute("CREATE TABLE test (num1 number)");
        oracleServer.execute("INSERT INTO test VALUES (12345678901234567890.1234567890)");
        assertQuery("SELECT * FROM test", "VALUES (12345678901234567890.1234567890)");
    }

    @Test
    public void testVarcharType()
    {
        DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "test")
                .addRoundTrip(stringDataType("varchar", createUnboundedVarcharType()), "test")
                .addRoundTrip(stringDataType("varchar(4000)", createUnboundedVarcharType()), "test")
                .addRoundTrip(varcharDataType(3), String.valueOf('\u2603'))
                .execute(getQueryRunner(), prestoCreateAsSelect("varchar_types"));
    }

    private static DataType<Boolean> booleanOracleType()
    {
        return DataType.dataType(
                "boolean",
                BigintType.BIGINT,
                value -> value ? "1" : "0",
                value -> value ? 1L : 0L);
    }

    private static DataType<BigDecimal> numberOracleType(DecimalType type)
    {
        String databaseType = format("decimal(%s, %s)", type.getPrecision(), type.getScale());
        return numberOracleType(databaseType, type);
    }

    private static <T> DataType<T> numberOracleType(String inputType, Type resultType)
    {
        Function<T, ?> queryResult = (Function<T, Object>) value ->
                (value instanceof BigDecimal && resultType instanceof DecimalType)
                        ? ((BigDecimal) value).setScale(((DecimalType) resultType).getScale(), HALF_UP)
                        : value;

        return DataType.dataType(
                inputType,
                resultType,
                value -> format("CAST('%s' AS %s)", value, resultType),
                queryResult);
    }

    @Test
    public void testRealInsert()
    {
        assertUpdate("create table test_real_type(datatype_real real)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_real_type"));
        try {
            assertUpdate("insert into test_real_type(datatype_real) values (96.5)", 1);
            assertQuery("SELECT datatype_real FROM test_real_type", "VALUES (96.5)");
        }
        finally {
            assertUpdate("DROP TABLE test_real_type");
            assertFalse(getQueryRunner().tableExists(getSession(), "test_real_type"));
        }
    }

    @Test
    public void testRealCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE test_real_ctas AS SELECT REAL '42.5' as value", 1);

        try {
            assertQuery("SELECT * FROM test_real_ctas", "VALUES (42.5)");

            // Verify the column type is REAL
            assertQuery(
                    "SELECT data_type FROM information_schema.columns " +
                            "WHERE table_name = 'test_real_ctas' AND column_name = 'value'",
                    "VALUES ('real')");
        }
        finally {
            assertUpdate("DROP TABLE test_real_ctas");
        }
    }

    /**
     * Test that REAL columns created in Oracle can be read by Presto.
     * This verifies that the BINARY_FLOAT type mapping works correctly.
     */
    @Test
    public void testReadOracleRealColumn()
    {
        // Create table directly in Oracle using BINARY_FLOAT
        oracleServer.execute("CREATE TABLE test_binary_float (id NUMBER, value BINARY_FLOAT)");

        try {
            // Insert data directly in Oracle
            oracleServer.execute("INSERT INTO test_binary_float VALUES (1, 1.5)");
            oracleServer.execute("INSERT INTO test_binary_float VALUES (2, -2.25)");
            oracleServer.execute("INSERT INTO test_binary_float VALUES (3, 0.5)");
            oracleServer.execute("COMMIT");

            // Verify Presto can read the BINARY_FLOAT column
            assertQuery(
                    "SELECT value FROM test_binary_float ORDER BY id",
                    "VALUES (1.5), (-2.25), (0.5)");
        }
        finally {
            oracleServer.execute("DROP TABLE test_binary_float");
        }
    }

    @Test
    public void testDoubleCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE test_double_ctas AS SELECT DOUBLE '42.5' as value", 1);

        try {
            assertQuery("SELECT * FROM test_double_ctas", "VALUES (42.5)");

            // Verify the column type is DOUBLE
            assertQuery(
                    "SELECT data_type FROM information_schema.columns " +
                            "WHERE table_name = 'test_double_ctas' AND column_name = 'value'",
                    "VALUES ('double')");
        }
        finally {
            assertUpdate("DROP TABLE test_double_ctas");
        }
    }

    @Test
    public void testDoubleInsert()
    {
        assertUpdate("create table test_double_type(datatype_double double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_double_type"));
        try {
            assertUpdate("insert into test_double_type(datatype_double) values (96.5)", 1);
            assertQuery("SELECT datatype_double FROM test_double_type", "VALUES (96.5)");
        }
        finally {
            assertUpdate("DROP TABLE test_double_type");
            assertFalse(getQueryRunner().tableExists(getSession(), "test_double_type"));
        }
    }

    /**
     * Test that DOUBLE columns created in Oracle can be read by Presto.
     * This verifies that the BINARY_DOUBLE type mapping works correctly.
     */
    @Test
    public void testReadOracleDoubleColumn()
    {
        // Create table directly in Oracle using BINARY_DOUBLE
        oracleServer.execute("CREATE TABLE test_binary_double (id NUMBER, value BINARY_DOUBLE)");

        try {
            // Insert data directly in Oracle
            oracleServer.execute("INSERT INTO test_binary_double VALUES (1, 1.5)");
            oracleServer.execute("INSERT INTO test_binary_double VALUES (2, -2.25)");
            oracleServer.execute("INSERT INTO test_binary_double VALUES (3, 0.5)");
            oracleServer.execute("COMMIT");

            // Verify Presto can read the BINARY_DOUBLE column
            assertQuery(
                    "SELECT value FROM test_binary_double ORDER BY id",
                    "VALUES (1.5), (-2.25), (0.5)");
        }
        finally {
            oracleServer.execute("DROP TABLE test_binary_double");
        }
    }
}
