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
import org.testcontainers.containers.OracleContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.function.Function;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.facebook.presto.tests.datatype.DataType.stringDataType;
import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

public class TestOracleTypes
        extends AbstractTestQueryFramework
{
    private final TestingOracleServer oracleServer;
    private final QueryRunner queryRunner;

    @Test
    public void test()
    {
        OracleContainer oracle = new OracleContainer("wnameless/oracle-xe-11g-r2");
        oracle.start();
    }

    private TestOracleTypes(TestingOracleServer oracleServer)
            throws Exception
    {
        this.queryRunner = createOracleQueryRunner(oracleServer);
        this.oracleServer = oracleServer;
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
        oracleServer.execute("INSERT INTO test VALUES (12345678901234567890.12345678901234567890123456789012345678)");
        assertQuery("SELECT * FROM test", "VALUES (12345678901234567890.1234567890)");
    }

    @Test
    public void testVarcharType()
    {
        DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "test")
                .addRoundTrip(stringDataType("varchar", createVarcharType(4000)), "test")
                .addRoundTrip(stringDataType("varchar(5000)", createUnboundedVarcharType()), "test")
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
}
