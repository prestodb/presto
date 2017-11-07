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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.datatype.CreateAndInsertDataSetup;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataSetup;
import com.facebook.presto.tests.datatype.DataType;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.facebook.presto.tests.sql.JdbcSqlExecutor;
import com.facebook.presto.tests.sql.PrestoSqlExecutor;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.tests.datatype.DataType.charDataType;
import static com.facebook.presto.tests.datatype.DataType.stringDataType;
import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

@Test
public class TestMySqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    private final TestingMySqlServer mysqlServer;

    public TestMySqlTypeMapping()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch"));
    }

    private TestMySqlTypeMapping(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(() -> createMySqlQueryRunner(mysqlServer, TpchTable.getTables()));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlServer.close();
    }

    @Test
    public void testPrestoCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("varchar(10)", createVarcharType(255)), "text_a")
                .addRoundTrip(stringDataType("varchar(255)", createVarcharType(255)), "text_b")
                .addRoundTrip(stringDataType("varchar(256)", createVarcharType(65535)), "text_c")
                .addRoundTrip(stringDataType("varchar(65535)", createVarcharType(65535)), "text_d")
                .addRoundTrip(stringDataType("varchar(65536)", createVarcharType(16777215)), "text_e")
                .addRoundTrip(stringDataType("varchar(16777215)", createVarcharType(16777215)), "text_f")
                .addRoundTrip(stringDataType("varchar(16777216)", createUnboundedVarcharType()), "text_g")
                .addRoundTrip(stringDataType("varchar(" + VarcharType.MAX_LENGTH + ")", createUnboundedVarcharType()), "text_h")
                .addRoundTrip(stringDataType("varchar", createUnboundedVarcharType()), "unbounded")
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_parameterized_varchar"));
    }

    @Test
    public void testMySqlCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext", createVarcharType(255)), "a")
                .addRoundTrip(stringDataType("text", createVarcharType(65535)), "b")
                .addRoundTrip(stringDataType("mediumtext", createVarcharType(16777215)), "c")
                .addRoundTrip(stringDataType("longtext", createUnboundedVarcharType()), "d")
                .addRoundTrip(varcharDataType(32), "e")
                .addRoundTrip(varcharDataType(20000), "f")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar"));
    }

    @Test
    public void testMySqlCreatedParameterizedVarcharUnicode()
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext " + CHARACTER_SET_UTF8, createVarcharType(255)), sampleUnicodeText)
                .addRoundTrip(stringDataType("text " + CHARACTER_SET_UTF8, createVarcharType(65535)), sampleUnicodeText)
                .addRoundTrip(stringDataType("mediumtext " + CHARACTER_SET_UTF8, createVarcharType(16777215)), sampleUnicodeText)
                .addRoundTrip(stringDataType("longtext " + CHARACTER_SET_UTF8, createUnboundedVarcharType()), sampleUnicodeText)
                .addRoundTrip(varcharDataType(sampleUnicodeText.length(), CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(32, CHARACTER_SET_UTF8), sampleUnicodeText)
                .addRoundTrip(varcharDataType(20000, CHARACTER_SET_UTF8), sampleUnicodeText)
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPrestoCreatedParameterizedChar()
    {
        mysqlCharTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("mysql_test_parameterized_char"));
    }

    @Test
    public void testMySqlCreatedParameterizedChar()
    {
        mysqlCharTypeTest()
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_char"));
    }

    private DataTypeTest mysqlCharTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(charDataType("char", 1), "")
                .addRoundTrip(charDataType("char", 1), "a")
                .addRoundTrip(charDataType(1), "")
                .addRoundTrip(charDataType(1), "a")
                .addRoundTrip(charDataType(8), "abc")
                .addRoundTrip(charDataType(8), "12345678")
                .addRoundTrip(charDataType(255), repeat("a", 255));
    }

    @Test
    public void testMySqlCreatedParameterizedCharUnicode()
    {
        DataTypeTest.create()
                .addRoundTrip(charDataType(1, CHARACTER_SET_UTF8), "\u653b")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb")
                .addRoundTrip(charDataType(5, CHARACTER_SET_UTF8), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.mysql_test_parameterized_varchar"));
    }

    @Test
    public void testMysqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), mysqlCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testPrestoCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), prestoCreateAsSelect("test_decimal"));
    }

    private DataTypeTest decimalTests()
    {
        return DataTypeTest.create()
                .addRoundTrip(DataType.decimalType(3, 0), new BigDecimal("193"))
                .addRoundTrip(DataType.decimalType(3, 0), new BigDecimal("19"))
                .addRoundTrip(DataType.decimalType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(DataType.decimalType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(DataType.decimalType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(DataType.decimalType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(DataType.decimalType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(DataType.decimalType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(DataType.decimalType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
            throws SQLException
    {
        testUnsupportedDataType("decimal(50,0)");
    }

    private void testUnsupportedDataType(String databaseDataType)
            throws SQLException
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(mysqlServer.getJdbcUrl());
        jdbcSqlExecutor.execute(format("CREATE TABLE tpch.test_unsupported_data_type(supported_column varchar(5), unsupported_column %s)", databaseDataType));
        try {
            assertQuery(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'tpch' AND TABLE_NAME = 'test_unsupported_data_type'",
                    "VALUES 'supported_column'"); // no 'unsupported_column'
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_unsupported_data_type");
        }
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup mysqlCreateAndInsert(String tableNamePrefix)
    {
        JdbcSqlExecutor mysqlUnicodeExecutor = new JdbcSqlExecutor(mysqlServer.getJdbcUrl() + "&useUnicode=true&characterEncoding=utf8");
        return new CreateAndInsertDataSetup(mysqlUnicodeExecutor, tableNamePrefix);
    }
}
