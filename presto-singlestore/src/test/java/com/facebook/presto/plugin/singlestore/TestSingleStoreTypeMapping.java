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
package com.facebook.presto.plugin.singlestore;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.datatype.CreateAndInsertDataSetup;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataSetup;
import com.facebook.presto.tests.datatype.DataType;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.facebook.presto.tests.sql.JdbcSqlExecutor;
import com.facebook.presto.tests.sql.PrestoSqlExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.tests.datatype.DataType.bigintDataType;
import static com.facebook.presto.tests.datatype.DataType.charDataType;
import static com.facebook.presto.tests.datatype.DataType.dataType;
import static com.facebook.presto.tests.datatype.DataType.decimalDataType;
import static com.facebook.presto.tests.datatype.DataType.doubleDataType;
import static com.facebook.presto.tests.datatype.DataType.integerDataType;
import static com.facebook.presto.tests.datatype.DataType.realDataType;
import static com.facebook.presto.tests.datatype.DataType.smallintDataType;
import static com.facebook.presto.tests.datatype.DataType.stringDataType;
import static com.facebook.presto.tests.datatype.DataType.tinyintDataType;
import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.function.Function.identity;

@Test
public class TestSingleStoreTypeMapping
        extends AbstractTestQueryFramework
{
    private final DockerizedSingleStoreServer singleStoreServer;

    public TestSingleStoreTypeMapping()
    {
        this.singleStoreServer = new DockerizedSingleStoreServer();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SingleStoreQueryRunner.createSingleStoreQueryRunner(singleStoreServer, ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        singleStoreServer.close();
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 125)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testPrestoCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("varchar(10)", createVarcharType(255 / 3)), "text_a")//utf-8
                .addRoundTrip(stringDataType("varchar(255)", createVarcharType(255 / 3)), "text_b")
                .addRoundTrip(stringDataType("varchar(256)", createVarcharType(65535 / 3)), "text_c")
                .addRoundTrip(stringDataType("varchar(65535)", createVarcharType(65535 / 3)), "text_d")
                .addRoundTrip(stringDataType("varchar(65536)", createVarcharType(16777215 / 3)), "text_e")
                .addRoundTrip(stringDataType("varchar(16777215)", createVarcharType(16777215 / 3)), "text_f")
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_parameterized_varchar"));
    }

    @Test
    public void testSingleStoreCreatedParameterizedVarchar()
    {
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext", createVarcharType(255 / 3)), "a")//utf-8
                .addRoundTrip(stringDataType("text", createVarcharType(65535 / 3)), "b")
                .addRoundTrip(stringDataType("mediumtext", createVarcharType(16777215 / 3)), "c")
                .addRoundTrip(stringDataType("longtext", createVarcharType((int) (4294967295L / 3))), "d")
                .addRoundTrip(varcharDataType(32), "e")
                .addRoundTrip(varcharDataType(15000), "f")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.singlestore_test_parameterized_varchar"));
    }

    @Test
    public void testSingleStoreCreatedParameterizedVarcharUnicode()
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        DataTypeTest.create()
                .addRoundTrip(stringDataType("tinytext", createVarcharType(255 / 3)), sampleUnicodeText)
                .addRoundTrip(stringDataType("text", createVarcharType(65535 / 3)), sampleUnicodeText)
                .addRoundTrip(stringDataType("mediumtext", createVarcharType(16777215 / 3)), sampleUnicodeText)
                .addRoundTrip(stringDataType("longtext", createVarcharType((int) (4294967295L / 3))), sampleUnicodeText)
                .addRoundTrip(varcharDataType(sampleUnicodeText.length()), sampleUnicodeText)
                .addRoundTrip(varcharDataType(32), sampleUnicodeText)
                .addRoundTrip(varcharDataType(20000), sampleUnicodeText)
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.singlestore_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPrestoCreatedParameterizedChar()
    {
        singleStoreCharTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("singlestore_test_parameterized_char"));
    }

    @Test
    public void testSingleStoreCreatedParameterizedChar()
    {
        singleStoreCharTypeTest()
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.singlestore_test_parameterized_char"));
    }

    private DataTypeTest singleStoreCharTypeTest()
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
    public void testSingleStoreCreatedParameterizedCharUnicode()
    {
        DataTypeTest.create()
                .addRoundTrip(charDataType(1), "\u653b")
                .addRoundTrip(charDataType(5), "\u653b\u6bbb")
                .addRoundTrip(charDataType(5), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.singlestore_test_parameterized_varchar"));
    }

    @Test
    public void testSingleStoreCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.test_decimal"));
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
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("193"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("19"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalDataType(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        testUnsupportedDataType("decimal(50,0)");
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay()).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(singleStoreDateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(singleStoreDateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(singleStoreDateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(singleStoreDateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(singleStoreDateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(singleStoreDateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(singleStoreDateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(singleStoreDateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, singleStoreCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, prestoCreateAsSelect("test_date"));
        }
    }

    private static DataType<LocalDate> singleStoreDateDataType()
    {
        return dataType(
                "DATE",
                DATE,
                DateTimeFormatter.ofPattern("''''yyyy-MM-dd''")::format,
                identity());
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(singleStoreServer.getJdbcUrl());
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

    private DataSetup singleStoreCreateAndInsert(String tableNamePrefix)
    {
        JdbcSqlExecutor singleStoreUnicodeExecutor = new JdbcSqlExecutor(singleStoreServer.getJdbcUrl());
        return new CreateAndInsertDataSetup(singleStoreUnicodeExecutor, tableNamePrefix);
    }
}
