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

import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.ArrayType;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static com.facebook.presto.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_JSON;
import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static com.facebook.presto.tests.datatype.DataType.bigintDataType;
import static com.facebook.presto.tests.datatype.DataType.booleanDataType;
import static com.facebook.presto.tests.datatype.DataType.dataType;
import static com.facebook.presto.tests.datatype.DataType.dateDataType;
import static com.facebook.presto.tests.datatype.DataType.decimalDataType;
import static com.facebook.presto.tests.datatype.DataType.doubleDataType;
import static com.facebook.presto.tests.datatype.DataType.integerDataType;
import static com.facebook.presto.tests.datatype.DataType.jsonDataType;
import static com.facebook.presto.tests.datatype.DataType.realDataType;
import static com.facebook.presto.tests.datatype.DataType.smallintDataType;
import static com.facebook.presto.tests.datatype.DataType.varbinaryDataType;
import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Test
public class TestPostgreSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlTypeMapping()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    private TestPostgreSqlTypeMapping(TestingPostgreSqlServer postgreSqlServer)
    {
        this.postgreSqlServer = postgreSqlServer;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createPostgreSqlQueryRunner(postgreSqlServer, ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        postgreSqlServer.close();
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false)
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases(varbinaryDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varbinary"));

        varbinaryTestCases(byteaDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_varbinary"));
    }

    private DataTypeTest varbinaryTestCases(DataType<byte[]> varbinaryDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(varbinaryDataType, "hello".getBytes(UTF_8))
                .addRoundTrip(varbinaryDataType, "Piękna łąka w 東京都".getBytes(UTF_8))
                .addRoundTrip(varbinaryDataType, "Bag full of 💰".getBytes(UTF_16LE))
                .addRoundTrip(varbinaryDataType, null)
                .addRoundTrip(varbinaryDataType, new byte[] {})
                .addRoundTrip(varbinaryDataType, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 13, -7, 54, 122, -89, 0, 0, 0});
    }

    @Test
    public void testPrestoCreatedParameterizedVarchar()
    {
        varcharDataTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_parameterized_varchar"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarchar()
    {
        varcharDataTypeTest()
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_varchar"));
    }

    private DataTypeTest varcharDataTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "text_a")
                .addRoundTrip(varcharDataType(255), "text_b")
                .addRoundTrip(varcharDataType(65535), "text_d")
                .addRoundTrip(varcharDataType(10485760), "text_f")
                .addRoundTrip(varcharDataType(), "unbounded");
    }

    @Test
    public void testPrestoCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("postgresql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest()
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPrestoCreatedParameterizedCharUnicode()
    {
        unicodeDataTypeTest(DataType::charDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("postgresql_test_parameterized_char_unicode"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedCharUnicode()
    {
        unicodeDataTypeTest(DataType::charDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_char_unicode"));
    }

    private DataTypeTest unicodeVarcharDateTypeTest()
    {
        return unicodeDataTypeTest(DataType::varcharDataType)
                .addRoundTrip(varcharDataType(), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
    }

    private DataTypeTest unicodeDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(sampleUnicodeText.length()), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(32), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(20000), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(1), sampleFourByteUnicodeCharacter);
    }

    @Test
    public void testPostgresSqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_decimal"));
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
        // Note: there is identical test for MySQL

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
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, prestoCreateAsSelect("test_date"));
        }
    }

    @Test
    public void testTimestamp()
    {
        // TODO timestamp is not correctly read (see comment in StandardColumnMappings.timestampColumnMapping), but testing this is hard because of #7122
    }

//    @Test
//    public void testJson()
//    {
//        jsonTestCases(jsonDataType())
//                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_json"));
//    }

    @Test
    public void testJsonb()
    {
        jsonTestCases(jsonbDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_jsonb"));
    }

    private DataTypeTest jsonTestCases(DataType<String> jsonDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(jsonDataType, "{}")
                .addRoundTrip(jsonDataType, null)
                .addRoundTrip(jsonDataType, "null")
                .addRoundTrip(jsonDataType, "123.4")
                .addRoundTrip(jsonDataType, "\"abc\"")
                .addRoundTrip(jsonDataType, "\"text with \\\" quotations and ' apostrophes\"")
                .addRoundTrip(jsonDataType, "\"\"")
                .addRoundTrip(jsonDataType, "{\"a\":1,\"b\":2}")
                .addRoundTrip(jsonDataType, "{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}")
                .addRoundTrip(jsonDataType, "[]");
    }

    private static DataType<String> jsonDataType()
    {
        return dataType(
                "json",
                JSON,
                value -> "JSON " + formatStringLiteral(value),
                identity());
    }

    public static DataType<String> jsonbDataType()
    {
        return dataType(
                "jsonb",
                JSON,
                value -> "JSON " + formatStringLiteral(value),
                identity());
    }

    public static String formatStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    @Test
    public void testUuid()
    {
        uuidTestCases(uuidDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_uuid"));
        uuidTestCases(uuidDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("tpch.presto_test_uuid"));
    }

    private DataTypeTest uuidTestCases(DataType<String> uuidDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(uuidDataType, "00000000-0000-0000-0000-000000000000")
                .addRoundTrip(uuidDataType, "71f9206a-75aa-4005-9ab6-4525c6fdec99");
    }

    private static DataType<String> uuidDataType()
    {
        return dataType(
                "uuid",
                UUID,
                value -> "UUID " + formatStringLiteral(value),
                Function.identity());
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        jdbcSqlExecutor.execute(format("CREATE TABLE tpch.test_unsupported_data_type(key varchar(5), unsupported_column %s)", databaseDataType));
        try {
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_unsupported_data_type'",
                    "VALUES 'key'"); // no 'unsupported_column'
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_unsupported_data_type");
        }
    }

    @Test
    public void jsonDataTypeTest()
    {
        DataTypeTest dataTypeTest = DataTypeTest.create()
                .addRoundTrip(jsonDataType(), "{}")
                .addRoundTrip(jsonDataType(), null)
                .addRoundTrip(jsonDataType(), "null")
                .addRoundTrip(jsonDataType(), "123.4")
                .addRoundTrip(jsonDataType(), "\"abc\"")
                .addRoundTrip(jsonDataType(), "\"\"")
                .addRoundTrip(jsonDataType(), "{\"a\":1,\"b\":2}")
                .addRoundTrip(jsonDataType(), "{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}")
                .addRoundTrip(jsonDataType(), "[]");

        dataTypeTest.execute(getQueryRunner(), prestoCreateAsSelect("presto_test_json"));
        dataTypeTest.execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_json"));
    }

    @Test
    public void testGeometry()
    {
        //TODO: added tests for geometry type, need to move postgresql server to docker, and add postgis image so that geometry data type is supported.
    }

    @Test
    public void testJson()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        jdbcSqlExecutor.execute("CREATE TABLE tpch.test_json(key varchar(5), json_column json, jsonb_column jsonb)");
        try {
            assertQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_json'",
                    "VALUES ('key','varchar(5)'),('json_column','json'),('jsonb_column','json')");
            assertUpdate("INSERT INTO tpch.test_json VALUES ('1', json'{\"x\":123}',json'{\"x\": 123}' )", 1);
            assertQuery("SELECT * FROM tpch.test_json", "SELECT '1' \"key\", '{\"x\":   123}' json_column, '{\"x\":123}' jsonb_column");
            assertQuery("SELECT * FROM test_json WHERE json_column = json'{\"x\":123}'", "SELECT '1' \"key\", '{\"x\":   123}' json_column, '{\"x\":123}' jsonb_column");
            assertUpdate("INSERT INTO test_json VALUES ('1', json'{\"x\":123}',json'{\"x\": 123}' )", 1);
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_json");
        }
    }

    private static DataType<byte[]> byteaDataType()
    {
        return DataType.dataType(
                "bytea",
                VARBINARY,
                bytes -> format("bytea E'\\\\x%s'", base16().encode(bytes)),
                identity());
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return prestoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup postgresCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl()), tableNamePrefix);
    }

    @Test
    public void testArray()
    {
        Session session = sessionWithArrayAsArray();
        // basic types
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(booleanDataType()), asList(true, false))
                .addRoundTrip(arrayDataType(bigintDataType()), asList(123_456_789_012L))
                .addRoundTrip(arrayDataType(integerDataType()), asList(1, 2, 1_234_567_890))
                .addRoundTrip(arrayDataType(smallintDataType()), asList((short) 32_456))
                .addRoundTrip(arrayDataType(doubleDataType()), asList(123.45d))
                .addRoundTrip(arrayDataType(realDataType()), asList(123.45f))
                .execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_array_basic"));

        arrayDateTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_array_date"));
        arrayDateTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_array_date"));

        arrayDecimalTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_array_decimal"));
        arrayDecimalTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_array_decimal"));

        arrayVarcharDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_array_varchar"));
        arrayVarcharDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_array_varchar"));

        arrayUnicodeDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_array_parameterized_char_unicode"));
        arrayUnicodeDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_array_parameterized_char_unicode"));
        arrayVarcharUnicodeDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_array_parameterized_varchar_unicode"));
        arrayVarcharUnicodeDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_array_parameterized_varchar_unicode"));
    }

    @Test
    public void testArrayEmptyOrNulls()
    {
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(bigintDataType()), asList())
                .addRoundTrip(arrayDataType(booleanDataType()), null)
                .addRoundTrip(arrayDataType(realDataType()), singletonList(null))
                .addRoundTrip(arrayDataType(integerDataType()), asList(1, null, 3, null))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_empty_or_nulls"));
    }

    private DataTypeTest arrayDecimalTest(Function<DataType<BigDecimal>, DataType<List<BigDecimal>>> arrayTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(3, 0)), asList(new BigDecimal("193"), new BigDecimal("19"), new BigDecimal("-193")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(3, 1)), asList(new BigDecimal("10.0"), new BigDecimal("10.1"), new BigDecimal("-10.1")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(4, 2)), asList(new BigDecimal("2"), new BigDecimal("2.3")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(24, 2)), asList(new BigDecimal("2"), new BigDecimal("2.3"), new BigDecimal("123456789.3")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(24, 4)), asList(new BigDecimal("12345678901234567890.31")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(30, 5)), asList(new BigDecimal("3141592653589793238462643.38327"), new BigDecimal("-3141592653589793238462643.38327")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(38, 0)), asList(
                        new BigDecimal("27182818284590452353602874713526624977"),
                        new BigDecimal("-27182818284590452353602874713526624977")));
    }

    private DataTypeTest arrayVarcharDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(10)), asList("text_a"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(255)), asList("text_b"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(65535)), asList("text_d"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(10485760)), asList("text_f"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType()), asList("unbounded"));
    }

    private DataTypeTest arrayVarcharUnicodeDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory)
    {
        return arrayUnicodeDataTypeTest(arrayTypeFactory, DataType::varcharDataType)
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType()), asList("\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!"));
    }

    private DataTypeTest arrayUnicodeDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory, Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(sampleUnicodeText.length())), asList(sampleUnicodeText))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(32)), asList(sampleUnicodeText))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(20000)), asList(sampleUnicodeText))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(1)), asList(sampleFourByteUnicodeCharacter));
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    private DataTypeTest arrayDateTest(Function<DataType<LocalDate>, DataType<List<LocalDate>>> arrayTypeFactory)
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(1952, 4, 3))) // before epoch
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(1970, 1, 1)))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(1970, 2, 3)))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(2017, 7, 1))) // summer on northern hemisphere (possible DST)
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(2017, 1, 1))) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(dateOfLocalTimeChangeForwardAtMidnightInJvmZone))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(dateOfLocalTimeChangeForwardAtMidnightInSomeZone))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone));
    }

    @Test
    public void testArrayMultidimensional()
    {
        // for multidimensional arrays, PostgreSQL requires subarrays to have the same dimensions, including nulls
        // e.g. [[1], [1, 2]] and [null, [1, 2]] are not allowed, but [[null, null], [1, 2]] is allowed
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(arrayDataType(booleanDataType())), asList(asList(null, null, null)))
                .addRoundTrip(arrayDataType(arrayDataType(booleanDataType())), asList(asList(true, null), asList(null, null), asList(false, false)))
                .addRoundTrip(arrayDataType(arrayDataType(integerDataType())), asList(asList(1, 2), asList(null, null), asList(3, 4)))
                .addRoundTrip(arrayDataType(arrayDataType(decimalDataType(3, 0))), asList(
                        asList(new BigDecimal("193")),
                        asList(new BigDecimal("19")),
                        asList(new BigDecimal("-193"))))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_2d"));

        DataTypeTest.create()
                .addRoundTrip(arrayDataType(arrayDataType(arrayDataType(doubleDataType()))), asList(
                        asList(asList(123.45d), asList(678.99d)),
                        asList(asList(543.21d), asList(998.76d)),
                        asList(asList(567.123d), asList(789.12d))))
                .addRoundTrip(arrayDataType(arrayDataType(arrayDataType(dateDataType()))), asList(
                        asList(asList(LocalDate.of(1952, 4, 3), LocalDate.of(1970, 1, 1))),
                        asList(asList(null, LocalDate.of(1970, 1, 1))),
                        asList(asList(LocalDate.of(1970, 2, 3), LocalDate.of(2017, 7, 1)))))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_3d"));
    }

    @Test
    public void testInternalArray()
    {
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(integerDataType(), "_int4"), asList(1, 2, 3))
                .addRoundTrip(arrayDataType(varcharDataType(), "_text"), asList("a", "b"))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_with_native_name"));
    }

    @Test
    public void testArrayAsJson()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("postgresql.array_mapping", AS_JSON.name())
                .build();

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("boolean[]"), null)
                .addRoundTrip(arrayAsJsonDataType("boolean[]"), "[[true,false],[false,true],[true,true]]")
                .addRoundTrip(arrayAsJsonDataType("boolean[3][2]"), "[[true,false],[false,true],[true,true]]")
                .addRoundTrip(arrayAsJsonDataType("boolean[100][100][100]"), "[true]")
                .addRoundTrip(arrayAsJsonDataType("_bool"), "[[true,false],[null,null]]")
                .addRoundTrip(arrayAsJsonDataType("_bool"), "[[[null]]]")
                .addRoundTrip(arrayAsJsonDataType("_bool"), "[]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_boolean_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("integer[]"), null)
                .addRoundTrip(arrayAsJsonDataType("integer[]"), "[[[1,2,3],[4,5,6]],[[7,8,9],[10,11,12]]]")
                .addRoundTrip(arrayAsJsonDataType("integer[100][100][100]"), "[0]")
                .addRoundTrip(arrayAsJsonDataType("integer[]"), "[[[null,null]]]")
                .addRoundTrip(arrayAsJsonDataType("integer[]"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_int4"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_int4"), "[[0],[1],[2],[3]]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_integer_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("varchar[]"), null)
                .addRoundTrip(arrayAsJsonDataType("varchar[]"), "[\"text\"]")
                .addRoundTrip(arrayAsJsonDataType("_text"), "[[\"one\",\"two\"],[\"three\",\"four\"]]")
                .addRoundTrip(arrayAsJsonDataType("_text"), "[[\"one\",null]]")
                .addRoundTrip(arrayAsJsonDataType("_text"), "[]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_varchar_array_as_json"));
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, format("ARRAY(%s)", elementType.getInsertType()));
    }

    private static <E> DataType<List<E>> postgresArrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, elementType.getInsertType() + "[]");
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType, String insertType)
    {
        return dataType(insertType,
                new ArrayType(elementType.getPrestoResultType()),
                valuesList -> "ARRAY" + valuesList.stream().map(elementType::toLiteral).collect(Collectors.toList()),
                valuesList -> valuesList == null ? null : valuesList.stream().map(elementType::toPrestoQueryResult).collect(Collectors.toList()));
    }

    private static DataType<String> arrayAsJsonDataType(String insertType)
    {
        return dataType(
                insertType,
                JSON,
                // naive conversion JSON array -> array literal, sufficient for tests
                value -> value
                        .replace("[", "ARRAY[")
                        .replace("\"", "'")
                        + "::" + insertType,
                identity());
    }

    private Session sessionWithArrayAsArray()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("postgresql.array_mapping", AS_ARRAY.name())
                .build();
    }
}
