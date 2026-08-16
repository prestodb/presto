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
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Function;

import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createJdbcProperties;
import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static com.facebook.presto.tests.datatype.DataType.bigintDataType;
import static com.facebook.presto.tests.datatype.DataType.booleanDataType;
import static com.facebook.presto.tests.datatype.DataType.dataType;
import static com.facebook.presto.tests.datatype.DataType.dateDataType;
import static com.facebook.presto.tests.datatype.DataType.decimalDataType;
import static com.facebook.presto.tests.datatype.DataType.doubleDataType;
import static com.facebook.presto.tests.datatype.DataType.integerDataType;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private final PostgreSQLContainer postgresContainer;

    public TestPostgreSqlTypeMapping()
            throws Exception
    {
        this.postgresContainer = new PostgreSQLContainer("postgres:14")
                .withDatabaseName("tpch")
                .withUsername("testuser")
                .withPassword("testpass");
        this.postgresContainer.start();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createPostgreSqlQueryRunner(postgresContainer.getJdbcUrl(), ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        postgresContainer.stop();
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
        LocalDate dateOfLocalTimeChangeForwardAtHour2InJvmZone = LocalDate.of(2012, 4, 1);
        verify(jvmZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtHour2InJvmZone.atTime(2, 1)).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(someZone.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(dateDataType(), LocalDate.of(1937, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtHour2InJvmZone)
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
    public void testTimestampUnderlyingStorageVerification()
            throws Exception
    {
        String jdbcUrl = postgresContainer.getJdbcUrl();
        String jdbcUrlWithCredentials = format("%s%suser=%s&password=%s",
                jdbcUrl,
                jdbcUrl.contains("?") ? "&" : "?",
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
        JdbcSqlExecutor jdbcExecutor = new JdbcSqlExecutor(jdbcUrlWithCredentials);

        try {
            jdbcExecutor.execute("CREATE TABLE tpch.test_timestamp_storage (" +
                    "id INT PRIMARY KEY, " +
                    "ts TIMESTAMP WITHOUT TIME ZONE, " +
                    "source VARCHAR(10))");

            // Postgres insertion, Postgres retrieval, and Presto retrieval all agree on wall clock time
            jdbcExecutor.execute("INSERT INTO tpch.test_timestamp_storage VALUES (1, '1970-01-01 00:00:00.000000'::timestamp, 'jdbc')");

            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT ts::text FROM tpch.test_timestamp_storage WHERE id = 1")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue1 = rs.getString(1);
                assertEquals(dbValue1, "1970-01-01 00:00:00", "JDBC insert should store wall clock time 1970-01-01 00:00:00 in DB");
            }

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("legacy_timestamp", "false")
                    .build();
            assertQuery(session,
                    "SELECT ts FROM postgresql.tpch.test_timestamp_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1970-01-01 00:00:00.000000'");

            // Presto insertion, retrieval via Postgres, and retrieval via Presto all agree on wall clock time
            assertUpdate(session, "INSERT INTO postgresql.tpch.test_timestamp_storage VALUES (2, TIMESTAMP '2023-06-15 14:30:00.000000', 'presto')", 1);

            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT ts::text FROM tpch.test_timestamp_storage WHERE id = 2")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue2 = rs.getString(1);
                assertEquals(dbValue2, "2023-06-15 14:30:00", "Presto insert should store wall clock time 2023-06-15 14:30:00 in DB");
            }

            assertQuery(session,
                    "SELECT ts FROM postgresql.tpch.test_timestamp_storage WHERE id = 2",
                    "VALUES TIMESTAMP '2023-06-15 14:30:00.000000'");

            for (String timeZoneId : ImmutableList.of("UTC", "America/New_York", "Asia/Tokyo", "Europe/Warsaw")) {
                Session sessionWithTimezone = Session.builder(getQueryRunner().getDefaultSession())
                        .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                        .setSystemProperty("legacy_timestamp", "false")
                        .build();

                assertQuery(sessionWithTimezone,
                        "SELECT ts FROM postgresql.tpch.test_timestamp_storage WHERE id = 1",
                        "VALUES TIMESTAMP '1970-01-01 00:00:00.000000'");

                assertQuery(sessionWithTimezone,
                        "SELECT ts FROM postgresql.tpch.test_timestamp_storage WHERE id = 2",
                        "VALUES TIMESTAMP '2023-06-15 14:30:00.000000'");
            }
        }
        finally {
            jdbcExecutor.execute("DROP TABLE IF EXISTS tpch.test_timestamp_storage");
        }
    }

    @Test
    public void testTimestampLegacyUnderlyingStorageVerification()
            throws Exception
    {
        String jdbcUrl = postgresContainer.getJdbcUrl();
        String jdbcUrlWithCredentials = format("%s%suser=%s&password=%s",
                jdbcUrl,
                jdbcUrl.contains("?") ? "&" : "?",
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
        JdbcSqlExecutor jdbcExecutor = new JdbcSqlExecutor(jdbcUrlWithCredentials);

        try {
            jdbcExecutor.execute("CREATE TABLE tpch.test_timestamp_legacy_storage (" +
                    "id INT PRIMARY KEY, " +
                    "ts TIMESTAMP WITHOUT TIME ZONE, " +
                    "source VARCHAR(10))");

            // Postgres insertion and Postgres retrieval agree, Presto incorrectly interprets DB value due to legacy mode
            jdbcExecutor.execute("INSERT INTO tpch.test_timestamp_legacy_storage VALUES (1, '1970-01-01 00:00:00.000000'::timestamp, 'jdbc')");

            // Prove that the value is 1970-01-01 00:00:00 by reading directly from the DB via JDBC
            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT ts::text FROM tpch.test_timestamp_legacy_storage WHERE id = 1")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue1 = rs.getString(1);
                assertEquals(dbValue1, "1970-01-01 00:00:00", "JDBC insert should store wall clock time 1970-01-01 00:00:00 in DB");
            }

            // Verify Presto reads it with legacy mode (interprets DB time as JVM timezone, displays in session timezone)
            // In legacy mode, DB value 1970-01-01 00:00:00 is interpreted as if it's in JVM timezone (America/Bahia_Banderas UTC-7)
            // and then converted to the session timezone. Since both are the same (America/Bahia_Banderas),
            // the offset comes from treating the wall-clock DB time as UTC, resulting in 1969-12-31 20:00:00
            Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("legacy_timestamp", "true")
                    .build();
            assertQuery(legacySession,
                    "SELECT ts FROM postgresql.tpch.test_timestamp_legacy_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1969-12-31 20:00:00.000000'");

            // Presto insertion with legacy mode, verify DB storage via JDBC (should apply JVM timezone conversion during write)
            assertUpdate(legacySession, "INSERT INTO postgresql.tpch.test_timestamp_legacy_storage VALUES (2, TIMESTAMP '2023-06-15 14:30:00.000000', 'presto')", 1);

            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT ts::text FROM tpch.test_timestamp_legacy_storage WHERE id = 2")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue2 = rs.getString(1);
                // JVM timezone is America/Bahia_Banderas (UTC-7), so 2023-06-15 14:30:00 becomes 2023-06-14 19:30:00
                assertEquals(dbValue2, "2023-06-14 19:30:00", "Legacy mode applies timezone conversion during write, expected 2023-06-14 19:30:00");
            }

            // Verify Presto reads it back correctly in legacy mode (round-trip should work)
            assertQuery(legacySession,
                    "SELECT ts FROM postgresql.tpch.test_timestamp_legacy_storage WHERE id = 2",
                    "VALUES TIMESTAMP '2023-06-15 14:30:00.000000'");

            // DB value 1970-01-01 00:00:00 is interpreted as JVM timezone (America/Bahia_Banderas UTC-7),
            // then converted to the session timezone
            Session legacyUtcSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("UTC"))
                    .setSystemProperty("legacy_timestamp", "true")
                    .build();
            assertQuery(legacyUtcSession,
                    "SELECT ts FROM postgresql.tpch.test_timestamp_legacy_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1970-01-01 07:00:00.000000'");

            Session legacyTokyoSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Tokyo"))
                    .setSystemProperty("legacy_timestamp", "true")
                    .build();
            assertQuery(legacyTokyoSession,
                    "SELECT ts FROM postgresql.tpch.test_timestamp_legacy_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1970-01-01 16:00:00.000000'");
        }
        finally {
            jdbcExecutor.execute("DROP TABLE IF EXISTS tpch.test_timestamp_legacy_storage");
        }
    }

    @Test
    public void testJson()
    {
        jsonTestCases(jsonDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_json"));
    }

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
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgresContainer.getJdbcUrl(), createJdbcProperties(postgresContainer));
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

    private static DataType<byte[]> byteaDataType()
    {
        return DataType.dataType(
                "bytea",
                VARBINARY,
                bytes -> format("bytea E'\\\\x%s'", base16().encode(bytes)),
                Function.identity());
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup postgresCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new JdbcSqlExecutor(postgresContainer.getJdbcUrl(), createJdbcProperties(postgresContainer)), tableNamePrefix);
    }
}
