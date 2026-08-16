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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.datatype.CreateAndInsertDataSetup;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataSetup;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.facebook.presto.tests.sql.JdbcSqlExecutor;
import com.facebook.presto.tests.sql.PrestoSqlExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.mysql.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.tests.datatype.DataType.bigintDataType;
import static com.facebook.presto.tests.datatype.DataType.charDataType;
import static com.facebook.presto.tests.datatype.DataType.dateDataType;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String CHARACTER_SET_UTF8 = "CHARACTER SET utf8";

    private final MySQLContainer mysqlContainer;

    public TestMySqlTypeMapping()
    {
        this.mysqlContainer = new MySQLContainer("mysql:8.0")
                .withDatabaseName("tpch")
                .withUsername("testuser")
                .withPassword("testpass");
        this.mysqlContainer.start();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMySqlQueryRunner(mysqlContainer.getJdbcUrl(), ImmutableMap.of(), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlContainer.stop();
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
                .addRoundTrip(varcharDataType(15000), "f")
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
        // Note: there is identical test for PostgreSQL

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
            testCases.execute(getQueryRunner(), session, mysqlCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, prestoCreateAsSelect("test_date"));
        }
    }

    @Test
    public void testDatetimeUnderlyingStorageVerification()
            throws Exception
    {
        String jdbcUrl = mysqlContainer.getJdbcUrl();
        String jdbcUrlWithCredentials = format("%s%suser=%s&password=%s",
                jdbcUrl,
                jdbcUrl.contains("?") ? "&" : "?",
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
        JdbcSqlExecutor jdbcExecutor = new JdbcSqlExecutor(jdbcUrlWithCredentials);

        try {
            jdbcExecutor.execute("CREATE TABLE tpch.test_datetime_storage (" +
                    "id INT PRIMARY KEY, " +
                    "dt DATETIME(6), " +
                    "source VARCHAR(10))");

            // MySQL insertion, MySQL retrieval, and Presto retrieval all agree on wall clock time
            jdbcExecutor.execute("INSERT INTO tpch.test_datetime_storage VALUES (1, '1970-01-01 00:00:00.000000', 'jdbc')");

            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CAST(dt AS CHAR) FROM tpch.test_datetime_storage WHERE id = 1")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue1 = rs.getString(1);
                assertEquals(dbValue1, "1970-01-01 00:00:00.000000", "JDBC insert should store wall clock time 1970-01-01 00:00:00 in DB");
            }

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("legacy_timestamp", "false")
                    .build();
            assertQuery(session,
                    "SELECT dt FROM mysql.tpch.test_datetime_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1970-01-01 00:00:00.000000'");

            // Presto insertion, retrieval via MySQL, and retrieval via Presto all agree on wall clock time
            assertUpdate(session, "INSERT INTO mysql.tpch.test_datetime_storage VALUES (2, TIMESTAMP '2023-06-15 14:30:00.000000', 'presto')", 1);

            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CAST(dt AS CHAR) FROM tpch.test_datetime_storage WHERE id = 2")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue2 = rs.getString(1);
                assertEquals(dbValue2, "2023-06-15 14:30:00.000000", "Presto insert should store wall clock time 2023-06-15 14:30:00 in DB");
            }

            assertQuery(session,
                    "SELECT dt FROM mysql.tpch.test_datetime_storage WHERE id = 2",
                    "VALUES TIMESTAMP '2023-06-15 14:30:00.000000'");

            for (String timeZoneId : ImmutableList.of("UTC", "America/New_York", "Asia/Tokyo", "Europe/Warsaw")) {
                Session sessionWithTimezone = Session.builder(getQueryRunner().getDefaultSession())
                        .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                        .setSystemProperty("legacy_timestamp", "false")
                        .build();

                assertQuery(sessionWithTimezone,
                        "SELECT dt FROM mysql.tpch.test_datetime_storage WHERE id = 1",
                        "VALUES TIMESTAMP '1970-01-01 00:00:00.000000'");

                assertQuery(sessionWithTimezone,
                        "SELECT dt FROM mysql.tpch.test_datetime_storage WHERE id = 2",
                        "VALUES TIMESTAMP '2023-06-15 14:30:00.000000'");
            }
        }
        finally {
            jdbcExecutor.execute("DROP TABLE IF EXISTS tpch.test_datetime_storage");
        }
    }

    @Test
    public void testDatetimeLegacyUnderlyingStorageVerification()
            throws Exception
    {
        String jdbcUrl = mysqlContainer.getJdbcUrl();
        String jdbcUrlWithCredentials = format("%s%suser=%s&password=%s",
                jdbcUrl,
                jdbcUrl.contains("?") ? "&" : "?",
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
        JdbcSqlExecutor jdbcExecutor = new JdbcSqlExecutor(jdbcUrlWithCredentials);

        try {
            jdbcExecutor.execute("CREATE TABLE tpch.test_datetime_legacy_storage (" +
                    "id INT PRIMARY KEY, " +
                    "dt DATETIME(6), " +
                    "source VARCHAR(10))");

            // MySQL insertion and MySQL retrieval agree, Presto incorrectly interprets DB value due to legacy mode
            jdbcExecutor.execute("INSERT INTO tpch.test_datetime_legacy_storage VALUES (1, '1970-01-01 00:00:00.000000', 'jdbc')");

            // Prove that the value is 1970-01-01 00:00:00 by reading directly from the DB via JDBC
            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CAST(dt AS CHAR) FROM tpch.test_datetime_legacy_storage WHERE id = 1")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue1 = rs.getString(1);
                assertEquals(dbValue1, "1970-01-01 00:00:00.000000", "JDBC insert should store wall clock time 1970-01-01 00:00:00 in DB");
            }

            // In legacy mode, DB value 1970-01-01 00:00:00 is interpreted as if it's in JVM timezone (America/Bahia_Banderas UTC-7)
            // and then converted to the session timezone. Since both are the same (America/Bahia_Banderas),
            // the offset comes from treating the wall-clock DB time as UTC, resulting in 1969-12-31 20:00:00
            Session legacySession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty("legacy_timestamp", "true")
                    .build();
            assertQuery(legacySession,
                    "SELECT dt FROM mysql.tpch.test_datetime_legacy_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1969-12-31 20:00:00.000000'");

            // Presto insertion with legacy mode, verify DB storage via JDBC (should apply JVM timezone conversion during write)
            assertUpdate(legacySession, "INSERT INTO mysql.tpch.test_datetime_legacy_storage VALUES (2, TIMESTAMP '2023-06-15 14:30:00.000000', 'presto')", 1);

            try (Connection conn = DriverManager.getConnection(jdbcUrlWithCredentials);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CAST(dt AS CHAR) FROM tpch.test_datetime_legacy_storage WHERE id = 2")) {
                assertTrue(rs.next(), "Expected one row");
                String dbValue2 = rs.getString(1);
                // JVM timezone is America/Bahia_Banderas (UTC-7), so 2023-06-15 14:30:00 becomes 2023-06-14 19:30:00.000000
                assertEquals(dbValue2, "2023-06-14 19:30:00.000000", "Legacy mode applies timezone conversion during write, expected 2023-06-14 19:30:00.000000");
            }

            // Verify Presto reads it back correctly in legacy mode (round-trip should work)
            assertQuery(legacySession,
                    "SELECT dt FROM mysql.tpch.test_datetime_legacy_storage WHERE id = 2",
                    "VALUES TIMESTAMP '2023-06-15 14:30:00.000000'");

            // DB value 1970-01-01 00:00:00 is interpreted as JVM timezone (America/Bahia_Banderas UTC-7),
            // then converted to the session timezone
            Session legacyUtcSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("UTC"))
                    .setSystemProperty("legacy_timestamp", "true")
                    .build();
            assertQuery(legacyUtcSession,
                    "SELECT dt FROM mysql.tpch.test_datetime_legacy_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1970-01-01 07:00:00.000000'");

            Session legacyTokyoSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Tokyo"))
                    .setSystemProperty("legacy_timestamp", "true")
                    .build();
            assertQuery(legacyTokyoSession,
                    "SELECT dt FROM mysql.tpch.test_datetime_legacy_storage WHERE id = 1",
                    "VALUES TIMESTAMP '1970-01-01 16:00:00.000000'");
        }
        finally {
            jdbcExecutor.execute("DROP TABLE IF EXISTS tpch.test_datetime_legacy_storage");
        }
    }

    private void testUnsupportedDataType(String databaseDataType)
    {
        String jdbcUrl = mysqlContainer.getJdbcUrl();
        String jdbcUrlWithCredentials = format("%s%suser=%s&password=%s",
                jdbcUrl,
                jdbcUrl.contains("?") ? "&" : "?",
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(jdbcUrlWithCredentials);
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
        String jdbcUrl = mysqlContainer.getJdbcUrl();
        String jdbcUrlWithCredentials = format("%s%suser=%s&password=%s&useUnicode=true&characterEncoding=utf8",
                jdbcUrl,
                jdbcUrl.contains("?") ? "&" : "?",
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
        JdbcSqlExecutor mysqlUnicodeExecutor = new JdbcSqlExecutor(jdbcUrlWithCredentials);
        return new CreateAndInsertDataSetup(mysqlUnicodeExecutor, tableNamePrefix);
    }
}
