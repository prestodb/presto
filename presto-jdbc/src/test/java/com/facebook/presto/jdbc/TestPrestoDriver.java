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
package com.facebook.presto.jdbc;

import com.facebook.airlift.log.Logging;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.type.ColorType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertContains;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Float.POSITIVE_INFINITY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoDriver
{
    private static final DateTimeZone ASIA_ORAL_ZONE = DateTimeZone.forID("Asia/Oral");
    private static final GregorianCalendar ASIA_ORAL_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone(ZoneId.of(ASIA_ORAL_ZONE.getID())));
    private static final String TEST_CATALOG = "test_catalog";

    private TestingPrestoServer server;
    private ExecutorService executorService;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        Catalog bogusTestingCatalog = createBogusTestingCatalog(TESTING_CATALOG);
        server.getCatalogManager().registerCatalog(bogusTestingCatalog);
        SessionPropertyManager sessionPropertyManager = server.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), TEST_CATALOG_PROPERTIES);
        waitForNodeRefresh(server);
        setupTestTables();
        executorService = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    static void waitForNodeRefresh(TestingPrestoServer server)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (server.refreshNodes().getActiveNodes().size() < 1) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }

    private void setupTestTables()
            throws SQLException
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate("CREATE SCHEMA blackhole.blackhole"), 0);
            assertEquals(statement.executeUpdate("CREATE TABLE test_table (x bigint)"), 0);

            assertEquals(statement.executeUpdate("CREATE TABLE slow_test_table (x bigint) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")"), 0);
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        executorService.shutdownNow();
    }

    @Test
    public void testDriverManager()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT " +
                        "  123 _integer" +
                        ",  12300000000 _bigint" +
                        ", 'foo' _varchar" +
                        ", 0.1E0 _double" +
                        ", true _boolean" +
                        ", cast('hello' as varbinary) _varbinary" +
                        ", DECIMAL '1234567890.1234567' _decimal_short" +
                        ", DECIMAL '.12345678901234567890123456789012345678' _decimal_long" +
                        ", approx_set(42) _hll" +
                        ", cast('foo' as char(5)) _char")) {
                    ResultSetMetaData metadata = rs.getMetaData();

                    assertEquals(metadata.getColumnCount(), 10);

                    assertEquals(metadata.getColumnLabel(1), "_integer");
                    assertEquals(metadata.getColumnType(1), Types.INTEGER);

                    assertEquals(metadata.getColumnLabel(2), "_bigint");
                    assertEquals(metadata.getColumnType(2), Types.BIGINT);

                    assertEquals(metadata.getColumnLabel(3), "_varchar");
                    assertEquals(metadata.getColumnType(3), Types.VARCHAR);

                    assertEquals(metadata.getColumnLabel(4), "_double");
                    assertEquals(metadata.getColumnType(4), Types.DOUBLE);

                    assertEquals(metadata.getColumnLabel(5), "_boolean");
                    assertEquals(metadata.getColumnType(5), Types.BOOLEAN);

                    assertEquals(metadata.getColumnLabel(6), "_varbinary");
                    assertEquals(metadata.getColumnType(6), Types.VARBINARY);

                    assertEquals(metadata.getColumnLabel(7), "_decimal_short");
                    assertEquals(metadata.getColumnType(7), Types.DECIMAL);

                    assertEquals(metadata.getColumnLabel(8), "_decimal_long");
                    assertEquals(metadata.getColumnType(8), Types.DECIMAL);

                    assertEquals(metadata.getColumnLabel(9), "_hll");
                    assertEquals(metadata.getColumnType(9), Types.JAVA_OBJECT);

                    assertEquals(metadata.getColumnLabel(10), "_char");
                    assertEquals(metadata.getColumnType(10), Types.CHAR);

                    assertTrue(rs.next());

                    assertEquals(rs.getObject(1), 123);
                    assertEquals(rs.getObject("_integer"), 123);
                    assertEquals(rs.getInt(1), 123);
                    assertEquals(rs.getInt("_integer"), 123);
                    assertEquals(rs.getLong(1), 123L);
                    assertEquals(rs.getLong("_integer"), 123L);

                    assertEquals(rs.getObject(2), 12300000000L);
                    assertEquals(rs.getObject("_bigint"), 12300000000L);
                    assertEquals(rs.getLong(2), 12300000000L);
                    assertEquals(rs.getLong("_bigint"), 12300000000L);

                    assertEquals(rs.getObject(3), "foo");
                    assertEquals(rs.getObject("_varchar"), "foo");
                    assertEquals(rs.getString(3), "foo");
                    assertEquals(rs.getString("_varchar"), "foo");

                    assertEquals(rs.getObject(4), 0.1);
                    assertEquals(rs.getObject("_double"), 0.1);
                    assertEquals(rs.getDouble(4), 0.1);
                    assertEquals(rs.getDouble("_double"), 0.1);

                    assertEquals(rs.getObject(5), true);
                    assertEquals(rs.getObject("_boolean"), true);
                    assertEquals(rs.getBoolean(5), true);
                    assertEquals(rs.getBoolean("_boolean"), true);
                    assertEquals(rs.getByte("_boolean"), 1);
                    assertEquals(rs.getShort("_boolean"), 1);
                    assertEquals(rs.getInt("_boolean"), 1);
                    assertEquals(rs.getLong("_boolean"), 1L);
                    assertEquals(rs.getFloat("_boolean"), 1.0f);
                    assertEquals(rs.getDouble("_boolean"), 1.0);

                    assertEquals(rs.getObject(6), "hello".getBytes(UTF_8));
                    assertEquals(rs.getObject("_varbinary"), "hello".getBytes(UTF_8));
                    assertEquals(rs.getBytes(6), "hello".getBytes(UTF_8));
                    assertEquals(rs.getBytes("_varbinary"), "hello".getBytes(UTF_8));

                    assertEquals(rs.getObject(7), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getObject("_decimal_short"), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getBigDecimal(7), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getBigDecimal("_decimal_short"), new BigDecimal("1234567890.1234567"));
                    assertEquals(rs.getBigDecimal(7, 1), new BigDecimal("1234567890.1"));
                    assertEquals(rs.getBigDecimal("_decimal_short", 1), new BigDecimal("1234567890.1"));

                    assertEquals(rs.getObject(8), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getObject("_decimal_long"), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getBigDecimal(8), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getBigDecimal("_decimal_long"), new BigDecimal(".12345678901234567890123456789012345678"));
                    assertEquals(rs.getBigDecimal(8, 6), new BigDecimal(".123457"));
                    assertEquals(rs.getBigDecimal("_decimal_long", 6), new BigDecimal(".123457"));

                    assertInstanceOf(rs.getObject(9), byte[].class);
                    assertInstanceOf(rs.getObject("_hll"), byte[].class);
                    assertInstanceOf(rs.getBytes(9), byte[].class);
                    assertInstanceOf(rs.getBytes("_hll"), byte[].class);

                    assertEquals(rs.getObject(10), "foo  ");
                    assertEquals(rs.getObject("_char"), "foo  ");
                    assertEquals(rs.getString(10), "foo  ");
                    assertEquals(rs.getString("_char"), "foo  ");

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT " +
                        "  TIME '3:04:05' as a" +
                        ", TIME '6:07:08 +06:17' as b" +
                        ", TIME '9:10:11 Europe/Berlin' as c" +
                        ", TIMESTAMP '2001-02-03 3:04:05' as d" +
                        ", TIMESTAMP '2004-05-06 6:07:08 +06:17' as e" +
                        ", TIMESTAMP '2007-08-09 9:10:11 Europe/Berlin' as f" +
                        ", DATE '2013-03-22' as g" +
                        ", INTERVAL '123-11' YEAR TO MONTH as h" +
                        ", INTERVAL '11 22:33:44.555' DAY TO SECOND as i" +
                        ", REAL '123.45' as j" +
                        ", REAL 'Infinity' as k" +
                        "")) {
                    assertTrue(rs.next());

                    assertEquals(rs.getTime(1), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime(1, ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject(1), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("a"), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTime("a", ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject("a"), new Time(new DateTime(1970, 1, 1, 3, 4, 5).getMillis()));

                    assertEquals(rs.getTime(2), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime(2, ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(2), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime("b"), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTime("b", ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("b"), new Time(new DateTime(1970, 1, 1, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));

                    assertEquals(rs.getTime(3), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime(3, ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject(3), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime("c"), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTime("c", ASIA_ORAL_CALENDAR), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject("c"), new Time(new DateTime(1970, 1, 1, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));

                    assertEquals(rs.getTimestamp(4), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp(4, ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject(4), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp("d"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));
                    assertEquals(rs.getTimestamp("d", ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject("d"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5).getMillis()));

                    assertEquals(rs.getTimestamp(5), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp(5, ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject(5), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp("e"), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getTimestamp("e", ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));
                    assertEquals(rs.getObject("e"), new Timestamp(new DateTime(2004, 5, 6, 6, 7, 8, DateTimeZone.forOffsetHoursMinutes(6, 17)).getMillis()));

                    assertEquals(rs.getTimestamp(6), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp(6, ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject(6), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp("f"), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getTimestamp("f", ASIA_ORAL_CALENDAR), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));
                    assertEquals(rs.getObject("f"), new Timestamp(new DateTime(2007, 8, 9, 9, 10, 11, DateTimeZone.forID("Europe/Berlin")).getMillis()));

                    assertEquals(rs.getDate(7), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertEquals(rs.getDate(7, ASIA_ORAL_CALENDAR), new Date(new DateTime(2013, 3, 22, 0, 0, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject(7), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertEquals(rs.getDate("g"), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));
                    assertEquals(rs.getDate("g", ASIA_ORAL_CALENDAR), new Date(new DateTime(2013, 3, 22, 0, 0, ASIA_ORAL_ZONE).getMillis()));
                    assertEquals(rs.getObject("g"), new Date(new DateTime(2013, 3, 22, 0, 0).getMillis()));

                    assertEquals(rs.getObject(8), new PrestoIntervalYearMonth(123, 11));
                    assertEquals(rs.getObject("h"), new PrestoIntervalYearMonth(123, 11));
                    assertEquals(rs.getObject(9), new PrestoIntervalDayTime(11, 22, 33, 44, 555));
                    assertEquals(rs.getObject("i"), new PrestoIntervalDayTime(11, 22, 33, 44, 555));

                    assertEquals(rs.getFloat(10), 123.45f);
                    assertEquals(rs.getObject(10), 123.45f);
                    assertEquals(rs.getFloat("j"), 123.45f);
                    assertEquals(rs.getObject("j"), 123.45f);

                    assertEquals(rs.getFloat(11), POSITIVE_INFINITY);
                    assertEquals(rs.getObject(11), POSITIVE_INFINITY);
                    assertEquals(rs.getFloat("k"), POSITIVE_INFINITY);
                    assertEquals(rs.getObject("k"), POSITIVE_INFINITY);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testGetCatalogs()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                assertEquals(readRows(rs), list(list("blackhole"), list("system"), list(TEST_CATALOG), list(TESTING_CATALOG)));

                ResultSetMetaData metadata = rs.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);
                assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
                assertEquals(metadata.getColumnType(1), Types.VARCHAR);
            }
        }
    }

    @Test
    public void testGetDriverVersion()
            throws Exception
    {
        Driver driver = DriverManager.getDriver("jdbc:presto:");
        assertEquals(driver.getMajorVersion(), 0);
        assertEquals(driver.getMajorVersion(), 0);

        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDriverName(), PrestoDriver.DRIVER_NAME);
            assertEquals(metaData.getDriverVersion(), "unknown");
            assertEquals(metaData.getDriverMajorVersion(), 0);
            assertEquals(metaData.getDriverMinorVersion(), 0);
        }
    }

    @Test
    public void testGetDatabaseProductVersion()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDatabaseProductName(), "Presto");
            assertEquals(metaData.getDatabaseProductVersion(), "testversion");
            assertEquals(metaData.getDatabaseMajorVersion(), 0);
            assertEquals(metaData.getDatabaseMinorVersion(), 0);
        }
    }

    @Test
    public void testGetSchemas()
            throws Exception
    {
        List<List<String>> system = new ArrayList<>();
        system.add(list("system", "information_schema"));
        system.add(list("system", "jdbc"));
        system.add(list("system", "metadata"));
        system.add(list("system", "runtime"));

        List<List<String>> blackhole = new ArrayList<>();
        blackhole.add(list("blackhole", "information_schema"));
        blackhole.add(list("blackhole", "default"));
        blackhole.add(list("blackhole", "blackhole"));

        List<List<String>> test = new ArrayList<>();
        test.add(list(TEST_CATALOG, "information_schema"));
        for (String schema : TpchMetadata.SCHEMA_NAMES) {
            test.add(list(TEST_CATALOG, schema));
        }

        List<List<String>> all = new ArrayList<>();
        all.addAll(system);
        all.addAll(test);
        all.addAll(blackhole);

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                assertGetSchemasResult(rs, all);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, null)) {
                assertGetSchemasResult(rs, all);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, null)) {
                assertGetSchemasResult(rs, test);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("", null)) {
                // all schemas in presto have a catalog name
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "information_schema")) {
                assertGetSchemasResult(rs, list(list(TEST_CATALOG, "information_schema")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "information_schema")) {
                assertGetSchemasResult(rs, list(
                        list(TEST_CATALOG, "information_schema"),
                        list("blackhole", "information_schema"),
                        list("system", "information_schema")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sf_")) {
                assertGetSchemasResult(rs, list(list(TEST_CATALOG, "sf1")));
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sf%")) {
                List<List<String>> expected = test.stream()
                        .filter(item -> item.get(1).startsWith("sf"))
                        .collect(toList());
                assertGetSchemasResult(rs, expected);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", null)) {
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "unknown")) {
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "unknown")) {
                assertGetSchemasResult(rs, list());
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", "unknown")) {
                assertGetSchemasResult(rs, list());
            }
        }
    }

    private static void assertGetSchemasResult(ResultSet rs, List<List<String>> expectedSchemas)
            throws SQLException
    {
        List<List<Object>> data = readRows(rs);

        assertEquals(data.size(), expectedSchemas.size());
        for (List<Object> row : data) {
            assertTrue(expectedSchemas.contains(list((String) row.get(1), (String) row.get(0))));
        }

        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 2);

        assertEquals(metadata.getColumnLabel(1), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_CATALOG");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);
    }

    @Test
    public void testGetTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, null, null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // no tables have an empty catalog
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("", null, null, null)) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // no tables have an empty schema
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "", null, null)) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("TABLE"))) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, array("TABLE"))) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "inf%", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tab%", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // no matching catalog
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("unknown", "information_schema", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        // no matching schema
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "unknown", "tables", array("TABLE"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        // no matching table
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "unknown", array("TABLE"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        // no matching type
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("unknown"))) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array("unknown", "TABLE"))) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
            }
        }

        // empty type list
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", array())) {
                assertTableMetadata(rs);
                assertEquals(readRows(rs).size(), 0);
            }
        }
    }

    private static List<Object> getTablesRow(String schema, String table)
    {
        return list(TEST_CATALOG, schema, table, "TABLE", null, null, null, null, null, null);
    }

    private static void assertTableMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 10);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4), "TABLE_TYPE");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5), "REMARKS");
        assertEquals(metadata.getColumnType(5), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(6), "TYPE_CAT");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7), "TYPE_SCHEM");
        assertEquals(metadata.getColumnType(7), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(8), "TYPE_NAME");
        assertEquals(metadata.getColumnType(8), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(9), "SELF_REFERENCING_COL_NAME");
        assertEquals(metadata.getColumnType(9), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(10), "REF_GENERATION");
        assertEquals(metadata.getColumnType(10), Types.VARCHAR);
    }

    @Test
    public void testGetTableTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                List<List<Object>> data = readRows(tableTypes);
                assertEquals(data, list(list("TABLE"), list("VIEW")));

                ResultSetMetaData metadata = tableTypes.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);

                assertEquals(metadata.getColumnLabel(1), "TABLE_TYPE");
                assertEquals(metadata.getColumnType(1), Types.VARCHAR);
            }
        }
    }

    @Test
    public void testGetColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, null, "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "blackhole");
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertEquals(rs.getString("TABLE_NAME"), "tables");
                assertEquals(rs.getString("COLUMN_NAME"), "table_name");
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "system");
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), "system");
                assertEquals(rs.getString("TABLE_SCHEM"), "jdbc");
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_CAT"), TEST_CATALOG);
                assertEquals(rs.getString("TABLE_SCHEM"), "information_schema");
                assertFalse(rs.next());
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, null, "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "information_schema", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 3);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "inf%", "tables", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 1);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tab%", "table_name")) {
                assertColumnMetadata(rs);
                assertEquals(readRows(rs).size(), 2);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "%m%")) {
                assertColumnMetadata(rs);
                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "table_schema");
                assertTrue(rs.next());
                assertEquals(rs.getString("COLUMN_NAME"), "table_name");
                assertFalse(rs.next());
            }
        }

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate(
                    "CREATE TABLE test_get_columns_table (" +
                            "c_boolean boolean, " +
                            "c_bigint bigint, " +
                            "c_integer integer, " +
                            "c_smallint smallint, " +
                            "c_tinyint tinyint, " +
                            "c_real real, " +
                            "c_double double, " +
                            "c_varchar_1234 varchar(1234), " +
                            "c_varchar varchar, " +
                            "c_char_345 char(345), " +
                            "c_varbinary varbinary, " +
                            "c_time time, " +
                            "c_time_with_time_zone time with time zone, " +
                            "c_timestamp timestamp, " +
                            "c_timestamp_with_time_zone timestamp with time zone, " +
                            "c_date date, " +
                            "c_decimal_8_2 decimal(8,2), " +
                            "c_decimal_38_0 decimal(38,0), " +
                            "c_array array<bigint>, " +
                            "c_color color" +
                            ")"), 0);

            try (ResultSet rs = connection.getMetaData().getColumns("blackhole", "blackhole", "test_get_columns_table", null)) {
                assertColumnMetadata(rs);
                assertColumnSpec(rs, Types.BOOLEAN, null, null, null, null, BooleanType.BOOLEAN);
                assertColumnSpec(rs, Types.BIGINT, 19L, 10L, null, null, BigintType.BIGINT);
                assertColumnSpec(rs, Types.INTEGER, 10L, 10L, null, null, IntegerType.INTEGER);
                assertColumnSpec(rs, Types.SMALLINT, 5L, 10L, null, null, SmallintType.SMALLINT);
                assertColumnSpec(rs, Types.TINYINT, 3L, 10L, null, null, TinyintType.TINYINT);
                assertColumnSpec(rs, Types.REAL, 24L, 2L, null, null, RealType.REAL);
                assertColumnSpec(rs, Types.DOUBLE, 53L, 2L, null, null, DoubleType.DOUBLE);
                assertColumnSpec(rs, Types.VARCHAR, 1234L, null, null, 1234L, createVarcharType(1234));
                assertColumnSpec(rs, Types.VARCHAR, (long) Integer.MAX_VALUE, null, null, (long) Integer.MAX_VALUE, createUnboundedVarcharType());
                assertColumnSpec(rs, Types.CHAR, 345L, null, null, 345L, createCharType(345));
                assertColumnSpec(rs, Types.VARBINARY, (long) Integer.MAX_VALUE, null, null, (long) Integer.MAX_VALUE, VarbinaryType.VARBINARY);
                assertColumnSpec(rs, Types.TIME, 8L, null, null, null, TimeType.TIME);
                assertColumnSpec(rs, Types.TIME_WITH_TIMEZONE, 14L, null, null, null, TimeWithTimeZoneType.TIME_WITH_TIME_ZONE);
                assertColumnSpec(rs, Types.TIMESTAMP, 23L, null, null, null, TimestampType.TIMESTAMP);
                assertColumnSpec(rs, Types.TIMESTAMP_WITH_TIMEZONE, 29L, null, null, null, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
                assertColumnSpec(rs, Types.DATE, 14L, null, null, null, DateType.DATE);
                assertColumnSpec(rs, Types.DECIMAL, 8L, 10L, 2L, null, createDecimalType(8, 2));
                assertColumnSpec(rs, Types.DECIMAL, 38L, 10L, 0L, null, createDecimalType(38, 0));
                assertColumnSpec(rs, Types.ARRAY, null, null, null, null, new ArrayType(BigintType.BIGINT));
                assertColumnSpec(rs, Types.JAVA_OBJECT, null, null, null, null, ColorType.COLOR);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertColumnSpec(ResultSet rs, int jdbcType, Long columnSize, Long numPrecRadix, Long decimalDigits, Long charOctetLength, Type type)
            throws SQLException
    {
        String message = " of " + type.getDisplayName() + ": ";
        assertTrue(rs.next());
        assertEquals(rs.getObject("DATA_TYPE"), (long) jdbcType, "DATA_TYPE" + message);
        assertEquals(rs.getObject("COLUMN_SIZE"), columnSize, "COLUMN_SIZE" + message);
        assertEquals(rs.getObject("NUM_PREC_RADIX"), numPrecRadix, "NUM_PREC_RADIX" + message);
        assertEquals(rs.getObject("DECIMAL_DIGITS"), decimalDigits, "DECIMAL_DIGITS" + message);
        assertEquals(rs.getObject("CHAR_OCTET_LENGTH"), charOctetLength, "CHAR_OCTET_LENGTH" + message);
    }

    private static void assertColumnMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 24);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4), "COLUMN_NAME");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5), "DATA_TYPE");
        assertEquals(metadata.getColumnType(5), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(6), "TYPE_NAME");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7), "COLUMN_SIZE");
        assertEquals(metadata.getColumnType(7), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(8), "BUFFER_LENGTH");
        assertEquals(metadata.getColumnType(8), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(9), "DECIMAL_DIGITS");
        assertEquals(metadata.getColumnType(9), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(10), "NUM_PREC_RADIX");
        assertEquals(metadata.getColumnType(10), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(11), "NULLABLE");
        assertEquals(metadata.getColumnType(11), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(12), "REMARKS");
        assertEquals(metadata.getColumnType(12), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(13), "COLUMN_DEF");
        assertEquals(metadata.getColumnType(13), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(14), "SQL_DATA_TYPE");
        assertEquals(metadata.getColumnType(14), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(15), "SQL_DATETIME_SUB");
        assertEquals(metadata.getColumnType(15), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(16), "CHAR_OCTET_LENGTH");
        assertEquals(metadata.getColumnType(16), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(17), "ORDINAL_POSITION");
        assertEquals(metadata.getColumnType(17), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(18), "IS_NULLABLE");
        assertEquals(metadata.getColumnType(18), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(19), "SCOPE_CATALOG");
        assertEquals(metadata.getColumnType(19), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(20), "SCOPE_SCHEMA");
        assertEquals(metadata.getColumnType(20), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(21), "SCOPE_TABLE");
        assertEquals(metadata.getColumnType(21), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(22), "SOURCE_DATA_TYPE");
        assertEquals(metadata.getColumnType(22), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(23), "IS_AUTOINCREMENT");
        assertEquals(metadata.getColumnType(23), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(24), "IS_GENERATEDCOLUMN");
        assertEquals(metadata.getColumnType(24), Types.VARCHAR);
    }

    @Test
    public void testGetPseudoColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getPseudoColumns(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetProcedures()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getProcedures(null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetProcedureColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getProcedureColumns(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetSuperTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSuperTables(null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetUdts()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getUDTs(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetAttributes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getAttributes(null, null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetSuperTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSuperTypes(null, null, null)) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testExecuteWithQuery()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z"));
                ResultSet rs = statement.getResultSet();

                assertEquals(statement.getUpdateCount(), -1);
                assertEquals(statement.getLargeUpdateCount(), -1);
                assertTrue(rs.next());

                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.wasNull());
                assertEquals(rs.getLong("x"), 123);
                assertFalse(rs.wasNull());

                assertEquals(rs.getLong(3), 0);
                assertTrue(rs.wasNull());
                assertEquals(rs.getLong("z"), 0);
                assertTrue(rs.wasNull());
                assertNull(rs.getObject("z"));
                assertTrue(rs.wasNull());

                assertEquals(rs.getString(2), "foo");
                assertFalse(rs.wasNull());
                assertEquals(rs.getString("y"), "foo");
                assertFalse(rs.wasNull());

                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testExecuteUpdateWithInsert()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(statement.executeUpdate("INSERT INTO test_table VALUES (1), (2)"), 2);
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 2);
                assertEquals(statement.getLargeUpdateCount(), 2);
            }
        }
    }

    @Test
    public void testExecuteUpdateWithCreateTable()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(statement.executeUpdate("CREATE TABLE test_execute_create (x bigint)"), 0);
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 0);
                assertEquals(statement.getLargeUpdateCount(), 0);
            }
        }
    }

    @Test
    public void testExecuteUpdateWithQuery()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                String sql = "SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z";
                try {
                    statement.executeUpdate(sql);
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "SQL is not an update statement: " + sql);
                }
            }
        }
    }

    @Test
    public void testExecuteQueryWithInsert()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                String sql = "INSERT INTO test_table VALUES (1)";
                try {
                    statement.executeQuery(sql);
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "SQL statement is not a query: " + sql);
                }
            }
        }
    }

    @Test
    public void testStatementReuse()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                // update statement
                assertFalse(statement.execute("INSERT INTO test_table VALUES (1), (2)"));
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 2);
                assertEquals(statement.getLargeUpdateCount(), 2);

                // query statement
                assertTrue(statement.execute("SELECT 123 x, 'foo' y, CAST(NULL AS bigint) z"));
                ResultSet resultSet = statement.getResultSet();
                assertNotNull(resultSet);
                assertEquals(statement.getUpdateCount(), -1);
                assertEquals(statement.getLargeUpdateCount(), -1);
                resultSet.close();

                // update statement
                assertFalse(statement.execute("INSERT INTO test_table VALUES (1), (2), (3)"));
                assertNull(statement.getResultSet());
                assertEquals(statement.getUpdateCount(), 3);
                assertEquals(statement.getLargeUpdateCount(), 3);
            }
        }
    }

    @Test
    public void testGetUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                assertEquals(statement.getUpdateCount(), -1);
                assertEquals(statement.getLargeUpdateCount(), -1);
            }
        }
    }

    @Test
    public void testResultSetClose()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet result = statement.getResultSet();
                assertFalse(result.isClosed());
                result.close();
                assertTrue(result.isClosed());
            }
        }
    }

    @Test
    public void testGetResultSet()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet result = statement.getResultSet();
                assertNotNull(result);
                assertFalse(result.isClosed());
                statement.getMoreResults();
                assertTrue(result.isClosed());

                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                result = statement.getResultSet();
                assertNotNull(result);
                assertFalse(result.isClosed());

                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                assertFalse(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            }
        }
    }

    @Test(expectedExceptions = SQLFeatureNotSupportedException.class, expectedExceptionsMessageRegExp = "Multiple open results not supported")
    public void testGetMoreResultsException()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        }
    }

    @Test
    public void testGetMoreResultsClearsUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "default")) {
            try (PrestoStatement statement = connection.createStatement().unwrap(PrestoStatement.class)) {
                assertFalse(statement.execute("CREATE TABLE test_more_results_clears_update_count (id bigint)"));
                assertEquals(statement.getUpdateCount(), 0);
                assertEquals(statement.getUpdateType(), "CREATE TABLE");
                assertFalse(statement.getMoreResults());
                assertEquals(statement.getUpdateCount(), -1);
                assertNull(statement.getUpdateType());
            }
            finally {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE test_more_results_clears_update_count");
                }
            }
        }
    }

    @Test
    public void testSetTimeZoneId()
            throws Exception
    {
        TimeZoneKey defaultZoneKey = TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID());
        DateTimeZone defaultZone = DateTimeZone.forTimeZone(TimeZone.getDefault());
        String sql = "SELECT current_timezone() zone, TIMESTAMP '2001-02-03 3:04:05' ts";

        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("zone"), defaultZoneKey.getId());
                assertEquals(rs.getTimestamp("ts"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, defaultZone).getMillis()));
            }

            connection.unwrap(PrestoConnection.class).setTimeZoneId("UTC");
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("zone"), "UTC");
                assertEquals(rs.getTimestamp("ts"), new Timestamp(new DateTime(2001, 2, 3, 3, 4, 5, DateTimeZone.UTC).getMillis()));
            }
        }
    }

    @Test
    public void testConnectionStringWithCatalogAndSchema()
            throws Exception
    {
        String prefix = format("jdbc:presto://%s", server.getAddress());

        Connection connection;
        connection = DriverManager.getConnection(prefix + "/a/b/", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "b");

        connection = DriverManager.getConnection(prefix + "/a/b", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "b");

        connection = DriverManager.getConnection(prefix + "/a/", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertNull(connection.getSchema());

        connection = DriverManager.getConnection(prefix + "/a", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertNull(connection.getSchema());

        connection = DriverManager.getConnection(prefix + "/", "test", null);
        assertNull(connection.getCatalog());
        assertNull(connection.getSchema());

        connection = DriverManager.getConnection(prefix, "test", null);
        assertNull(connection.getCatalog());
        assertNull(connection.getSchema());
    }

    @Test
    public void testConnectionWithCatalogAndSchema()
            throws Exception
    {
        try (Connection connection = createConnection(TEST_CATALOG, "information_schema")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT table_catalog, table_schema " +
                        "FROM tables " +
                        "WHERE table_schema = 'information_schema' " +
                        "  AND table_name = 'tables'")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 2);
                    assertEquals(metadata.getColumnLabel(1), "table_catalog");
                    assertEquals(metadata.getColumnLabel(2), "table_schema");
                    assertTrue(rs.next());
                    assertEquals(rs.getString("table_catalog"), TEST_CATALOG);
                }
            }
        }
    }

    @Test
    public void testConnectionWithCatalog()
            throws Exception
    {
        try (Connection connection = createConnection(TEST_CATALOG)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT table_catalog, table_schema " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = 'information_schema' " +
                        "  AND table_name = 'tables'")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 2);
                    assertEquals(metadata.getColumnLabel(1), "table_catalog");
                    assertEquals(metadata.getColumnLabel(2), "table_schema");
                    assertTrue(rs.next());
                    assertEquals(rs.getString("table_catalog"), TEST_CATALOG);
                }
            }
        }
    }

    @Test
    public void testConnectionResourceHandling()
            throws Exception
    {
        List<Connection> connections = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Connection connection = createConnection();
            connections.add(connection);

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT 123")) {
                assertTrue(rs.next());
            }
        }

        for (Connection connection : connections) {
            connection.close();
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".* does not exist")
    public void testBadQuery()
            throws Exception
    {
        try (Connection connection = createConnection("test", "tiny")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("SELECT * FROM bad_table")) {
                    fail("expected exception");
                }
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'user' is required")
    public void testUserIsRequired()
            throws Exception
    {
        try (Connection ignored = DriverManager.getConnection(format("jdbc:presto://%s", server.getAddress()))) {
            fail("expected exception");
        }
    }

    @Test
    public void testSetRole()
            throws Exception
    {
        try (PrestoConnection connection = createConnection(TEST_CATALOG, "tiny").unwrap(PrestoConnection.class)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET ROLE ALL");
            }
            assertEquals(connection.getRoles(), ImmutableMap.of(TEST_CATALOG, new SelectedRole(SelectedRole.Type.ALL, Optional.empty())));
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET ROLE NONE");
            }
            assertEquals(connection.getRoles(), ImmutableMap.of(TEST_CATALOG, new SelectedRole(SelectedRole.Type.NONE, Optional.empty())));
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("SET ROLE bar");
            }
            assertEquals(connection.getRoles(), ImmutableMap.of(TEST_CATALOG, new SelectedRole(SelectedRole.Type.ROLE, Optional.of("bar"))));
        }
    }

    // Disabled due to https://github.com/prestodb/presto/issues/16080
    @Test(enabled = false, timeOut = 10000)
    public void testQueryCancelByInterrupt()
            throws Exception
    {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<String> queryId = new AtomicReference<>();
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        Future<?> queryFuture = executorService.submit(() -> {
            try (Connection connection = createConnection("blackhole", "blackhole");
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM slow_test_table")) {
                queryId.set(resultSet.unwrap(PrestoResultSet.class).getQueryId());
                queryStarted.countDown();
                try {
                    resultSet.next();
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            }
            return null;
        });

        // start query and make sure it is not finished
        assertTrue(queryStarted.await(10, SECONDS));
        assertNotNull(queryId.get());
        assertFalse(getQueryState(queryId.get()).isDone());

        // interrupt JDBC thread that is waiting for query results
        queryFuture.cancel(true);

        // make sure the query was aborted
        assertTrue(queryFinished.await(10, SECONDS));
        assertThat(queryFailure.get())
                .isInstanceOf(SQLException.class)
                .hasMessage("ResultSet thread was interrupted");
        assertEquals(getQueryState(queryId.get()), FAILED);
    }

    @Test(timeOut = 10000)
    public void testQueryCancelExplicit()
            throws Exception
    {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<String> queryId = new AtomicReference<>();
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            // execute the slow query on another thread
            executorService.execute(() -> {
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM slow_test_table")) {
                    queryId.set(resultSet.unwrap(PrestoResultSet.class).getQueryId());
                    queryStarted.countDown();
                    resultSet.next();
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            });

            // start query and make sure it is not finished
            queryStarted.await(10, SECONDS);
            assertNotNull(queryId.get());
            assertFalse(getQueryState(queryId.get()).isDone());

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertNotNull(queryFailure.get());
            assertEquals(getQueryState(queryId.get()), FAILED);
        }
    }

    @Test(timeOut = 10000)
    public void testUpdateCancelExplicit()
            throws Exception
    {
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<String> queryId = new AtomicReference<>();
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();
        String queryUuid = "/* " + UUID.randomUUID().toString() + " */";

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            // execute the slow update on another thread
            executorService.execute(() -> {
                try {
                    statement.executeUpdate("CREATE TABLE test_cancel_create AS SELECT * FROM slow_test_table " + queryUuid);
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            });

            // start query and make sure it is not finished
            while (true) {
                Optional<QueryState> state = findQueryState(queryUuid);
                if (state.isPresent()) {
                    assertFalse(state.get().isDone());
                    break;
                }
                MILLISECONDS.sleep(50);
            }

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertNotNull(queryFailure.get());
            assertEquals(findQueryState(queryUuid), Optional.of(FAILED));
        }
    }

    @Test(timeOut = 10000)
    public void testQueryTimeout()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE test_query_timeout (key BIGINT) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")");
        }

        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        executorService.submit(() -> {
            try (Connection connection = createConnection("blackhole", "blackhole");
                    Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(1);
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_query_timeout")) {
                    try {
                        resultSet.next();
                    }
                    catch (SQLException t) {
                        queryFailure.set(t);
                    }
                    finally {
                        queryFinished.countDown();
                    }
                }
            }
            return null;
        });

        // make sure the query timed out
        queryFinished.await();
        assertNotNull(queryFailure.get());
        assertContains(queryFailure.get().getMessage(), "Query exceeded maximum time limit of 1.00s");

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE test_query_timeout");
        }
    }

    @Test(timeOut = 10000)
    public void testQueryPartialCancel()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM slow_test_table")) {
            statement.unwrap(PrestoStatement.class).partialCancel();
            assertTrue(resultSet.next());
            assertEquals(resultSet.getLong(1), 0);
        }
    }

    @Test(timeOut = 10000)
    public void testUpdatePartialCancel()
            throws Exception
    {
        CountDownLatch queryRunning = new CountDownLatch(1);

        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            // execute the slow update on another thread
            Future<Integer> future = executorService.submit(() ->
                    statement.executeUpdate("INSERT INTO test_table SELECT count(*) x FROM slow_test_table"));

            // wait for query to start running
            statement.unwrap(PrestoStatement.class).setProgressMonitor(stats -> {
                if (stats.getState().equals(RUNNING.toString())) {
                    queryRunning.countDown();
                }
            });
            queryRunning.await(10, SECONDS);

            // perform partial cancel from this test thread
            statement.unwrap(PrestoStatement.class).partialCancel();

            // make sure query completes
            assertEquals(future.get(10, SECONDS), (Integer) 1);
        }
    }

    @Test
    public void testEncodeDecodeSessionValue()
            throws Exception
    {
        boolean isValidSessionValue = false;
        String targetValue = "a+1==3";
        try (Connection connection = createConnection(TESTING_CATALOG)) {
            try (Statement statement = connection.createStatement()) {
                assertFalse(statement.execute(format("set session %s.connector_string='%s'", TESTING_CATALOG, targetValue)));
                assertNull(statement.getResultSet());

                assertTrue(statement.execute("show session"));
                ResultSet rs = statement.getResultSet();
                while (rs.next()) {
                    String sessionName = rs.getString("Name");
                    if (sessionName.equals(format("%s.connector_string", TESTING_CATALOG))) {
                        assertEquals(rs.getString("Value"), targetValue);
                        isValidSessionValue = true;
                        break;
                    }
                }
            }
        }
        assertTrue(isValidSessionValue);
    }

    private QueryState getQueryState(String queryId)
            throws SQLException
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE query_id = '%s'", queryId);
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Query was not found");
            return QueryState.valueOf(requireNonNull(resultSet.getString(1)));
        }
    }

    private Optional<QueryState> findQueryState(String text)
            throws SQLException
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE regexp_like(query, '%s$') /* */", Pattern.quote(text));
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                return Optional.empty();
            }
            QueryState state = QueryState.valueOf(requireNonNull(resultSet.getString(1)));
            assertFalse(resultSet.next(), "Found multiple queries");
            return Optional.of(state);
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s", server.getAddress(), catalog);
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    private static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        return rows.build();
    }

    @SafeVarargs
    private static <T> List<T> list(T... elements)
    {
        return asList(elements);
    }

    @SafeVarargs
    private static <T> T[] array(T... elements)
    {
        return elements;
    }

    static void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }
}
