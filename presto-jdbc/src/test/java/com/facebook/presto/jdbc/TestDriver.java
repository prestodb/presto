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

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.server.testing.TestingPrestoServer.TEST_CATALOG;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDriver
{
    private static final DateTimeZone ASIA_ORAL_ZONE = DateTimeZone.forID("Asia/Oral");
    private static final GregorianCalendar ASIA_ORAL_CALENDAR = new GregorianCalendar(ASIA_ORAL_ZONE.toTimeZone());

    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("default", "tpch"); // TODO: change catalog name
    }

    @AfterClass
    public void teardown()
    {
        closeQuietly(server);
    }

    @Test
    public void testDriverManager()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT " +
                        "  123 _bigint" +
                        ", 'foo' _varchar" +
                        ", 0.1 _double" +
                        ", true _boolean" +
                        ", cast('hello' as varbinary) _varbinary" +
                        ", approx_set(42) _hll")) {
                    ResultSetMetaData metadata = rs.getMetaData();

                    assertEquals(metadata.getColumnCount(), 6);

                    assertEquals(metadata.getColumnLabel(1), "_bigint");
                    assertEquals(metadata.getColumnType(1), Types.BIGINT);

                    assertEquals(metadata.getColumnLabel(2), "_varchar");
                    assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);

                    assertEquals(metadata.getColumnLabel(3), "_double");
                    assertEquals(metadata.getColumnType(3), Types.DOUBLE);

                    assertEquals(metadata.getColumnLabel(4), "_boolean");
                    assertEquals(metadata.getColumnType(4), Types.BOOLEAN);

                    assertEquals(metadata.getColumnLabel(5), "_varbinary");
                    assertEquals(metadata.getColumnType(5), Types.LONGVARBINARY);

                    assertEquals(metadata.getColumnLabel(6), "_hll");
                    assertEquals(metadata.getColumnType(6), Types.JAVA_OBJECT);

                    assertTrue(rs.next());

                    assertEquals(rs.getObject(1), 123L);
                    assertEquals(rs.getObject("_bigint"), 123L);
                    assertEquals(rs.getLong(1), 123);
                    assertEquals(rs.getLong("_bigint"), 123);

                    assertEquals(rs.getObject(2), "foo");
                    assertEquals(rs.getObject("_varchar"), "foo");
                    assertEquals(rs.getString(2), "foo");
                    assertEquals(rs.getString("_varchar"), "foo");

                    assertEquals(rs.getObject(3), 0.1);
                    assertEquals(rs.getObject("_double"), 0.1);
                    assertEquals(rs.getDouble(3), 0.1);
                    assertEquals(rs.getDouble("_double"), 0.1);

                    assertEquals(rs.getObject(4), true);
                    assertEquals(rs.getObject("_boolean"), true);
                    assertEquals(rs.getBoolean(4), true);
                    assertEquals(rs.getBoolean("_boolean"), true);

                    assertEquals(rs.getObject(5), "hello".getBytes(UTF_8));
                    assertEquals(rs.getObject("_varbinary"), "hello".getBytes(UTF_8));
                    assertEquals(rs.getBytes(5), "hello".getBytes(UTF_8));
                    assertEquals(rs.getBytes("_varbinary"), "hello".getBytes(UTF_8));

                    assertInstanceOf(rs.getObject(6), byte[].class);
                    assertInstanceOf(rs.getObject("_hll"), byte[].class);
                    assertInstanceOf(rs.getBytes(6), byte[].class);
                    assertInstanceOf(rs.getBytes("_hll"), byte[].class);

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
                assertRowCount(rs, 1);
                ResultSetMetaData metadata = rs.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);
                assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
                assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);
            }
        }
    }

    @Test
    public void testGetSchemas()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                assertGetSchemasResult(rs, 11);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, null)) {
                assertGetSchemasResult(rs, 11);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, null)) {
                assertGetSchemasResult(rs, 11);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("", null)) {
                // all schemas in presto have a catalog name
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "sys")) {
                assertGetSchemasResult(rs, 1);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sys")) {
                assertGetSchemasResult(rs, 1);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "s_s")) {
                assertGetSchemasResult(rs, 1);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sf%")) {
                assertGetSchemasResult(rs, 8);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", null)) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", "sys")) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "unknown")) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "unknown")) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", "unknown")) {
                assertGetSchemasResult(rs, 0);
            }
        }
    }

    private static void assertGetSchemasResult(ResultSet rs, int expectedRows)
            throws SQLException
    {
        assertRowCount(rs, expectedRows);

        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 2);

        assertEquals(metadata.getColumnLabel(1), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_CATALOG");
        assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);
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
                assertTrue(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, null, null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
                assertTrue(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("", null, null, null)) {
                assertTableMetadata(rs);

                // all tables in presto have a catalog name
                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertEquals(rows.size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertEquals(rows.size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertTrue(rows.contains(getTablesRow("information_schema", "schemata")));
                assertTrue(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "inf%", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tab%", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("unknown", "information_schema", "tables", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        // todo why does Presto require that the schema name be lower case
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "unknown", "tables", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "unknown", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", new String[] {"unknown"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", new String[] {"unknown", "BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "information_schema", "tables", new String[] {})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(getTablesRow("information_schema", "tables")));
                assertFalse(rows.contains(getTablesRow("information_schema", "schemata")));
                assertFalse(rows.contains(getTablesRow("sys", "node")));
            }
        }
    }

    private static List<Object> getTablesRow(String schema, String table)
    {
        return ImmutableList.<Object>of(TEST_CATALOG, schema, table, "BASE TABLE", "", "", "", "", "", "");
    }

    private static void assertTableMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 10);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(4), "TABLE_TYPE");
        assertEquals(metadata.getColumnType(4), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(5), "REMARKS");
        assertEquals(metadata.getColumnType(5), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(6), "TYPE_CAT");
        assertEquals(metadata.getColumnType(6), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(7), "TYPE_SCHEM");
        assertEquals(metadata.getColumnType(7), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(8), "TYPE_NAME");
        assertEquals(metadata.getColumnType(8), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(9), "SELF_REFERENCING_COL_NAME");
        assertEquals(metadata.getColumnType(9), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(10), "REF_GENERATION");
        assertEquals(metadata.getColumnType(10), Types.LONGNVARCHAR);
    }

    @Test
    public void testGetTableTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                List<List<Object>> data = readRows(tableTypes);
                assertEquals(data.size(), 1);
                assertEquals(data.get(0).get(0), "BASE TABLE");

                ResultSetMetaData metadata = tableTypes.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);

                assertEquals(metadata.getColumnLabel(1), "TABLE_TYPE");
                assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);
            }
        }
    }

    @Test
    public void testGetColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, null, "tables", "column_name")) {
                assertColumnMetadata(rs);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, null, "tables", "column_name")) {
                assertColumnMetadata(rs);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "information_schema", "tables", "column_name")) {
                assertColumnMetadata(rs);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "column_name")) {
                assertColumnMetadata(rs);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "inf%", "tables", "column_name")) {
                assertColumnMetadata(rs);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tab%", "column_name")) {
                assertColumnMetadata(rs);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "information_schema", "tables", "col%")) {
                assertColumnMetadata(rs);
            }
        }
    }

    private static void assertColumnMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 24);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(4), "COLUMN_NAME");
        assertEquals(metadata.getColumnType(4), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(5), "DATA_TYPE");
        assertEquals(metadata.getColumnType(5), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(6), "TYPE_NAME");
        assertEquals(metadata.getColumnType(6), Types.LONGNVARCHAR);

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
        assertEquals(metadata.getColumnType(12), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(13), "COLUMN_DEF");
        assertEquals(metadata.getColumnType(13), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(14), "SQL_DATA_TYPE");
        assertEquals(metadata.getColumnType(14), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(15), "SQL_DATETIME_SUB");
        assertEquals(metadata.getColumnType(15), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(16), "CHAR_OCTET_LENGTH");
        assertEquals(metadata.getColumnType(16), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(17), "ORDINAL_POSITION");
        assertEquals(metadata.getColumnType(17), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(18), "IS_NULLABLE");
        assertEquals(metadata.getColumnType(18), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(19), "SCOPE_CATALOG");
        assertEquals(metadata.getColumnType(19), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(20), "SCOPE_SCHEMA");
        assertEquals(metadata.getColumnType(20), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(21), "SCOPE_TABLE");
        assertEquals(metadata.getColumnType(21), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(22), "SOURCE_DATA_TYPE");
        assertEquals(metadata.getColumnType(22), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(23), "IS_AUTOINCREMENT");
        assertEquals(metadata.getColumnType(23), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(24), "IS_GENERATEDCOLUMN");
        assertEquals(metadata.getColumnType(24), Types.LONGNVARCHAR);
    }

    @Test
    public void testExecute()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertEquals(rs.getLong("x"), 123);
                assertEquals(rs.getString(2), "foo");
                assertEquals(rs.getString("y"), "foo");
                assertFalse(rs.next());
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
        assertEquals(connection.getSchema(), TEST_CATALOG);

        connection = DriverManager.getConnection(prefix + "/a", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), TEST_CATALOG);

        connection = DriverManager.getConnection(prefix + "/", "test", null);
        assertEquals(connection.getCatalog(), TEST_CATALOG);
        assertEquals(connection.getSchema(), TEST_CATALOG);

        connection = DriverManager.getConnection(prefix, "test", null);
        assertEquals(connection.getCatalog(), TEST_CATALOG);
        assertEquals(connection.getSchema(), TEST_CATALOG);
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
                        "WHERE table_schema = 'sys' AND table_name = 'node'")) {
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
                        "WHERE table_schema = 'sys' AND table_name = 'node'")) {
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
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("SELECT * FROM bad_table")) {
                    fail("expected exception");
                }
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Username property \\(user\\) must be set")
    public void testUserIsRequired()
            throws Exception
    {
        try (Connection ignored = DriverManager.getConnection("jdbc:presto://test.invalid/")) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Invalid path segments in URL: .*")
    public void testBadUrlExtraPathSegments()
            throws Exception
    {
        String url = format("jdbc:presto://%s/hive/default/bad_string", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlMissingCatalog()
            throws Exception
    {
        String url = format("jdbc:presto://%s//default", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlEndsInSlashes()
            throws Exception
    {
        String url = format("jdbc:presto://%s//", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Schema name is empty: .*")
    public void testBadUrlMissingSchema()
            throws Exception
    {
        String url = format("jdbc:presto://%s/a//", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
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

    private static void assertRowCount(ResultSet rs, int expected)
            throws SQLException
    {
        List<List<Object>> data = readRows(rs);
        assertEquals(data.size(), expected);
    }

    private static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            ImmutableList.Builder<Object> row = ImmutableList.builder();
            for (int i = 0; i < columnCount; i++) {
                row.add(rs.getObject(i + 1));
            }
            rows.add(row.build());
        }
        return rows.build();
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
