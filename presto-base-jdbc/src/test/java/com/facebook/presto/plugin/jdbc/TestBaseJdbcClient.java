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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.h2.Driver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestBaseJdbcClient
{
    public static final String SCHEMA_BASE = "mem";
    private String schema;
    private Map<String, String> properties;
    private BaseJdbcClient client;

    @BeforeMethod
    public void setUp()
            throws SQLException
    {
        schema = SCHEMA_BASE + UUID.randomUUID().toString().replace("-", "");
        properties = TestingH2JdbcModule.createProperties();

        try (Connection connection = DriverManager.getConnection(properties.get("connection-url"));
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }

        client = getBaseJdbcClient(properties);
    }

    @AfterMethod
    public void tearDown()
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(properties.get("connection-url"));
                Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA " + schema);
        }
    }

    @Test
    public void testCreateWithDefaultAndNullable()
            throws SQLException
    {
        JdbcOutputTableHandle handle = null;
        try (Connection connection = DriverManager.getConnection(properties.get("connection-url"))) {
            handle = client.beginCreateTable(getConnectorTableMetadata(UUID.randomUUID().toString(),
                    new ColumnMetadata("columnA", VarcharType.VARCHAR, null, null, false, emptyMap()),
                    new ColumnMetadata("columnB", VarcharType.VARCHAR, null, null, false, emptyMap(), true, Slices.utf8Slice("a")),
                    new ColumnMetadata("columnC", VarcharType.VARCHAR, null, null, false, emptyMap(), false, null),
                    new ColumnMetadata("columnD", DateType.DATE, null, null, false, emptyMap(), false, 1L),
                    new ColumnMetadata("columnE", TimeType.TIME, null, null, false, emptyMap(), false, 1L),
                    new ColumnMetadata("columnF", TimestampType.TIMESTAMP, null, null, false, emptyMap(), false, 1L)));
            try (ResultSet rs = getColumnMetadata(handle, connection, "columnA")) {
                assertNull(rs.getString("COLUMN_DEF"), "Column default wasn't null");
                assertTrue(rs.getBoolean("NULLABLE"), "Expected nullable");
            }
            try (ResultSet rs = getColumnMetadata(handle, connection, "columnB")) {
                assertEquals(rs.getString("COLUMN_DEF"), "'a'", "Column default mismatch");
                assertTrue(rs.getBoolean("NULLABLE"), "Expected nullable");
            }
            try (ResultSet rs = getColumnMetadata(handle, connection, "columnC")) {
                assertNull(rs.getString("COLUMN_DEF"), "Column default wasn't null");
                assertFalse(rs.getBoolean("NULLABLE"), "Expected nullable");
            }
            try (ResultSet rs = getColumnMetadata(handle, connection, "columnD")) {
                assertEquals(rs.getString("COLUMN_DEF"), "'1970-01-02'", "Column default mismatch");
                assertFalse(rs.getBoolean("NULLABLE"), "Expected nullable");
            }
            try (ResultSet rs = getColumnMetadata(handle, connection, "columnE")) {
                assertEquals(rs.getString("COLUMN_DEF"), "'00:00:01'", "Column default mismatch");
                assertFalse(rs.getBoolean("NULLABLE"), "Expected nullable");
            }
            try (ResultSet rs = getColumnMetadata(handle, connection, "columnF")) {
                assertEquals(rs.getString("COLUMN_DEF"), "'1970-01-01 00:00:00.001'", "Column default mismatch");
                assertFalse(rs.getBoolean("NULLABLE"), "Expected nullable");
            }
        }
        finally {
            if (handle != null) {
                client.rollbackCreateTable(handle);
            }
        }
    }

    private ResultSet getColumnMetadata(JdbcOutputTableHandle handle, Connection connection, String columnName)
            throws SQLException
    {
        ResultSet rs = connection.getMetaData().getColumns(
                connection.getCatalog(),
                handle.getSchemaName().toUpperCase(ENGLISH),
                // Hack: H2 seems to force table names to lower case, but other
                // identifiers like the schema and columns to upper case
                handle.getTemporaryTableName().toLowerCase(ENGLISH),
                columnName.toUpperCase(ENGLISH));
        assertTrue(rs.next(), format("Column %s not found in table %s", columnName, handle.getTableName()));
        return rs;
    }

    private BaseJdbcClient getBaseJdbcClient(Map<String, String> properties)
    {
        BaseJdbcConfig config = new BaseJdbcConfig()
                .setConnectionUrl(properties.get("connection-url"));
        return new BaseJdbcClient(new JdbcConnectorId("123"), config, "\"",
                new DriverConnectionFactory(new Driver(), config));
    }

    private ConnectorTableMetadata getConnectorTableMetadata(String inputTableName, ColumnMetadata... columns)
    {
        SchemaTableName tableName = new SchemaTableName(schema, inputTableName);
        return new ConnectorTableMetadata(tableName, ImmutableList.copyOf(columns));
    }
}
