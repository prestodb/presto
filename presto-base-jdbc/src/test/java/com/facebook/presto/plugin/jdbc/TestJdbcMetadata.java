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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.TestingDatabase.CONNECTOR_ID;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestJdbcMetadata
{
    private TestingDatabase database;
    private JdbcMetadata metadata;
    private JdbcTableHandle tableHandle;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        metadata = new JdbcMetadata(database.getJdbcClient(), false);
        tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testListSchemaNames()
    {
        assertTrue(metadata.listSchemaNames(SESSION).containsAll(ImmutableSet.of("example", "tpch")));
    }

    @Test
    public void testGetTableHandle()
    {
        JdbcTableHandle tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers")), tableHandle);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, tableHandle), ImmutableMap.of(
                "text", new JdbcColumnHandle(CONNECTOR_ID, "TEXT", VARCHAR),
                "text_short", new JdbcColumnHandle(CONNECTOR_ID, "TEXT_SHORT", createVarcharType(32)),
                "value", new JdbcColumnHandle(CONNECTOR_ID, "VALUE", BIGINT)));

        // unknown table
        unknownTableColumnHandle(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("unknown", "unknown"), "unknown", "unknown", "unknown"));
        unknownTableColumnHandle(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("example", "numbers"), null, "example", "unknown"));
    }

    private void unknownTableColumnHandle(JdbcTableHandle tableHandle)
    {
        try {
            metadata.getColumnHandles(SESSION, tableHandle);
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException ignored) {
        }
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("example", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("text", VARCHAR),
                new ColumnMetadata("text_short", createVarcharType(32)),
                new ColumnMetadata("value", BIGINT)));

        // escaping name patterns
        JdbcTableHandle specialTableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("exa_ple", "num_ers"));
        ConnectorTableMetadata specialTableMetadata = metadata.getTableMetadata(SESSION, specialTableHandle);
        assertEquals(specialTableMetadata.getTable(), new SchemaTableName("exa_ple", "num_ers"));
        assertEquals(specialTableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("te_t", VARCHAR),
                new ColumnMetadata("va%ue", BIGINT)));

        // unknown tables should produce null
        unknownTableMetadata(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("u", "numbers"), null, "unknown", "unknown"));
        unknownTableMetadata(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("example", "numbers"), null, "example", "unknown"));
        unknownTableMetadata(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("example", "numbers"), null, "unknown", "numbers"));
    }

    private void unknownTableMetadata(JdbcTableHandle tableHandle)
    {
        try {
            metadata.getTableMetadata(SESSION, tableHandle);
            fail("Expected getTableMetadata of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException ignored) {
        }
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, null)), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem"),
                new SchemaTableName("exa_ple", "table_with_float_col"),
                new SchemaTableName("exa_ple", "num_ers")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "example")), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "tpch")), ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "exa_ple")), ImmutableSet.of(
                new SchemaTableName("exa_ple", "num_ers"),
                new SchemaTableName("exa_ple", "table_with_float_col")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, "unknown")), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(
                metadata.getColumnMetadata(SESSION, tableHandle, new JdbcColumnHandle(CONNECTOR_ID, "text", VARCHAR)),
                new ColumnMetadata("text", VARCHAR));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCreateTable()
    {
        metadata.createTable(SESSION, new ConnectorTableMetadata(
                new SchemaTableName("example", "foo"),
                ImmutableList.of(new ColumnMetadata("text", VARCHAR))));
    }

    @Test
    public void testDropTableTable()
    {
        try {
            metadata.dropTable(SESSION, tableHandle);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), PERMISSION_DENIED.toErrorCode());
        }

        metadata = new JdbcMetadata(database.getJdbcClient(), true);
        metadata.dropTable(SESSION, tableHandle);

        try {
            metadata.getTableMetadata(SESSION, tableHandle);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }
}
