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
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.TestingDatabase.CONNECTOR_ID;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestJdbcMetadata
{
    private TestingDatabase database;
    private JdbcMetadata metadata;
    private JdbcMetadataCache jdbcMetadataCache;
    private JdbcTableHandle tableHandle;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        jdbcMetadataCache = new JdbcMetadataCache(executor, database.getJdbcClient(), new JdbcMetadataCacheStats(), OptionalLong.of(0), OptionalLong.of(0), 100);
        metadata = new JdbcMetadata(jdbcMetadataCache, database.getJdbcClient(), false);
        tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("EXAMPLE", "NUMBERS"));
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
        assertTrue(metadata.listSchemaNames(SESSION).containsAll(ImmutableSet.of("EXAMPLE", "TPCH")));
    }

    @Test
    public void testGetTableHandle()
    {
        JdbcTableHandle tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("EXAMPLE", "NUMBERS"));
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("EXAMPLE", "NUMBERS")), tableHandle);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("EXAMPLE", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, tableHandle), ImmutableMap.of(
                "TEXT", new JdbcColumnHandle(CONNECTOR_ID, "TEXT", JDBC_VARCHAR, VARCHAR, true, Optional.empty()),
                "TEXT_SHORT", new JdbcColumnHandle(CONNECTOR_ID, "TEXT_SHORT", JDBC_VARCHAR, createVarcharType(32), true, Optional.empty()),
                "VALUE", new JdbcColumnHandle(CONNECTOR_ID, "VALUE", JDBC_BIGINT, BIGINT, true, Optional.empty())));

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
        assertEquals(tableMetadata.getTable(), new SchemaTableName("EXAMPLE", "NUMBERS"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("TEXT", VARCHAR, false, null, null, false, emptyMap()), // primary key is not null in H2
                new ColumnMetadata("TEXT_SHORT", createVarcharType(32)),
                new ColumnMetadata("VALUE", BIGINT)));

        // escaping name patterns
        JdbcTableHandle specialTableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("EXA_PLE", "NUM_ERS"));
        ConnectorTableMetadata specialTableMetadata = metadata.getTableMetadata(SESSION, specialTableHandle);
        assertEquals(specialTableMetadata.getTable(), new SchemaTableName("EXA_PLE", "NUM_ERS"));
        assertEquals(specialTableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("TE_T", VARCHAR, false, null, null, false, emptyMap()), // primary key is not null in H2
                new ColumnMetadata("VA%UE", BIGINT)));

        // unknown tables should produce null
        unknownTableMetadata(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("u", "numbers"), null, "unknown", "unknown"));
        unknownTableMetadata(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("example", "numbers"), null, "example", "unknown"));
        unknownTableMetadata(new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("example", "numbers"), null, "unknown", "numbers"));
    }

    @Test
    public void testListTableColumns()
    {
        SchemaTableName tpchOrders = new SchemaTableName("TPCH", "ORDERS");
        ImmutableList<ColumnMetadata> tpchOrdersColumnMetadata = ImmutableList.of(
                ColumnMetadata.builder().setName("ORDERKEY").setType(BIGINT).setNullable(false).build(),
                ColumnMetadata.builder().setName("CUSTKEY").setType(BIGINT).setNullable(true).build());

        SchemaTableName tpchLineItem = new SchemaTableName("TPCH", "LINEITEM");
        ImmutableList<ColumnMetadata> tpchLineItemColumnMetadata = ImmutableList.of(
                ColumnMetadata.builder().setName("ORDERKEY").setType(BIGINT).setNullable(false).build(),
                ColumnMetadata.builder().setName("PARTKEY").setType(BIGINT).setNullable(true).build());

        //List columns for a given schema and table
        Map<SchemaTableName, List<ColumnMetadata>> tpchOrdersColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("TPCH", "ORDERS"));
        assertThat(tpchOrdersColumns)
                .containsOnly(
                        entry(tpchOrders, tpchOrdersColumnMetadata));

        //List columns for a given schema
        Map<SchemaTableName, List<ColumnMetadata>> tpchColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("TPCH"));
        assertThat(tpchColumns)
                .containsOnly(
                        entry(tpchOrders, tpchOrdersColumnMetadata),
                        entry(tpchLineItem, tpchLineItemColumnMetadata));
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
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("EXAMPLE", "NUMBERS"),
                new SchemaTableName("EXAMPLE", "VIEW_SOURCE"),
                new SchemaTableName("EXAMPLE", "VIEW"),
                new SchemaTableName("TPCH", "ORDERS"),
                new SchemaTableName("TPCH", "LINEITEM"),
                new SchemaTableName("EXA_PLE", "TABLE_WITH_FLOAT_COL"),
                new SchemaTableName("EXA_PLE", "NUM_ERS")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("EXAMPLE"))), ImmutableSet.of(
                new SchemaTableName("EXAMPLE", "NUMBERS"),
                new SchemaTableName("EXAMPLE", "VIEW_SOURCE"),
                new SchemaTableName("EXAMPLE", "VIEW")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("TPCH"))), ImmutableSet.of(
                new SchemaTableName("TPCH", "ORDERS"),
                new SchemaTableName("TPCH", "LINEITEM")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("EXA_PLE"))), ImmutableSet.of(
                new SchemaTableName("EXA_PLE", "NUM_ERS"),
                new SchemaTableName("EXA_PLE", "TABLE_WITH_FLOAT_COL")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(
                metadata.getColumnMetadata(SESSION, tableHandle, new JdbcColumnHandle(CONNECTOR_ID, "text", JDBC_VARCHAR, VARCHAR, true, Optional.empty())),
                new ColumnMetadata("text", VARCHAR));
    }

    @Test
    public void testCreateAndAlterTable()
    {
        SchemaTableName table = new SchemaTableName("EXAMPLE", "FOO");
        metadata.createTable(SESSION, new ConnectorTableMetadata(table, ImmutableList.of(new ColumnMetadata("TEXT", VARCHAR))), false);

        JdbcTableHandle handle = metadata.getTableHandle(SESSION, table);

        ConnectorTableMetadata layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getTable(), table);
        assertEquals(layout.getColumns().size(), 1);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("TEXT", VARCHAR));

        metadata.addColumn(SESSION, handle, new ColumnMetadata("X", VARCHAR));
        layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getColumns().size(), 2);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("TEXT", VARCHAR));
        assertEquals(layout.getColumns().get(1), new ColumnMetadata("X", VARCHAR));

        JdbcColumnHandle columnHandle = new JdbcColumnHandle(CONNECTOR_ID, "X", JDBC_VARCHAR, VARCHAR, true, Optional.empty());
        metadata.dropColumn(SESSION, handle, columnHandle);
        layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getColumns().size(), 1);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("TEXT", VARCHAR));

        SchemaTableName newTableName = new SchemaTableName("EXAMPLE", "BAR");
        metadata.renameTable(SESSION, handle, newTableName);
        handle = metadata.getTableHandle(SESSION, newTableName);
        layout = metadata.getTableMetadata(SESSION, handle);
        assertEquals(layout.getTable(), newTableName);
        assertEquals(layout.getColumns().size(), 1);
        assertEquals(layout.getColumns().get(0), new ColumnMetadata("TEXT", VARCHAR));
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

        metadata = new JdbcMetadata(jdbcMetadataCache, database.getJdbcClient(), true);
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
