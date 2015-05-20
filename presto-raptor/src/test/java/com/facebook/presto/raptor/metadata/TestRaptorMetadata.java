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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRaptorMetadata
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);
    private static final SchemaTableName DEFAULT_TEST_ORDERS = new SchemaTableName("test", "orders");

    private Handle dummyHandle;
    private ConnectorMetadata metadata;

    @BeforeMethod
    public void setupDatabase()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        dummyHandle = dbi.open();
        metadata = new RaptorMetadata(new RaptorConnectorId("default"), dbi, new DatabaseShardManager(dbi), SHARD_INFO_CODEC);
    }

    @AfterMethod
    public void cleanupDatabase()
    {
        dummyHandle.close();
    }

    @Test
    public void testRenameColumn()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));
        metadata.createTable(SESSION, getOrdersTable());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        ColumnHandle columnHandle = metadata.getColumnHandles(tableHandle).get("orderkey");

        metadata.renameColumn(raptorTableHandle, columnHandle, "orderkey_renamed");

        assertNull(metadata.getColumnHandles(tableHandle).get("orderkey"));
        assertNotNull(metadata.getColumnHandles(tableHandle).get("orderkey_renamed"));
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));
        metadata.createTable(SESSION, getOrdersTable());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        SchemaTableName renamedTable = new SchemaTableName(raptorTableHandle.getSchemaName(), "orders_renamed");

        metadata.renameTable(raptorTableHandle, renamedTable);
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));
        ConnectorTableHandle renamedTableHandle = metadata.getTableHandle(SESSION, renamedTable);
        assertNotNull(renamedTableHandle);
        assertEquals(((RaptorTableHandle) renamedTableHandle).getTableName(), renamedTable.getTableName());
    }

    @Test
    public void testCreateTable()
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));

        metadata.createTable(SESSION, getOrdersTable());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        assertEquals(((RaptorTableHandle) tableHandle).getTableId(), 1);

        ConnectorTableMetadata table = metadata.getTableMetadata(tableHandle);
        assertTableEqual(table, getOrdersTable());

        ColumnHandle columnHandle = metadata.getColumnHandles(tableHandle).get("orderkey");
        assertInstanceOf(columnHandle, RaptorColumnHandle.class);
        assertEquals(((RaptorColumnHandle) columnHandle).getColumnId(), 1);

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableHandle, columnHandle);
        assertNotNull(columnMetadata);
        assertEquals(columnMetadata.getName(), "orderkey");
        assertEquals(columnMetadata.getType(), BIGINT);
        assertEquals(columnMetadata.getOrdinalPosition(), 0);
    }

    @Test
    public void testListTables()
    {
        metadata.createTable(SESSION, getOrdersTable());
        List<SchemaTableName> tables = metadata.listTables(SESSION, null);
        assertEquals(tables, ImmutableList.of(DEFAULT_TEST_ORDERS));
    }

    @Test
    public void testListTableColumns()
    {
        metadata.createTable(SESSION, getOrdersTable());
        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix());
        assertEquals(columns, ImmutableMap.of(DEFAULT_TEST_ORDERS, getOrdersTable().getColumns()));
    }

    @Test
    public void testListTableColumnsFiltering()
    {
        metadata.createTable(SESSION, getOrdersTable());
        Map<SchemaTableName, List<ColumnMetadata>> filterCatalog = metadata.listTableColumns(SESSION, new SchemaTablePrefix());
        Map<SchemaTableName, List<ColumnMetadata>> filterSchema = metadata.listTableColumns(SESSION, new SchemaTablePrefix("test"));
        Map<SchemaTableName, List<ColumnMetadata>> filterTable = metadata.listTableColumns(SESSION, new SchemaTablePrefix("test", "orders"));
        assertEquals(filterCatalog, filterSchema);
        assertEquals(filterCatalog, filterTable);
    }

    @Test
    public void testViews()
    {
        SchemaTableName test1 = new SchemaTableName("test", "test_view1");
        SchemaTableName test2 = new SchemaTableName("test", "test_view2");

        // create views
        metadata.createView(SESSION, test1, "test1", false);
        metadata.createView(SESSION, test2, "test2", false);

        // verify listing
        List<SchemaTableName> list = metadata.listViews(SESSION, "test");
        assertEqualsIgnoreOrder(list, ImmutableList.of(test1, test2));

        // verify getting data
        Map<SchemaTableName, String> views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertEquals(views.keySet(), ImmutableSet.of(test1, test2));
        assertEquals(views.get(test1), "test1");
        assertEquals(views.get(test2), "test2");

        // drop first view
        metadata.dropView(SESSION, test1);

        views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertEquals(views.keySet(), ImmutableSet.of(test2));

        // drop second view
        metadata.dropView(SESSION, test2);

        views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertTrue(views.isEmpty());

        // verify listing everything
        views = metadata.getViews(SESSION, new SchemaTablePrefix());
        assertTrue(views.isEmpty());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "View already exists: test\\.test_view")
    public void testCreateViewWithoutReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");
        try {
            metadata.createView(SESSION, test, "test", false);
        }
        catch (Exception e) {
            fail("should have succeeded");
        }

        metadata.createView(SESSION, test, "test", false);
    }

    @Test
    public void testCreateViewWithReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");

        metadata.createView(SESSION, test, "aaa", true);
        metadata.createView(SESSION, test, "bbb", true);

        assertEquals(metadata.getViews(SESSION, test.toSchemaTablePrefix()).get(test), "bbb");
    }

    private static ConnectorTableMetadata getOrdersTable()
    {
        return tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                .column("orderkey", BIGINT)
                .column("custkey", BIGINT)
                .column("totalprice", DOUBLE)
                .column("orderdate", VARCHAR)
                .build();
    }

    private static void assertTableEqual(ConnectorTableMetadata actual, ConnectorTableMetadata expected)
    {
        assertEquals(actual.getTable(), expected.getTable());

        List<ColumnMetadata> actualColumns = actual.getColumns();
        List<ColumnMetadata> expectedColumns = expected.getColumns();
        assertEquals(actualColumns.size(), expectedColumns.size());
        for (int i = 0; i < actualColumns.size(); i++) {
            ColumnMetadata actualColumn = actualColumns.get(i);
            ColumnMetadata expectedColumn = expectedColumns.get(i);
            assertEquals(actualColumn.getName(), expectedColumn.getName());
            assertEquals(actualColumn.getType(), expectedColumn.getType());
        }
    }
}
