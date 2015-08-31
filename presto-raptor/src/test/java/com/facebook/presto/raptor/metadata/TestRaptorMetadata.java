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

import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorSessionProperties;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.BooleanMapper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.raptor.RaptorTableProperties.ORDERING_PROPERTY;
import static com.facebook.presto.raptor.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRaptorMetadata
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);
    private static final SchemaTableName DEFAULT_TEST_ORDERS = new SchemaTableName("test", "orders");
    private static final ConnectorSession SESSION = new TestingConnectorSession(
            new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties());

    private DBI dbi;
    private Handle dummyHandle;
    private ShardManager shardManager;
    private ConnectorMetadata metadata;

    @BeforeMethod
    public void setupDatabase()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        dummyHandle = dbi.open();
        shardManager = new DatabaseShardManager(dbi);
        metadata = new RaptorMetadata(new RaptorConnectorId("raptor"), dbi, shardManager, SHARD_INFO_CODEC, SHARD_DELTA_CODEC);
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
        ColumnHandle columnHandle = metadata.getColumnHandles(SESSION, tableHandle).get("orderkey");

        metadata.renameColumn(SESSION, raptorTableHandle, columnHandle, "orderkey_renamed");

        assertNull(metadata.getColumnHandles(SESSION, tableHandle).get("orderkey"));
        assertNotNull(metadata.getColumnHandles(SESSION, tableHandle).get("orderkey_renamed"));
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

        metadata.renameTable(SESSION, raptorTableHandle, renamedTable);
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

        ConnectorTableMetadata table = metadata.getTableMetadata(SESSION, tableHandle);
        assertTableEqual(table, getOrdersTable());

        ColumnHandle columnHandle = metadata.getColumnHandles(SESSION, tableHandle).get("orderkey");
        assertInstanceOf(columnHandle, RaptorColumnHandle.class);
        assertEquals(((RaptorColumnHandle) columnHandle).getColumnId(), 1);

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);
        assertNotNull(columnMetadata);
        assertEquals(columnMetadata.getName(), "orderkey");
        assertEquals(columnMetadata.getType(), BIGINT);
    }

    @Test
    public void testTableProperties()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(
                ORDERING_PROPERTY, ImmutableList.of("orderdate", "custkey"),
                TEMPORAL_COLUMN_PROPERTY, "orderdate"));
        metadata.createTable(SESSION, ordersTable);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertEquals(raptorTableHandle.getTableId(), 1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertEquals(sortColumns.size(), 2);
        assertEquals(sortColumns, ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderdate", DATE, 4),
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2)));

        // verify temporal column
        assertEquals(metadataDao.getTemporalColumnId(tableId), Long.valueOf(4));
        metadata.dropTable(SESSION, tableHandle);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Ordering column .* does not exist")
    public void testInvalidOrderingColumns()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(ORDERING_PROPERTY, ImmutableList.of("orderdatefoo")));
        metadata.createTable(SESSION, ordersTable);
        fail("Expected createTable to fail");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Temporal column .* does not exist")
    public void testInvalidTemporalColumn()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "foo"));
        metadata.createTable(SESSION, ordersTable);
        fail("Expected createTable to fail");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Temporal column must be of type timestamp or date: orderkey")
    public void testInvalidTemporalColumnType()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));
        metadata.createTable(SESSION, getOrdersTable(ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "orderkey")));
    }

    @Test
    public void testSortOrderProperty()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(ORDERING_PROPERTY, ImmutableList.of("orderdate", "custkey")));
        metadata.createTable(SESSION, ordersTable);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertEquals(raptorTableHandle.getTableId(), 1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertEquals(sortColumns.size(), 2);
        assertEquals(sortColumns, ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderdate", DATE, 4),
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2)));

        // verify temporal column is not set
        assertEquals(metadataDao.getTemporalColumnId(tableId), null);
        metadata.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testTemporalColumn()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS));

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "orderdate"));
        metadata.createTable(SESSION, ordersTable);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertEquals(raptorTableHandle.getTableId(), 1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns are not set
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertEquals(sortColumns.size(), 0);
        assertEquals(sortColumns, ImmutableList.of());

        // verify temporal column is set
        assertEquals(metadataDao.getTemporalColumnId(tableId), Long.valueOf(4));
        metadata.dropTable(SESSION, tableHandle);
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
        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, new SchemaTablePrefix("test"));
        assertEquals(views.keySet(), ImmutableSet.of(test1, test2));
        assertEquals(views.get(test1).getViewData(), "test1");
        assertEquals(views.get(test2).getViewData(), "test2");

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

        assertEquals(metadata.getViews(SESSION, test.toSchemaTablePrefix()).get(test).getViewData(), "bbb");
    }

    @Test
    public void testTransactionSelect()
            throws Exception
    {
        metadata.createTable(SESSION, getOrdersTable());

        // reads do not create a transaction
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        assertFalse(((RaptorTableHandle) tableHandle).getTransactionId().isPresent());
    }

    @Test
    public void testTransactionTableWrite()
            throws Exception
    {
        // start table creation
        long transactionId = 1;
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, getOrdersTable());

        // transaction is in progress
        assertTrue(transactionExists(transactionId));
        assertNull(transactionSuccessful(transactionId));

        // commit table creation
        metadata.commitCreateTable(SESSION, outputHandle, ImmutableList.of());
        assertTrue(transactionExists(transactionId));
        assertTrue(transactionSuccessful(transactionId));
    }

    @Test
    public void testTransactionInsert()
            throws Exception
    {
        // creating a table allocates a transaction
        long transactionId = 1;
        metadata.createTable(SESSION, getOrdersTable());
        assertTrue(transactionSuccessful(transactionId));

        // start insert
        transactionId++;
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        ConnectorInsertTableHandle insertHandle = metadata.beginInsert(SESSION, tableHandle);

        // transaction is in progress
        assertTrue(transactionExists(transactionId));
        assertNull(transactionSuccessful(transactionId));

        // commit insert
        metadata.commitInsert(SESSION, insertHandle, ImmutableList.<Slice>of());
        assertTrue(transactionExists(transactionId));
        assertTrue(transactionSuccessful(transactionId));
    }

    @Test
    public void testTransactionDelete()
            throws Exception
    {
        // creating a table allocates a transaction
        long transactionId = 1;
        metadata.createTable(SESSION, getOrdersTable());
        assertTrue(transactionSuccessful(transactionId));

        // start delete
        transactionId++;
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        tableHandle = metadata.beginDelete(SESSION, tableHandle);

        // verify transaction is assigned for deletion handle
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertEquals(raptorTableHandle.getTableId(), 1);
        assertEquals(raptorTableHandle.getTransactionId(), OptionalLong.of(transactionId));

        // transaction is in progress
        assertTrue(transactionExists(transactionId));
        assertNull(transactionSuccessful(transactionId));

        // rollback delete
        metadata.rollbackDelete(SESSION, tableHandle);
        assertTrue(transactionExists(transactionId));
        assertFalse(transactionSuccessful(transactionId));

        // start another delete
        transactionId++;
        tableHandle = metadata.beginDelete(SESSION, tableHandle);

        // transaction is in progress
        assertTrue(transactionExists(transactionId));
        assertNull(transactionSuccessful(transactionId));

        // commit delete
        metadata.commitDelete(SESSION, tableHandle, ImmutableList.of());
        assertTrue(transactionExists(transactionId));
        assertTrue(transactionSuccessful(transactionId));
    }

    @Test
    public void testTransactionAbort()
            throws Exception
    {
        // start table creation
        long transactionId = 1;
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, getOrdersTable());

        // transaction is in progress
        assertTrue(transactionExists(transactionId));
        assertNull(transactionSuccessful(transactionId));

        // force transaction to abort
        shardManager.rollbackTransaction(transactionId);
        assertTrue(transactionExists(transactionId));
        assertFalse(transactionSuccessful(transactionId));

        // commit table creation
        try {
            metadata.commitCreateTable(SESSION, outputHandle, ImmutableList.of());
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
    }

    private boolean transactionExists(long transactionId)
    {
        try (Handle handle = dbi.open()) {
            return handle
                    .createQuery("SELECT count(*) FROM transactions WHERE transaction_id = ?")
                    .bind(0, transactionId)
                    .map(BooleanMapper.FIRST)
                    .first();
        }
    }

    private Boolean transactionSuccessful(long transactionId)
    {
        try (Handle handle = dbi.open()) {
            return (Boolean) handle
                    .createQuery("SELECT successful FROM transactions WHERE transaction_id = ?")
                    .bind(0, transactionId)
                    .first()
                    .get("successful");
        }
    }

    private static ConnectorTableMetadata getOrdersTable()
    {
        return getOrdersTable(ImmutableMap.of());
    }

    private static ConnectorTableMetadata getOrdersTable(Map<String, Object> properties)
    {
        MetadataUtil.TableMetadataBuilder builder = tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                .column("orderkey", BIGINT)
                .column("custkey", BIGINT)
                .column("totalprice", DOUBLE)
                .column("orderdate", DATE);

        if (!properties.isEmpty()) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                builder.property(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
    }

    private static void assertTableEqual(ConnectorTableMetadata actual, ConnectorTableMetadata expected)
    {
        assertEquals(actual.getTable(), expected.getTable());

        List<ColumnMetadata> actualColumns = actual.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(Collectors.toList());

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
