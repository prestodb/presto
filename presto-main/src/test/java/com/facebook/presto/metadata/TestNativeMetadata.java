package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.spi.ColumnType.DOUBLE;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNativeMetadata
{
    private static final SchemaTableName DEFAULT_TEST_ORDERS = new SchemaTableName("test", "orders");

    private Handle dummyHandle;
    private ConnectorMetadata metadata;

    @BeforeMethod
    public void setupDatabase()
            throws Exception
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        metadata = new NativeMetadata(dbi);
    }

    @AfterMethod
    public void cleanupDatabase()
    {
        dummyHandle.close();
    }

    @Test
    public void testCreateTable()
    {
        assertNull(metadata.getTableHandle(DEFAULT_TEST_ORDERS));

        TableHandle tableHandle = metadata.createTable(getOrdersTable());
        assertInstanceOf(tableHandle, NativeTableHandle.class);
        assertEquals(((NativeTableHandle) tableHandle).getTableId(), 1);

        SchemaTableMetadata table = metadata.getTableMetadata(tableHandle);
        assertTableEqual(table, getOrdersTable());

        ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, "orderkey");
        assertInstanceOf(columnHandle, NativeColumnHandle.class);
        assertEquals(((NativeColumnHandle) columnHandle).getColumnId(), 1);
    }

    @Test
    public void testListTables()
    {
        metadata.createTable(getOrdersTable());
        List<SchemaTableName> tables = metadata.listTables(Optional.<String>absent());
        assertEquals(tables, ImmutableList.of(DEFAULT_TEST_ORDERS));
    }

    @Test
    public void testListTableColumns()
    {
        metadata.createTable(getOrdersTable());
        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(new SchemaTablePrefix());
        assertEquals(columns, ImmutableMap.of(DEFAULT_TEST_ORDERS, columnsBuilder()
                .column("orderkey", LONG)
                .column("custkey", LONG)
                .column("totalprice", DOUBLE)
                .column("orderdate", STRING)
                .build()));
    }

    @Test
    public void testListTableColumnsFiltering()
    {
        metadata.createTable(getOrdersTable());
        Map<SchemaTableName, List<ColumnMetadata>> filterCatalog = metadata.listTableColumns(new SchemaTablePrefix());
        Map<SchemaTableName, List<ColumnMetadata>> filterSchema = metadata.listTableColumns(new SchemaTablePrefix("test"));
        Map<SchemaTableName, List<ColumnMetadata>> filterTable = metadata.listTableColumns(new SchemaTablePrefix("test", "orders"));
        assertEquals(filterCatalog, filterSchema);
        assertEquals(filterCatalog, filterTable);
    }

    private static SchemaTableMetadata getOrdersTable()
    {
        return new SchemaTableMetadata(DEFAULT_TEST_ORDERS, columnsBuilder()
                .column("orderkey", LONG)
                .column("custkey", LONG)
                .column("totalprice", DOUBLE)
                .column("orderdate", STRING)
                .build());
    }

    private static void assertTableEqual(SchemaTableMetadata actual, SchemaTableMetadata expected)
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
