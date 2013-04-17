package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNativeMetadata
{
    private static final QualifiedTableName DEFAULT_TEST_ORDERS = new QualifiedTableName("default", "test", "orders");

    private Handle dummyHandle;
    private Metadata metadata;

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
        assertNull(metadata.getTable(DEFAULT_TEST_ORDERS));

        metadata.createTable(getOrdersTable());

        TableMetadata table = metadata.getTable(DEFAULT_TEST_ORDERS);
        assertTableEqual(table, getOrdersTable());

        TableHandle tableHandle = table.getTableHandle().get();
        assertInstanceOf(tableHandle, NativeTableHandle.class);
        assertEquals(((NativeTableHandle) tableHandle).getTableId(), 1);

        ColumnHandle columnHandle = table.getColumns().get(0).getColumnHandle().get();
        assertInstanceOf(columnHandle, NativeColumnHandle.class);
        assertEquals(((NativeColumnHandle) columnHandle).getColumnId(), 1);
    }

    @Test
    public void testListTables()
    {
        metadata.createTable(getOrdersTable());
        List<QualifiedTableName> tables = metadata.listTables(QualifiedTablePrefix.builder("default").build());
        assertEquals(tables, ImmutableList.of(DEFAULT_TEST_ORDERS));
    }

    @Test
    public void testListTableColumns()
    {
        metadata.createTable(getOrdersTable());
        List<TableColumn> columns = metadata.listTableColumns(QualifiedTablePrefix.builder("default").build());
        assertEquals(columns, ImmutableList.<TableColumn>builder()
                .add(new TableColumn(DEFAULT_TEST_ORDERS, "orderkey", 1, FIXED_INT_64))
                .add(new TableColumn(DEFAULT_TEST_ORDERS, "custkey", 2, FIXED_INT_64))
                .add(new TableColumn(DEFAULT_TEST_ORDERS, "totalprice", 3, DOUBLE))
                .add(new TableColumn(DEFAULT_TEST_ORDERS, "orderdate", 4, VARIABLE_BINARY))
                .build());
    }

    @Test
    public void testListTableColumnsFiltering()
    {
        metadata.createTable(getOrdersTable());
        List<TableColumn> filterCatalog = metadata.listTableColumns(QualifiedTablePrefix.builder("default").build());
        List<TableColumn> filterSchema = metadata.listTableColumns(QualifiedTablePrefix.builder("default").schemaName("test").build());
        List<TableColumn> filterTable = metadata.listTableColumns(QualifiedTablePrefix.builder("default").schemaName("test").tableName("orders").build());
        assertEquals(filterCatalog, filterSchema);
        assertEquals(filterCatalog, filterTable);
    }

    private static TableMetadata getOrdersTable()
    {
        return new TableMetadata(DEFAULT_TEST_ORDERS,
                ImmutableList.of(
                        new ColumnMetadata("orderkey", FIXED_INT_64),
                        new ColumnMetadata("custkey", FIXED_INT_64),
                        new ColumnMetadata("totalprice", DOUBLE),
                        new ColumnMetadata("orderdate", VARIABLE_BINARY)));
    }

    private static void assertTableEqual(TableMetadata actual, TableMetadata expected)
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
