package com.facebook.presto.metadata;

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

        TableMetadata table = metadata.getTableMetadata(tableHandle);
        assertTableEqual(table, getOrdersTable());

        ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, "orderkey");
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
        Map<QualifiedTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(QualifiedTablePrefix.builder("default").build());
        assertEquals(columns, ImmutableMap.of(DEFAULT_TEST_ORDERS, columnsBuilder()
                .column("orderkey", FIXED_INT_64)
                .column("custkey", FIXED_INT_64)
                .column("totalprice", DOUBLE)
                .column("orderdate", VARIABLE_BINARY)
                .build()));
    }

    @Test
    public void testListTableColumnsFiltering()
    {
        metadata.createTable(getOrdersTable());
        Map<QualifiedTableName, List<ColumnMetadata>> filterCatalog = metadata.listTableColumns(QualifiedTablePrefix.builder("default").build());
        Map<QualifiedTableName, List<ColumnMetadata>> filterSchema = metadata.listTableColumns(QualifiedTablePrefix.builder("default").schemaName("test").build());
        Map<QualifiedTableName, List<ColumnMetadata>> filterTable = metadata.listTableColumns(QualifiedTablePrefix.builder("default").schemaName("test").tableName("orders").build());
        assertEquals(filterCatalog, filterSchema);
        assertEquals(filterCatalog, filterTable);
    }

    private static TableMetadata getOrdersTable()
    {
        return new TableMetadata(DEFAULT_TEST_ORDERS, columnsBuilder()
                .column("orderkey", FIXED_INT_64)
                .column("custkey", FIXED_INT_64)
                .column("totalprice", DOUBLE)
                .column("orderdate", VARIABLE_BINARY)
                .build());
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
