package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestDatabaseMetadata
{
    private Handle dummyHandle;
    private Metadata metadata;

    @BeforeMethod
    public void setupDatabase()
            throws IOException
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
        assertNull(metadata.getTable("default", "default", "orders"));

        metadata.createTable(getOrdersTable());

        TableMetadata table = metadata.getTable("default", "default", "orders");
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
        List<QualifiedTableName> tables = metadata.listTables("default");
        assertEquals(tables, ImmutableList.of(new QualifiedTableName("default", "default", "orders")));
    }

    private static TableMetadata getOrdersTable()
    {
        return new TableMetadata("default", "default", "ORDERS", ImmutableList.of(
                new ColumnMetadata("orderkey", TupleInfo.Type.FIXED_INT_64),
                new ColumnMetadata("custkey", TupleInfo.Type.FIXED_INT_64),
                new ColumnMetadata("totalprice", TupleInfo.Type.DOUBLE),
                new ColumnMetadata("orderdate", TupleInfo.Type.VARIABLE_BINARY)));
    }

    private static void assertTableEqual(TableMetadata actual, TableMetadata expected)
    {
        assertEquals(actual.getCatalogName(), expected.getCatalogName());
        assertEquals(actual.getSchemaName(), expected.getSchemaName());
        assertEquals(actual.getTableName(), expected.getTableName());

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
