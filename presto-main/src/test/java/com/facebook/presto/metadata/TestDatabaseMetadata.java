package com.facebook.presto.metadata;

import com.facebook.presto.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

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
        metadata = new DatabaseMetadata(dbi);
    }

    @AfterMethod
    public void cleanupDatabase()
    {
        dummyHandle.close();
    }

    @Test
    public void testCreateTable()
    {
        metadata.createTable(getOrdersTable());

        TableMetadata table = metadata.getTable("default", "default", "orders");
        assertEquals(table, getOrdersTable());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*already defined")
    public void testAlreadyExists()
    {
        metadata.createTable(getOrdersTable());
        metadata.createTable(getOrdersTable());
    }

    private static TableMetadata getOrdersTable()
    {
        return new TableMetadata("default", "default", "ORDERS", ImmutableList.of(
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "orderkey"),
                new ColumnMetadata(TupleInfo.Type.FIXED_INT_64, "custkey"),
                new ColumnMetadata(TupleInfo.Type.DOUBLE, "totalprice"),
                new ColumnMetadata(TupleInfo.Type.VARIABLE_BINARY, "orderdate")));
    }
}
