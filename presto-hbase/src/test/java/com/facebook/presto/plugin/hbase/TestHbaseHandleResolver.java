package com.facebook.presto.plugin.hbase;


import com.facebook.presto.hbase.HbaseHandleResolver;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.hbase.model.HbaseTableLayoutHandle;
import com.facebook.presto.hbase.model.HbaseTransactionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.assertEquals;

public class TestHbaseHandleResolver {

    private HbaseHandleResolver resolver;

    @BeforeMethod
    public void setUp() {
        resolver = new HbaseHandleResolver();
    }

    @Test
    public void getTableLayoutHandleClass_ReturnsHbaseTableLayoutHandleClass() {
        assertEquals(resolver.getTableLayoutHandleClass(), HbaseTableLayoutHandle.class);
    }

    @Test
    public void getTableHandleClass_ReturnsHbaseTableHandleClass() {
        assertEquals(resolver.getTableHandleClass(), HbaseTableHandle.class);
    }

    @Test
    public void getInsertTableHandleClass_ReturnsHbaseTableHandleClass() {
        assertEquals(resolver.getInsertTableHandleClass(), HbaseTableHandle.class);
    }

    @Test
    public void getOutputTableHandleClass_ReturnsHbaseTableHandleClass() {
        assertEquals(resolver.getOutputTableHandleClass(), HbaseTableHandle.class);
    }

    @Test
    public void getColumnHandleClass_ReturnsHbaseColumnHandleClass() {
        assertEquals(resolver.getColumnHandleClass(), HbaseColumnHandle.class);
    }

    @Test
    public void getSplitClass_ReturnsHbaseSplitClass() {
        assertEquals(resolver.getSplitClass(), HbaseSplit.class);
    }

    @Test
    public void getTransactionHandleClass_ReturnsHbaseTransactionHandleClass() {
        assertEquals(resolver.getTransactionHandleClass(), HbaseTransactionHandle.class);
    }
}
