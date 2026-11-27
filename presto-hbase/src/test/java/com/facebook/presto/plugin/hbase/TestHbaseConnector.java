package com.facebook.presto.plugin.hbase;


import com.facebook.presto.hbase.HbaseConnector;
import com.facebook.presto.hbase.HbaseMetadata;
import com.facebook.presto.hbase.HbaseSplitManager;
import com.facebook.presto.hbase.conf.HbaseSessionProperties;
import com.facebook.presto.hbase.conf.HbaseTableProperties;
import com.facebook.presto.hbase.io.HbasePageSinkProvider;
import com.facebook.presto.hbase.io.HbaseRecordSetProvider;
import com.facebook.presto.hbase.model.HbaseTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static org.mockito.Mockito.*;
import static org.testng.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHbaseConnector {

    private LifeCycleManager lifeCycleManager;
    private HbaseMetadata metadata;
    private HbaseSplitManager splitManager;
    private HbaseRecordSetProvider recordSetProvider;
    private HbasePageSinkProvider pageSinkProvider;
    private HbaseSessionProperties sessionProperties;
    private HbaseTableProperties tableProperties;
    private HbaseConnector connector;

    @BeforeMethod
    public void setUp() {
        lifeCycleManager = mock(LifeCycleManager.class);
        metadata = mock(HbaseMetadata.class);
        splitManager = mock(HbaseSplitManager.class);
        recordSetProvider = mock(HbaseRecordSetProvider.class);
        pageSinkProvider = mock(HbasePageSinkProvider.class);
        sessionProperties = mock(HbaseSessionProperties.class);
        tableProperties = mock(HbaseTableProperties.class);

        connector = new HbaseConnector(lifeCycleManager, metadata, splitManager, recordSetProvider, pageSinkProvider, sessionProperties, tableProperties);
    }

    @Test
    public void getMetadata_ReturnsMetadata() {
        ConnectorMetadata result = connector.getMetadata(mock(ConnectorTransactionHandle.class));
        assertEquals(result, metadata);
    }

    @Test
    public void beginTransaction_SupportedIsolationLevel_ReturnsTransactionHandle() {
        ConnectorTransactionHandle result = connector.beginTransaction(READ_UNCOMMITTED, true);
        assertTrue(result instanceof HbaseTransactionHandle);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void beginTransaction_UnsupportedIsolationLevel_ThrowsException() {
        connector.beginTransaction(IsolationLevel.SERIALIZABLE, true);
    }

    @Test
    public void getSplitManager_ReturnsSplitManager() {
        ConnectorSplitManager result = connector.getSplitManager();
        assertEquals(result, splitManager);
    }

    @Test
    public void getRecordSetProvider_ReturnsRecordSetProvider() {
        ConnectorRecordSetProvider result = connector.getRecordSetProvider();
        assertEquals(result, recordSetProvider);
    }

    @Test
    public void getPageSinkProvider_ReturnsPageSinkProvider() {
        ConnectorPageSinkProvider result = connector.getPageSinkProvider();
        assertEquals(result, pageSinkProvider);
    }

    @Test
    public void getTableProperties_ReturnsTableProperties() {
        List<PropertyMetadata<?>> properties = Collections.singletonList(mock(PropertyMetadata.class));
        when(tableProperties.getTableProperties()).thenReturn(properties);

        List<PropertyMetadata<?>> result = connector.getTableProperties();
        assertEquals(result, properties);
    }

    @Test
    public void getSessionProperties_ReturnsSessionProperties() {
        List<PropertyMetadata<?>> properties = Collections.singletonList(mock(PropertyMetadata.class));
        when(sessionProperties.getSessionProperties()).thenReturn(properties);

        List<PropertyMetadata<?>> result = connector.getSessionProperties();
        assertEquals(result, properties);
    }

    @Test
    public void shutdown_NormalShutdown_StopsLifeCycleManager() throws Exception {
        connector.shutdown();
        verify(lifeCycleManager, times(1)).stop();
    }

    @Test
    public void shutdown_ShutdownWithException_LogsError() throws Exception {
        doThrow(new RuntimeException("Test exception")).when(lifeCycleManager).stop();

        connector.shutdown();

        verify(lifeCycleManager, times(1)).stop();
    }
}
