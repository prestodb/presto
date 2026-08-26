package com.facebook.presto.plugin.hbase;


import com.facebook.presto.hbase.HbaseConnector;
import com.facebook.presto.hbase.HbaseConnectorFactory;
import com.facebook.presto.hbase.HbaseHandleResolver;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.common.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.*;
import static org.testng.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestHbaseConnectorFactory {

    private HbaseConnectorFactory connectorFactory;
    private ConnectorContext mockContext;
    private TypeManager mockTypeManager;
    private NodeManager mockNodeManager;
    private HbaseConnector mockConnector;

    @BeforeMethod
    public void setUp() {
        connectorFactory = new HbaseConnectorFactory("hbase");
        mockContext = mock(ConnectorContext.class);
        mockTypeManager = mock(TypeManager.class);
        mockNodeManager = mock(NodeManager.class);
        mockConnector = mock(HbaseConnector.class);

        when(mockContext.getTypeManager()).thenReturn(mockTypeManager);
        when(mockContext.getNodeManager()).thenReturn(mockNodeManager);
    }

    @Test
    public void getName_ShouldReturnCorrectName() {
        assertEquals(connectorFactory.getName(), "hbase");
    }

    @Test
    public void getHandleResolver_ShouldReturnHbaseHandleResolver() {
        ConnectorHandleResolver resolver = connectorFactory.getHandleResolver();
        assertNotNull(resolver);
        assertEquals(resolver.getClass(), HbaseHandleResolver.class);
    }

    @Test
    public void create_ShouldReturnConnectorInstance() {
        Map<String, String> config = ImmutableMap.of("configKey", "configValue");

        Connector connector = connectorFactory.create("testConnectorId", config, mockContext);

        assertNotNull(connector);
        assertEquals(connector.getClass(), HbaseConnector.class);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void create_NullConnectorId_ShouldThrowException() {
        connectorFactory.create(null, ImmutableMap.of(), mockContext);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void create_NullConfig_ShouldThrowException() {
        connectorFactory.create("testConnectorId", null, mockContext);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void create_NullContext_ShouldThrowException() {
        connectorFactory.create("testConnectorId", ImmutableMap.of(), null);
    }
}
