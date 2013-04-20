/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;

public class HiveImportClientFactoryTest
{
    @Test
    public void testGetClient()
            throws Exception
    {
        HiveConnectorFactory connectorFactory = new HiveConnectorFactory(ImmutableMap.<String, String>of("node.environment", "test"));
        Connector connector = connectorFactory.create("hive", ImmutableMap.<String, String>of());
        assertInstanceOf(connector.getService(ConnectorMetadata.class), ClassLoaderSafeConnectorMetadata.class);
        assertInstanceOf(connector.getService(ConnectorSplitManager.class), ClassLoaderSafeConnectorSplitManager.class);
        assertInstanceOf(connector.getService(ConnectorRecordSetProvider.class), ClassLoaderSafeConnectorRecordSetProvider.class);
        assertInstanceOf(connector.getService(ConnectorHandleResolver.class), ClassLoaderSafeConnectorHandleResolver.class);
    }
}
