/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import org.testng.annotations.Test;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class HiveImportClientFactoryTest
{
    @Test
    public void testGetClient()
            throws Exception
    {
        ServiceSelector selector = new StaticServiceSelector(
                serviceDescriptor("hive-metastore").addProperty("thrift", "meta1.test:9083").addProperty("name", "fuu").build(),
                serviceDescriptor("hive-metastore").addProperty("thrift", "meta2.text:9083").addProperty("name", "fuu").build(),
                serviceDescriptor("hive-metastore").addProperty("thrift", "meta3.text:9083").addProperty("name", "bar").build(),
                serviceDescriptor("hive-metastore").addProperty("thrift", "missing-port").build(),
                serviceDescriptor("hive-metastore").build());

        HiveClientFactory hiveClientFactory = new HiveClientFactory(new HiveClientConfig(), new HdfsEnvironment());
        DiscoveryLocatedHiveCluster hiveCluster = new DiscoveryLocatedHiveCluster(selector, new HiveMetastoreClientFactory(new HiveClientConfig()));
        HiveImportClientFactory factory = new HiveImportClientFactory(hiveCluster, hiveClientFactory);
        assertInstanceOf(factory.createClient("hive"), HiveClient.class);
    }
}
