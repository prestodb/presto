/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNull;

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

        HiveClientFactory hiveClientFactory = new HiveClientFactory(new HiveClientConfig(), new HiveChunkEncoder(getHivePartitionChunkCodec()));
        DiscoveryLocatedHiveCluster hiveCluster = new DiscoveryLocatedHiveCluster(selector, new HiveMetastoreClientFactory());
        HiveImportClientFactory factory = new HiveImportClientFactory(hiveCluster, hiveClientFactory);
        assertInstanceOf(factory.createClient("hive"), HiveClient.class);
        assertNull(factory.createClient("hive_test"));
        assertNull(factory.createClient("unknown"));
    }

    protected JsonCodec<HivePartitionChunk> getHivePartitionChunkCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Path.class, PathJsonDeserializer.INSTANCE));
        objectMapperProvider.setJsonSerializers(ImmutableMap.<Class<?>, JsonSerializer<?>>of(Path.class, ToStringSerializer.instance));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(HivePartitionChunk.class);
    }
}
