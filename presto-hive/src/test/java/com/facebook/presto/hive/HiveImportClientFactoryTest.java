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
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNull;

public class HiveImportClientFactoryTest
{
    private final MBeanExporter testExporter = new MBeanExporter(new TestingMBeanServer());

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
        HiveImportClientFactory factory = new HiveImportClientFactory(selector, hiveClientFactory, testExporter);
        assertInstanceOf(factory.createClient("hive_fuu"), CachingHiveClient.class);
        assertInstanceOf(factory.createClient("hive_bar"), CachingHiveClient.class);
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
