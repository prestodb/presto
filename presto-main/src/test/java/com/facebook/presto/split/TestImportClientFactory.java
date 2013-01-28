package com.facebook.presto.split;

import com.facebook.presto.hive.CachingHiveClient;
import com.facebook.presto.hive.HivePartitionChunk;
import com.facebook.presto.hive.PathJsonDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.StaticServiceSelector;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ser.ToStringSerializer;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import static io.airlift.discovery.client.ServiceDescriptor.serviceDescriptor;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestImportClientFactory
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

        ImportClientFactory factory = new ImportClientFactory(selector, new HiveClientConfig(), getHivePartitionChunkCodec(), testExporter);
        assertInstanceOf(factory.getClient("hive_fuu"), CachingHiveClient.class);
        assertInstanceOf(factory.getClient("hive_bar"), CachingHiveClient.class);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "hive metastore not available for name fuu in pool general")
    public void testGetClientFailure()
    {
        ServiceSelector selector = new StaticServiceSelector(
                serviceDescriptor("hive-metastore").addProperty("thrift", "missing-port").addProperty("name", "fuu").build(),
                serviceDescriptor("hive-metastore").build());

        new ImportClientFactory(selector, new HiveClientConfig(), getHivePartitionChunkCodec(), testExporter).getClient("hive_fuu");
    }

    protected JsonCodec<HivePartitionChunk> getHivePartitionChunkCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Path.class, PathJsonDeserializer.INSTANCE));
        objectMapperProvider.setJsonSerializers(ImmutableMap.<Class<?>, JsonSerializer<?>>of(Path.class, ToStringSerializer.instance));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(HivePartitionChunk.class);
    }
}
