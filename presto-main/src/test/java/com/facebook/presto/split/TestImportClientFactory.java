package com.facebook.presto.split;

import com.facebook.presto.hive.CachingHiveClient;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.StaticServiceSelector;
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

        ImportClientFactory factory = new ImportClientFactory(selector, new HiveClientConfig(), testExporter);
        assertInstanceOf(factory.getClient("hive_fuu"), CachingHiveClient.class);
        assertInstanceOf(factory.getClient("hive_bar"), CachingHiveClient.class);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "hive metastore not available for name fuu in pool general")
    public void testGetClientFailure()
    {
        ServiceSelector selector = new StaticServiceSelector(
                serviceDescriptor("hive-metastore").addProperty("thrift", "missing-port").addProperty("name", "fuu").build(),
                serviceDescriptor("hive-metastore").build());

        new ImportClientFactory(selector, new HiveClientConfig(), testExporter).getClient("hive_fuu");
    }
}
